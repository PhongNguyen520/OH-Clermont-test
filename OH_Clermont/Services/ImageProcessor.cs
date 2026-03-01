using System.Globalization;
using System.Text.RegularExpressions;
using ImageMagick;
using OH_Clermont;
using Microsoft.Playwright;
using OH_Clermont.Models;

namespace OH_Clermont.Services;

/// <summary>Image processing utilities: base64 to TIFF, multi-page capture.</summary>
public static class ImageProcessor
{
    /// <summary>Converts base64 images to a single Multi-page TIFF with CCITT Group 4 compression.</summary>
    public static byte[] CreateMultiPageCompressedTiff(string[] base64Images)
    {
        using var collection = new MagickImageCollection();
        foreach (var b64 in base64Images)
        {
            if (string.IsNullOrWhiteSpace(b64)) continue;

            var imgBytes = Convert.FromBase64String(b64);
            var image = new MagickImage(imgBytes);
            image.HasAlpha = false;
            image.ColorSpace = ColorSpace.Gray;
            image.Threshold(new Percentage(50));
            image.ColorType = ColorType.Bilevel;
            image.Format = MagickFormat.Tif;
            image.Settings.Compression = CompressionMethod.Group4;
            image.Density = new Density(300, 300);
            collection.Add(image);
        }

        using var outputStream = new MemoryStream();
        collection.Write(outputStream, MagickFormat.Tif);
        return outputStream.ToArray();
    }

    /// <summary>Process all image pages for current record: capture canvas, convert to TIFF, store via Apify KV.</summary>
    public static async Task ProcessImagesForCurrentRecordAsync(IPage page, ClermontRecord record, DateTime searchDate, int recordIndex)
    {
        var imageLinks = new List<string>();

        var docImgFrame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "docImgViewFrame", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("InstrumentImageViewInternal.jsp", StringComparison.OrdinalIgnoreCase));

        if (docImgFrame == null)
        {
            Console.WriteLine($"[Images] docImgViewFrame not found for record {recordIndex} ({record.DocumentNumber})");
            record.Images = string.Join("\n", imageLinks);
            return;
        }

        await Task.Delay(1500);

        int totalPages = 1;
        try
        {
            var totalSpan = docImgFrame.Locator("span#totalPages");
            if (await totalSpan.CountAsync() > 0)
            {
                var raw = (await totalSpan.First.InnerTextAsync()).Trim();
                var m = Regex.Match(raw, @"\d+");
                if (m.Success && int.TryParse(m.Value, out var n) && n > 0)
                    totalPages = n;
            }
        }
        catch
        {
            totalPages = 1;
        }

        var pagesToProcess = totalPages;
        Console.WriteLine($"[Images] Record {recordIndex} ({record.DocumentNumber}) totalPages = {pagesToProcess}");

        if (pagesToProcess <= 0)
        {
            record.Images = string.Join("\n", imageLinks);
            return;
        }

        var dateForFilename = searchDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var baseName = string.IsNullOrWhiteSpace(record.DocumentNumber)
            ? $"record-{recordIndex}"
            : record.DocumentNumber.Replace("/", "_").Replace("\\", "_").Trim();

        const string isImageReadyScript = @"() => {
            function findImage() {
                var img = document.querySelector('#mainCropperImage');
                if (img && img.src && img.complete && img.naturalWidth > 0) return img;
                var canvasImg = document.querySelector('.cropper-canvas img');
                if (canvasImg && canvasImg.src && canvasImg.complete && canvasImg.naturalWidth > 0) return canvasImg;
                return null;
            }
            return !!findImage();
        }";

        const double imageScale = 0.5;
        var captureScript = $@"() => {{
            function findImage() {{
                var img = document.querySelector('#mainCropperImage');
                if (img && img.src && img.complete && img.naturalWidth > 0) return img;
                var canvasImg = document.querySelector('.cropper-canvas img');
                if (canvasImg && canvasImg.src && canvasImg.complete && canvasImg.naturalWidth > 0) return canvasImg;
                return null;
            }}
            var img = findImage();
            if (!img) return null;
            var w = img.naturalWidth, h = img.naturalHeight;
            var scale = {imageScale.ToString(CultureInfo.InvariantCulture)};
            var cw = Math.max(1, Math.floor(w * scale)), ch = Math.max(1, Math.floor(h * scale));
            var canvas = document.createElement('canvas');
            canvas.width = cw;
            canvas.height = ch;
            var ctx = canvas.getContext('2d');
            if (!ctx) return null;
            ctx.drawImage(img, 0, 0, w, h, 0, 0, cw, ch);
            var dataUrl = canvas.toDataURL('image/png');
            if (!dataUrl) return null;
            var parts = dataUrl.split(',');
            return parts.length === 2 ? parts[1] : null;
        }}";

        var base64List = new List<string>();

        const int maxReadyAttempts = 180;  // 180 * 500ms = 90s max wait per page (ensure image loads)
        const int maxCaptureRetries = 8;   // retry capture 8 times
        const int maxNextBtnRetries = 8;
        const int maxPageRetries = 5;      // retry same page up to 5 times before giving up

        for (var pageIndex = 1; pageIndex <= pagesToProcess; pageIndex++)
        {
            var base64 = (string?)null;
            var pageRetries = 0;
            var success = false;

            while (!success && pageRetries < maxPageRetries)
            {
                pageRetries++;
                for (var attempt = 0; attempt < maxReadyAttempts; attempt++)
                {
                    try
                    {
                        var ready = await docImgFrame.EvaluateAsync<bool>(isImageReadyScript);
                        if (ready)
                        {
                            for (var capRetry = 0; capRetry < maxCaptureRetries; capRetry++)
                            {
                                base64 = await docImgFrame.EvaluateAsync<string?>(captureScript);
                                if (!string.IsNullOrEmpty(base64))
                                    break;
                                await Task.Delay(800);
                            }
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Images] Page {pageIndex} attempt {attempt + 1}: {ex.Message}");
                    }
                    await Task.Delay(500);
                }

                if (!string.IsNullOrEmpty(base64))
                {
                    base64List.Add(base64);
                    success = true;
                    break;
                }

                Console.WriteLine($"[Images] Page {pageIndex}/{pagesToProcess} for record {recordIndex} failed (retry {pageRetries}/{maxPageRetries}). Retrying in 3s...");
                await Task.Delay(3000);
            }

            if (!success)
            {
                Console.WriteLine($"[Images] Page {pageIndex}/{pagesToProcess} for record {recordIndex} FAILED after {maxPageRetries} retries - page not captured.");
                if (pageIndex < pagesToProcess)
                {
                    try
                    {
                        var nextBtn = docImgFrame.Locator("button[onclick*=\"nextPage()\"]");
                        if (await nextBtn.CountAsync() > 0)
                        {
                            await nextBtn.First.ClickAsync();
                            await Task.Delay(2000);
                        }
                    }
                    catch { }
                }
                continue;
            }

            if (pageIndex < pagesToProcess)
            {
                var nextClicked = false;
                for (var btnRetry = 0; btnRetry < maxNextBtnRetries; btnRetry++)
                {
                    try
                    {
                        var nextBtn = docImgFrame.Locator("button[onclick*=\"nextPage()\"]");
                        if (await nextBtn.CountAsync() > 0)
                        {
                            await nextBtn.First.ClickAsync();
                            await Task.Delay(2000);
                            nextClicked = true;
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Images] Next button click attempt {btnRetry + 1}: {ex.Message}");
                    }
                    await Task.Delay(1000);
                }
                if (!nextClicked)
                {
                    Console.WriteLine($"[Images] Next button not found for page {pageIndex}/{pagesToProcess}. Waiting 3s and retrying...");
                    await Task.Delay(3000);
                    var extraNext = docImgFrame.Locator("button[onclick*=\"nextPage()\"], a[onclick*=\"nextPage()\"], img[onclick*=\"nextPage()\"]");
                    if (await extraNext.CountAsync() > 0)
                    {
                        await extraNext.First.ClickAsync();
                        await Task.Delay(2000);
                    }
                }
            }
        }

        var base64Arr = base64List.ToArray();
        if (base64Arr.Length > 0)
        {
            try
            {
                var bytes = CreateMultiPageCompressedTiff(base64Arr);
                var fileName = $"{baseName}.tif";
                var key = $"Images/{dateForFilename}/{baseName}/{fileName}";

                await ApifyHelper.SaveImageAsync(key, bytes);
                imageLinks.Add(ApifyHelper.GetRecordUrl(key));

                Console.WriteLine($"[Images] Saved Multi-page TIF ({base64Arr.Length} pages): {key}");

                base64List.Clear();
                GC.Collect();
                GC.WaitForPendingFinalizers();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Images] Error saving Multi-page TIF for record {recordIndex}: {ex.Message}");
            }
        }

        record.Images = string.Join("\n", imageLinks);
    }
}
