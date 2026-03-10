using System.Globalization;
using System.Text.RegularExpressions;
using ImageMagick;
using CountyFusion;
using CountyFusion.Utils;
using Microsoft.Playwright;
using CountyFusion.Models;

namespace CountyFusion.Services;

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

        IFrame? docImgFrame = null;
        for (var i = 0; i < 15; i++)
        {
            docImgFrame = page.Frames.FirstOrDefault(f =>
                string.Equals(f.Name, "docImgViewFrame", StringComparison.OrdinalIgnoreCase) ||
                f.Url.Contains("InstrumentImageViewInternal.jsp", StringComparison.OrdinalIgnoreCase));

            if (docImgFrame != null)
                break;

            await Task.Delay(500);
        }

        if (docImgFrame == null)
        {
            Console.WriteLine($"[Images] docImgViewFrame not found for record {recordIndex} ({record.DocumentNumber}) after waiting.");
            record.Images = string.Join("\n", imageLinks);
            return;
        }

        await Task.Delay(500);

        int totalPages = 1;
        for (var poll = 0; poll < 12; poll++)
        {
            try
            {
                var totalSpan = docImgFrame.Locator("span#totalPages");
                if (await totalSpan.CountAsync() > 0)
                {
                    var raw = (await totalSpan.First.InnerTextAsync()).Trim();
                    foreach (Match m in Regex.Matches(raw, @"\d+"))
                    {
                        if (int.TryParse(m.Value, out var n) && n > 0 && n > totalPages)
                            totalPages = n;
                    }
                }
            }
            catch
            {
            }
            if (poll < 11)
                await Task.Delay(500);
        }

        var pagesToProcess = totalPages;
        Console.WriteLine($"[Images] Record {recordIndex} ({record.DocumentNumber}) totalPages = {pagesToProcess}");

        if (pagesToProcess <= 0)
        {
            record.Images = string.Join("\n", imageLinks);
            return;
        }

        var dateForFilename = searchDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var docNum = DomHelper.ExtractInstrumentNumber(record.DocumentNumber);
        if (string.IsNullOrWhiteSpace(docNum))
            docNum = record.DocumentNumber;
        var baseName = string.IsNullOrWhiteSpace(docNum)
            ? $"record-{recordIndex}"
            : SanitizeFilename(docNum);

        const double imageScale = 0.5;
        var captureScript = @"(expectedSrc) => {
            function findImage() {
                var main = document.getElementById('mainCropperImage');
                if (main && main.src === expectedSrc && main.complete && main.naturalWidth > 0) return main;
                
                var canvasImg = document.querySelector('.cropper-canvas img');
                if (canvasImg && canvasImg.src === expectedSrc && canvasImg.complete && canvasImg.naturalWidth > 0) return canvasImg;
                
                return null;
            }
            var img = findImage();
            if (!img) return null;
            
            var w = img.naturalWidth, h = img.naturalHeight;
            var scale = " + imageScale.ToString("R", CultureInfo.InvariantCulture) + @";
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
        }";

        const string stableImageSrcScript = @"(expectedPage) => {
            var pageSpan = document.getElementById('selectedPageId');
            if (!pageSpan) return null;
            var txt = (pageSpan.textContent || pageSpan.innerText || '').trim();
            var currentPage = parseInt(txt, 10);
            if (isNaN(currentPage) || currentPage !== expectedPage) return null;

            var main = document.getElementById('mainCropperImage');
            var loading = document.getElementById('loadingImage');
            
            if (!main || !main.src) return null;
            if (!main.complete || main.naturalWidth === 0) return null;

            var mainVis = window.getComputedStyle(main).visibility;
            if (mainVis === 'hidden') return null;

            if (loading) {
                var loadingVis = window.getComputedStyle(loading).visibility;
                if (loadingVis !== 'hidden') return null;
            }

            return main.src.toString();
        }";

        var base64List = new List<string>();

        const int maxReadyAttempts = 300;
        const int maxCaptureRetries = 8;   // retry capture 8 times
        const int maxPageRetries = 5;      // retry same page up to 5 times before giving up

        for (var pageIndex = 1; pageIndex <= pagesToProcess; pageIndex++)
        {
            var base64 = (string?)null;
            var pageRetries = 0;
            var success = false;

            while (!success && pageRetries < maxPageRetries)
            {
                pageRetries++;
                if (pageIndex > 1)
                {
                    try
                    {
                        var pageSpan = docImgFrame.Locator("span#selectedPageId");
                        var currentSelectedStr = await pageSpan.CountAsync() > 0 ? await pageSpan.First.InnerTextAsync() : "";
                        int.TryParse(currentSelectedStr, out var currentSelected);

                        if (currentSelected != pageIndex)
                        {
                            if (currentSelected == pageIndex - 1)
                            {
                                var nextBtn = docImgFrame.Locator("button[onclick='nextPage()']");
                                if (await nextBtn.CountAsync() > 0)
                                    await nextBtn.First.ClickAsync(new LocatorClickOptions { Force = true });
                            }
                            else
                            {
                                await docImgFrame.EvaluateAsync($"try {{ if (typeof goToPage === 'function') goToPage({pageIndex}); }} catch(e){{}}");
                            }
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Images] Navigation click for page {pageIndex} failed: {ex.Message}");
                    }
                }

                string? foundStableSrc = null;
                for (var attempt = 0; attempt < maxReadyAttempts; attempt++)
                {
                    try
                    {
                        foundStableSrc = await docImgFrame.EvaluateAsync<string?>(stableImageSrcScript, pageIndex);
                        if (!string.IsNullOrEmpty(foundStableSrc))
                        {
                            for (var capRetry = 0; capRetry < maxCaptureRetries; capRetry++)
                            {
                                base64 = await docImgFrame.EvaluateAsync<string?>(captureScript, foundStableSrc);
                                if (!string.IsNullOrEmpty(base64))
                                    break;
                                await Task.Delay(100);
                            }
                            break;
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[Images] Page {pageIndex} attempt {attempt + 1}: {ex.Message}");
                    }
                    if (attempt > 0 && attempt % 30 == 0 && pageIndex > 1)
                    {
                        try
                        {
                            await docImgFrame.EvaluateAsync($"try {{ if (typeof goToPage === 'function') goToPage({pageIndex}); }} catch(e){{}}");
                        }
                        catch
                        {
                        }
                    }
                    await Task.Delay(150);
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
                continue;
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

    static string SanitizeFilename(string s)
    {
        if (string.IsNullOrWhiteSpace(s))
            return s ?? string.Empty;
        var trimmed = s.Trim().Replace("/", "_").Replace("\\", "_");
        var invalid = Path.GetInvalidFileNameChars();
        foreach (var c in invalid)
            trimmed = trimmed.Replace(c, '_');
        var firstLine = trimmed.Split(new[] { '\r', '\n' }, StringSplitOptions.RemoveEmptyEntries).FirstOrDefault() ?? trimmed;
        return firstLine.Length > 120 ? firstLine[..120] : firstLine;
    }
}
