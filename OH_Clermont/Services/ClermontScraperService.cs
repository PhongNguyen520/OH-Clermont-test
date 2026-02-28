using System.Globalization;
using System.Runtime;
using System.Text.Json;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using CsvHelper;
using ImageMagick;
using Microsoft.Playwright;
using OH_Clermont;
using OH_Clermont.Models;

namespace OH_Clermont.Services;

/// <summary>
/// Playwright-based scraper for Clermont County.
/// - Open login page
/// - Handle timeout/session errors
/// - Click "Login as Public"
/// - If "Login Failed: Active Session" appears, clear cookies/storage, reload, and retry once.
/// </summary>
public class ClermontScraperService
{
    const string CountyLoginUrl = "https://countyfusion2.govos.com/countyweb/loginDisplay.action?countyname=ClermontOH";
    const string TimeoutErrorSnippet = "This form has already been processed or the session timed out";
    const string ActiveSessionSnippet = "Login Failed: Active Session.";

    IPlaywright? _playwright;
    IBrowser? _browser;
    IBrowserContext? _context;
    StreamWriter? _csvStreamWriter;
    CsvWriter? _csvWriter;

    /// <summary>
    /// Launches Chromium/Chrome, opens login page and handles session errors.
    /// Then navigates to the Search Criteria form and fills it according to InputConfig (display, name, fromDate, toDate).
    /// </summary>
    public async Task<IPage> LaunchAsync(InputConfig config)
    {
        config ??= new InputConfig();

        await ApifyHelper.SetStatusMessageAsync("Starting OH-Clermont scraper...");

        // Validate dates before retry loop (validation errors are not retryable)
        DateTime fromDate, toDate;
        try
        {
            fromDate = ParseDate(config.FromDate, "fromDate");
            toDate = ParseDate(config.ToDate, "toDate");
        }
        catch (ArgumentException ex)
        {
            await ApifyHelper.SetStatusMessageAsync($"Validation Error: {ex.Message}", isTerminal: true);
            throw;
        }

        int maxRetries = 3;
        IPage? resultPage = null;

        for (int attempt = 1; attempt <= maxRetries; attempt++)
        {
            try
            {
                await ApifyHelper.SetStatusMessageAsync($"Attempt {attempt} of {maxRetries}...");

                _playwright = await Playwright.CreateAsync();

                // On Apify we run headless; locally we keep the UI visible for debugging
                var isApify = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("APIFY_CONTAINER_PORT"));

                // Prefer Chrome channel when available, otherwise fall back to plain Chromium.
                // Chromium args to reduce memory use and avoid OOM (especially in Docker/Apify)
                var browserArgs = new[]
        {
            "--no-default-browser-check",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--no-sandbox",
            "--disable-software-rasterizer",
            "--disable-extensions",
            "--disable-background-networking",
            "--disable-default-apps",
            "--disable-sync",
            "--disable-translate",
            "--mute-audio",
            "--no-first-run",
            "--disable-renderer-backgrounding"
        };
        try
        {
            _browser = await _playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
            {
                Channel = "chrome",
                Headless = isApify,
                Timeout = 60_000,
                Args = browserArgs
            });
        }
        catch
        {
            _browser = await _playwright.Chromium.LaunchAsync(new BrowserTypeLaunchOptions
            {
                Headless = isApify,
                Timeout = 60_000,
                Args = browserArgs
            });
        }

        // Context: keep cookies and JavaScript enabled (Playwright defaults).
        _context = await _browser.NewContextAsync(new BrowserNewContextOptions
        {
            IgnoreHTTPSErrors = true
        });

        var page = await _context.NewPageAsync();
        page.SetDefaultTimeout(30_000);

        // 1) Go to login page — wait for NetworkIdle to ensure GovOS sets session cookies
        await page.GotoAsync(CountyLoginUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        // 2) If the page immediately shows "This form has already been processed..." then clear cookies/storage and reload
        if (await HasErrorSnippetAsync(page, TimeoutErrorSnippet))
        {
            await ClearCookiesAndReloadAsync(page);
        }

        // 3) First click on "Login as Public"
        await ClickLoginAsPublicAsync(page);

        // 4) If "Login Failed: Active Session" appears, clear cookies/storage, reload, and retry one more time
        if (await HasErrorSnippetAsync(page, ActiveSessionSnippet))
        {
            await ClearCookiesAndReloadAsync(page);
            await ClickLoginAsPublicAsync(page);
        }

        // 5) Disclaimer page: click the Accept button (<input id="accept" ... />)
        await ClickDisclaimerAcceptAsync(page);

        // 6) Welcome page (inside iframe bodyframe): click "Search Public Records"
        await ClickSearchPublicRecordsAsync(page);

        // 7) Search Criteria page (iframe dynSearchFrame): set All Names + Display (always 500) + All Document Types + One Row per Document
        var display = config.Display <= 0 ? 500 : config.Display;
        await SetupSearchPageAsync(page, display);

            // 8) Set Recorded Date From/To (fromDate/toDate validated above), search, and scrape
            await SetDateRangeForDayAsync(page, fromDate);
            await Task.Delay(1500);
            await ClickSearchAsync(page);

            // 9) Open CSV once for the whole run (like OH-Montgomery) to avoid OOM from repeated open/close per record
            var outputDirectory = Directory.GetCurrentDirectory();
            var exportImages = config.ExportImages;
            if (!exportImages)
                Console.WriteLine("[Scrape] ExportImages=false: skipping image download to reduce memory.");
            OpenCsvStreamForRun(fromDate, outputDirectory);
            try
            {
                await ScrapeAllRecordsOnResultsPageAsync(page, fromDate, outputDirectory, exportImages);
                Console.WriteLine("[Scrape] Done. Records were pushed to Dataset and appended to CSV.");
            }
            finally
            {
                CloseCsvStream();
            }
            resultPage = page;
            break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Attempt {attempt}] Error: {ex.Message}");
                await StopAsync();
                if (attempt == maxRetries)
                {
                    await ApifyHelper.SetStatusMessageAsync($"Fatal Error after {maxRetries} attempts: {ex.Message}", isTerminal: true);
                    throw;
                }
                await ApifyHelper.SetStatusMessageAsync($"Attempt {attempt} failed. Retrying in 10 seconds...");
                await Task.Delay(10000);
            }
        }

        return resultPage!;
    }

    static DateTime ParseDate(string value, string fieldName)
    {
        if (string.IsNullOrWhiteSpace(value))
            throw new ArgumentException($"Input field '{fieldName}' is required.");

        const string fmt = "MM/dd/yyyy";
        if (DateTime.TryParseExact(value.Trim(), fmt, CultureInfo.InvariantCulture,
                DateTimeStyles.None, out var dt))
        {
            return dt;
        }

        throw new ArgumentException($"Input field '{fieldName}' has invalid date '{value}'. Expected format MM/dd/yyyy (e.g. 01/01/2024).");
    }

    /// <summary>
    /// Parse text like "Displaying 1-100 of 112 Items" and return how many records are displayed on the current page (100).
    /// </summary>
    static int GetPageCountFromNavDisplay(string? text)
    {
        if (string.IsNullOrWhiteSpace(text))
            return 0;

        var match = Regex.Match(
            text,
            @"Displaying\s+(\d+)\s*-\s*(\d+)\s+of\s+(\d+)\s+Items",
            RegexOptions.IgnoreCase);

        if (!match.Success)
            return 0;

        if (!int.TryParse(match.Groups[1].Value, out var start))
            return 0;
        if (!int.TryParse(match.Groups[2].Value, out var end))
            return 0;

        if (end < start)
            return 0;

        return end - start + 1;
    }

   /// <summary>
    /// Converts multiple base64 PNG/JPEG strings into a single Multi-page TIFF byte array,
    /// applying CCITT Group 4 compression to drastically reduce file size.
    /// </summary>
    static byte[] CreateMultiPageCompressedTiff(string[] base64Images)
    {
        using var collection = new MagickImageCollection();
        foreach (var b64 in base64Images)
        {
            if (string.IsNullOrWhiteSpace(b64)) continue;

            var imgBytes = Convert.FromBase64String(b64);
            var image = new MagickImage(imgBytes);

            // Bắt buộc loại bỏ kênh Alpha (trong suốt) vì TIFF Group 4 không hỗ trợ
            image.HasAlpha = false; 

            // Ép màu sắc về thang xám (Grayscale)
            image.ColorSpace = ColorSpace.Gray;

            // Dùng thuật toán Threshold (ngưỡng 50%) để ép tất cả pixel thành 100% Trắng hoặc 100% Đen
            image.Threshold(new Percentage(50));

            // SỬA LỖI Ở ĐÂY: Dùng ColorType.Bilevel thay vì Type = MagickType.Bilevel
            image.ColorType = ColorType.Bilevel; 

            // Cấu hình định dạng và chuẩn nén
            image.Format = MagickFormat.Tif;
            image.Settings.Compression = CompressionMethod.Group4;
            
            // Mật độ điểm ảnh (Độ phân giải) chuẩn văn phòng
            image.Density = new Density(300, 300);

            collection.Add(image);
        }

        using var outputStream = new MemoryStream();
        collection.Write(outputStream, MagickFormat.Tif);
        return outputStream.ToArray();
    }

    static async Task<bool> HasErrorSnippetAsync(IPage page, string snippet)
    {
        var errors = page.Locator("tr.error td");
        var count = await errors.CountAsync();
        for (var i = 0; i < count; i++)
        {
            var text = (await errors.Nth(i).InnerTextAsync()).Trim();
            if (text.Contains(snippet, StringComparison.OrdinalIgnoreCase))
                return true;
        }
        return false;
    }

    /// <summary>
    /// Scrape Instrument Info (group 1 fields) from the DocumentInfoView.jsp detail page into a ClermontRecord.
    /// </summary>
    public static async Task<ClermontRecord> ScrapeInstrumentInfoAsync(IPage page)
    {
        // Frame chain: bodyframe -> documentFrame -> docInfoFrame (actual docDetails content)
        var docInfoFrame = page
            .FrameLocator("iframe[name='bodyframe']")
            .FrameLocator("iframe[name='documentFrame']")
            .FrameLocator("iframe[name='docInfoFrame']");

        // Give the detail frame a moment to render content
        await Task.Delay(2000);

        var record = new ClermontRecord
        {
            Amount = string.Empty,
            Remarks = string.Empty,
            ParcelNumber = string.Empty,
            PropertyAddress = string.Empty
        };

        // Local helper: read the value cell next to a label (td) in docInfoFrame
        async Task<string> GetByLabelAsync(string label)
        {
            var labelLocator = docInfoFrame.Locator(
                $"xpath=//td[contains(normalize-space(), '{label}')]");

            if (await labelLocator.CountAsync() == 0)
                return string.Empty;

            return await labelLocator.First.EvaluateAsync<string>(@"(node) => {
                function getText(el) {
                    if (!el) return '';
                    return el.textContent.trim();
                }
                var td = node.closest('td') || node;
                var cell = td.nextElementSibling;
                return getText(cell);
            }");
        }

        // Document Number: "Instrument Number:"
        record.DocumentNumber = (await GetByLabelAsync("Instrument Number:")).Trim();

        // Book Type: "Book Type:"
        record.BookType = (await GetByLabelAsync("Book Type:")).Trim();

        // Book / Page: "Book / Page:"
        var bookPage = await GetByLabelAsync("Book / Page:");
        if (!string.IsNullOrWhiteSpace(bookPage))
        {
            var parts = bookPage.Split('/');

            // Book: text before '/'
            if (parts.Length > 0)
            {
                record.Book = parts[0].Trim();
            }

            // Page: text after '/', may contain JS/junk -> keep only the first token
            if (parts.Length > 1)
            {
                var rawPage = parts[1];
                var tokens = rawPage
                    .Split(new[] { ' ', '\n', '\r', '\t', '\u00A0' }, StringSplitOptions.RemoveEmptyEntries);
                var cleanPage = tokens.FirstOrDefault();
                record.Page = cleanPage?.Trim() ?? string.Empty;
            }
        }

        // Recording Date: "Recorded Date:", keep only the date portion (strip time)
        var recDateRaw = await GetByLabelAsync("Recorded Date:");
        if (!string.IsNullOrWhiteSpace(recDateRaw))
        {
            var idx = recDateRaw.IndexOf(' ');
            record.RecordingDate = idx > 0 ? recDateRaw[..idx].Trim() : recDateRaw.Trim();
        }

        // Document Type: "Instrument Type:"
        record.DocumentType = (await GetByLabelAsync("Instrument Type:")).Trim();

        // Instrument Date: "Document Date:"
        record.InstrumentDate = (await GetByLabelAsync("Document Date:")).Trim();

        // Grantor: Names section -> "Grantor:"
        try
        {
            var grantorCells = docInfoFrame.Locator(
                "xpath=//span[@id='7header' and contains(@class,'subsectionheader')]" +
                "/ancestor::table[1]/following-sibling::table[1]" +
                "//tr[contains(@class,'evenrow')]/td");

            if (await grantorCells.CountAsync() > 0)
            {
                var texts = await grantorCells.AllInnerTextsAsync();
                var joined = string.Join(
                    "; ",
                    texts.Select(t => t.Replace("\u00A0", " ").Trim())
                         .Where(t => !string.IsNullOrWhiteSpace(t)));
                record.Grantor = joined;
            }
        }
        catch
        {
            // Ignore if structure changes
        }

        // Grantee: Names section -> "Grantee:"
        try
        {
            var granteeCells = docInfoFrame.Locator(
                "xpath=//span[@id='6header' and contains(@class,'subsectionheader')]" +
                "/ancestor::table[1]/following-sibling::table[1]" +
                "//tr[contains(@class,'evenrow')]/td");

            if (await granteeCells.CountAsync() > 0)
            {
                var texts = await granteeCells.AllInnerTextsAsync();
                var joined = string.Join(
                    "; ",
                    texts.Select(t => t.Replace("\u00A0", " ").Trim())
                         .Where(t => !string.IsNullOrWhiteSpace(t)));
                record.Grantee = joined;
            }
        }
        catch
        {
            // Ignore if structure changes
        }

        // Reference: full row per Marginal Reference (Doc# + Type + Book/Page + Date), multiple rows joined by "; "
        try
        {
            var referenceLinks = docInfoFrame.Locator("xpath=//a[contains(@onclick,\"loadDoc('\")]");
            var refCount = await referenceLinks.CountAsync();
            if (refCount > 0)
            {
                var rowLines = new List<string>();
                for (var r = 0; r < refCount; r++)
                {
                    var link = referenceLinks.Nth(r);
                    var row = link.Locator("xpath=ancestor::tr[1]");
                    var cells = row.Locator("td");
                    var cellCount = await cells.CountAsync();
                    if (cellCount == 0) continue;
                    var cellTexts = await cells.AllInnerTextsAsync();
                    var line = string.Join("\t",
                        cellTexts
                            .Select(t => (t ?? string.Empty).Replace("\u00A0", " ").Trim())
                            .Where(t => !string.IsNullOrWhiteSpace(t)));
                    if (!string.IsNullOrWhiteSpace(line))
                        rowLines.Add(line);
                }
                record.Reference = string.Join("; ", rowLines);
            }
        }
        catch
        {
            // Ignore if not found
        }

        // Group 3: Legal & Long Description (Legal Description tab)
        try
        {
            // Tab header is inside the child "tabs" iframe within docInfoFrame
            var tabsFrame = docInfoFrame.FrameLocator("iframe[name='tabs'], iframe#tabs");
            var legalTab = tabsFrame.Locator(".tabs-title:has-text(\"Legal Description\")");

            if (await legalTab.CountAsync() > 0)
            {
                await legalTab.First.ClickAsync();
                await Task.Delay(1500); // wait for Legal tab contents to load into docInfoFrame
            }
        }
        catch
        {
            // If the tab cannot be clicked, skip it and keep Legal/LongDescription empty
        }

        // Scrape Legal from td.basesm in docInfoFrame after the Legal Description tab is selected
        try
        {
            var legalTexts = await docInfoFrame.Locator("td.basesm").AllInnerTextsAsync();
            if (legalTexts != null && legalTexts.Count > 0)
            {
                var cleaned = legalTexts
                    .Select(t => (t ?? string.Empty).Trim())
                    .Where(t => !string.IsNullOrWhiteSpace(t))
                    .Select(t => t.Replace("|", string.Empty));

                record.Legal = string.Join("; ", cleaned);
            }
        }
        catch
        {
            // Ignore if there is no Legal structure
        }

        // Scrape Long Description: td immediately following span#fc210span
        try
        {
            var longDescLocator = docInfoFrame
                .Locator("span#fc210span")
                .Locator("xpath=../following-sibling::td");

            if (await longDescLocator.CountAsync() > 0)
            {
                var raw = await longDescLocator.First.InnerTextAsync();
                if (!string.IsNullOrWhiteSpace(raw))
                {
                    var normalized = raw
                        .Replace("\r", " ")
                        .Replace("\n", " ")
                        .Trim();

                    record.LongDescription = normalized;
                }
            }
        }
        catch
        {
            // Ignore if Long Description field is missing
        }

        // Cap very long fields to avoid OOM when serializing to JSON for Dataset (and large CSV rows)
        const int MaxFieldLength = 32768;
        if (record.Legal.Length > MaxFieldLength)
            record.Legal = record.Legal.Substring(0, MaxFieldLength) + "... [truncated]";
        if (record.LongDescription.Length > MaxFieldLength)
            record.LongDescription = record.LongDescription.Substring(0, MaxFieldLength) + "... [truncated]";

        return record;
    }

    /// <summary>
    /// On the result list page, click the first record in the Instrument # column,
    /// then use the Next button (navToDocument('next')) in DocumentInfoView
    /// to move through subsequent records and scrape each one.
    /// CSV and Dataset are flushed after every 1 record to minimize memory (avoids OOM even with many records).
    /// </summary>
    public async Task<List<ClermontRecord>> ScrapeAllRecordsOnResultsPageAsync(
        IPage page,
        DateTime searchDate,
        string outputDirectory,
        bool exportImages = true)
    {
        var batch = new List<ClermontRecord>(capacity: 1);

        // Correct iframe chain:
        // - resultFrame: contains header "Displaying 1-100 of 112 Items"
        // - resultListFrame: contains the result grid and Instrument # column
        var bodyFrame = page.FrameLocator("iframe[name='bodyframe']");
        var resultFrame = bodyFrame.FrameLocator("iframe[name='resultFrame']");
        var resultListFrame = resultFrame.FrameLocator("iframe[name='resultListFrame']");

        // Give results time to finish rendering
        await Task.Delay(2000);

        const string instrumentSelector =
            "#instList .datagrid-view2 .datagrid-body .datagrid-btable tr.datagrid-row td[field='2'] a";

        // 1) Read navDisplay to know how many records are shown on this page (e.g. 1-100 of 112 Items -> 100)
        int pageCount = 0;
        try
        {
            var navDisplay = resultFrame.Locator("td#navDisplay");
            if (await navDisplay.CountAsync() > 0)
            {
                var navText = (await navDisplay.First.InnerTextAsync()).Trim();
                pageCount = GetPageCountFromNavDisplay(navText);
            }
        }
        catch
        {
            pageCount = 0;
        }

        // 1b) Read "Page X of Y" to know current page and total pages
        int currentPage = 0, totalPages = 0;
        try
        {
            var pageInfoCell = resultFrame.Locator("td.msg1:has-text(\"Page\")");
            if (await pageInfoCell.CountAsync() > 0)
            {
                var pageText = (await pageInfoCell.First.InnerTextAsync()).Trim();
                // Example: "Page 1 of 2"
                var match = Regex.Match(pageText, @"Page\s+(\d+)\s+of\s+(\d+)", RegexOptions.IgnoreCase);
                if (match.Success)
                {
                    int.TryParse(match.Groups[1].Value, out currentPage);
                    int.TryParse(match.Groups[2].Value, out totalPages);
                }
            }
        }
        catch
        {
            currentPage = totalPages = 0;
        }

        // 2) Ensure we do not exceed the actual number of Instrument # links in the grid
        var linkCount = await resultListFrame.Locator(instrumentSelector).CountAsync();
        var totalToProcess = pageCount > 0 ? Math.Min(pageCount, linkCount) : linkCount;

        // Log to console how many records will be processed on this page and which page we are on
        Console.WriteLine($"[Results] navDisplay pageCount = {pageCount}, instrumentLinks = {linkCount}, totalToProcess = {totalToProcess}, currentPage = {currentPage}, totalPages = {totalPages}");

        if (totalToProcess == 0)
            return new List<ClermontRecord>();

        await ApifyHelper.SetStatusMessageAsync("Found records. Preparing to extract...");

        for (var i = 0; i < totalToProcess; i++)
        {
            if ((i + 1) % 10 == 0)
            {
                await ApifyHelper.SetStatusMessageAsync($"Processing record {i + 1} of {totalToProcess} on current page...");
            }

            if (i == 0)
            {
                // First record: click the Instrument # link directly from the result list
                var firstLink = resultListFrame.Locator(instrumentSelector).First;
                await firstLink.ScrollIntoViewIfNeededAsync();
                await firstLink.ClickAsync();
            }
            else
            {
                // Subsequent records: use the Next button in DocumentInfoView
                var moved = await ClickNextDocumentAsync(page);
                if (!moved)
                    break;
            }

            // Wait for detail view to load, then scrape text fields
            var record = await ScrapeInstrumentInfoAsync(page);
            batch.Add(record);

            // Process images only when enabled (set ExportImages=false in input to avoid OOM)
            if (exportImages)
                await ProcessImagesForCurrentRecordAsync(page, record, searchDate, i + 1);

            // Free image buffers before allocating for Dataset/CSV
            GC.Collect();
            GC.WaitForPendingFinalizers();

            // Flush after every record. PushData first, then write CSV; then compact LOH to reduce OOM risk
            if (batch.Count >= 1 || i == totalToProcess - 1)
            {
                await ApifyHelper.PushDataAsync(batch);
                WriteBatchToCsvAndFlush(batch);
                batch.Clear();
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                GC.Collect(2, GCCollectionMode.Forced);
                GC.WaitForPendingFinalizers();
            }
        }

        // If there are more result pages: go back to results, next page, then recurse
        if (currentPage > 0 && totalPages > 0 && currentPage < totalPages)
        {
            await ClickBackToResultsAsync(page);
            await ClickNextResultsPageAsync(page);
            await ScrapeAllRecordsOnResultsPageAsync(page, searchDate, outputDirectory, exportImages);
        }
        else
        {
            await ApifyHelper.SetStatusMessageAsync("Success: All records exported to CSV and Dataset.", isTerminal: true);
        }

        return new List<ClermontRecord>();
    }

    /// <summary>
    /// Process all images (pages) for the current record inside the docImgViewFrame viewer.
    /// - Determine total image pages from span#totalPages.
    /// - For each page: render the current image to canvas, capture PNG base64, convert to TIFF, store via Apify KV.
    /// - After each page (except the last), click the "nextPage()" button to move to the next image.
    /// </summary>
    static async Task ProcessImagesForCurrentRecordAsync(IPage page, ClermontRecord record, DateTime searchDate, int recordIndex)
    {
        var imageLinks = new List<string>();

        // Find the docImgViewFrame iframe (image viewer) directly from the frame tree
        var docImgFrame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "docImgViewFrame", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("InstrumentImageViewInternal.jsp", StringComparison.OrdinalIgnoreCase));

        if (docImgFrame == null)
        {
            Console.WriteLine($"[Images] docImgViewFrame not found for record {recordIndex} ({record.DocumentNumber})");
            return;
        }

        // Give the viewer a short time to load
        await Task.Delay(1500);

        // Determine the total image pages from span#totalPages (e.g. " of 2 ")
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

        // Cap image pages per document to avoid OOM (e.g. documents with 50+ pages)
        const int MaxImagePagesPerDocument = 20;
        var pagesToProcess = Math.Min(totalPages, MaxImagePagesPerDocument);
        if (pagesToProcess < totalPages)
            Console.WriteLine($"[Images] Record {recordIndex} capped to {pagesToProcess}/{totalPages} pages to reduce memory.");
        else
            Console.WriteLine($"[Images] Record {recordIndex} ({record.DocumentNumber}) totalPages = {pagesToProcess}");

        if (pagesToProcess <= 0)
            return;

        // Base key for images: Images/yyyy-MM-dd/DocumentNumber/...
        var dateForFilename = searchDate.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
        var baseName = string.IsNullOrWhiteSpace(record.DocumentNumber)
            ? $"record-{recordIndex}"
            : record.DocumentNumber.Replace("/", "_").Replace("\\", "_").Trim();

        // Shared JS: check if image is ready and capture base64
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

        // Scale 0.5 to reduce memory (base64 + PNG + TIFF) and avoid OOM on Apify
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

        for (var pageIndex = 1; pageIndex <= pagesToProcess; pageIndex++)
        {
            // Wait until the image on the current page is fully loaded (poll several times)
            try
            {
                var ready = false;
                for (var attempt = 0; attempt < 25; attempt++)
                {
                    ready = await docImgFrame.EvaluateAsync<bool>(isImageReadyScript);
                    if (ready) break;
                    await Task.Delay(400);
                }

                if (!ready)
                {
                    Console.WriteLine($"[Images] Page {pageIndex}/{pagesToProcess} for record {recordIndex} not ready, skipping.");
                    continue;
                }

                var base64 = await docImgFrame.EvaluateAsync<string?>(captureScript);
                if (!string.IsNullOrEmpty(base64))
                {
                    base64List.Add(base64);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Images] Error processing page {pageIndex} for record {recordIndex}: {ex.Message}");
            }

            // If there is another page, click the Next (nextPage()) button
            if (pageIndex < pagesToProcess)
            {
                try
                {
                    var nextBtn = docImgFrame.Locator("button[onclick*=\"nextPage()\"]");
                    if (await nextBtn.CountAsync() > 0)
                    {
                        await nextBtn.First.ClickAsync();
                        await Task.Delay(1500);
                    }
                    else
                    {
                        break;
                    }
                }
                catch
                {
                    break;
                }
            }
        }

        // --- Merge all collected pages into ONE Multi-page TIF ---
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

                // Cleanup
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

    /// <summary>
    /// In DocumentInfoView (iframe documentFrame), click the Next button (navToDocument('next'))
    /// to move to the next record. Returns true if the click succeeded, false if the button was not found.
    /// </summary>
    static async Task<bool> ClickNextDocumentAsync(IPage page)
    {
        var docFrame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "documentFrame", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("DocumentInfoView", StringComparison.OrdinalIgnoreCase));

        if (docFrame == null)
            return false;

        var nextLink = docFrame.Locator("a[onclick*=\"navToDocument('next')\"]");

        if (await nextLink.CountAsync() == 0)
        {
            // Fallback: parent of img#nextimg
            var parentOfImg = docFrame.Locator("img#nextimg").Locator("xpath=..");
            if (await parentOfImg.CountAsync() == 0)
                return false;

            nextLink = parentOfImg;
        }

        await nextLink.First.ClickAsync();
        await Task.Delay(2000); // wait for DocumentInfoView to load the new record
        return true;
    }


    /// <summary>
    /// On the SearchResultsView page, click the "Go to next result page" button
    /// in the subnav iframe (navbar.do?page=search.resultNav.next&subnav=1&positionTop=0).
    /// </summary>
    static async Task ClickNextResultsPageAsync(IPage page)
    {
        var subnavFrame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "subnav", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("navbar.do?page=search.resultNav.next", StringComparison.OrdinalIgnoreCase));

        if (subnavFrame == null)
        {
            Console.WriteLine("[ResultsNav] subnav frame not found, cannot go to next result page.");
            return;
        }

        var nextPageLink = subnavFrame.Locator(
            "a[onclick*=\"navigateResults('next')\"], a[onclick*=\"navigateResults ('next')\"]");

        if (await nextPageLink.CountAsync() == 0)
        {
            Console.WriteLine("[ResultsNav] Next result page link not found.");
            return;
        }

        await nextPageLink.First.ClickAsync();
        await Task.Delay(2000); // wait for next results page to load
        Console.WriteLine("[ResultsNav] Clicked Go to next result page.");
    }

    /// <summary>
    /// From the detail screen (DocumentInfoView), click the "Back to Results" button
    /// in the resnavframe iframe to return to the SearchResultsView (result list).
    /// </summary>
    static async Task ClickBackToResultsAsync(IPage page)
    {
        var resNavFrame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "resnavframe", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("navbar.do?page=search.details.external", StringComparison.OrdinalIgnoreCase));

        if (resNavFrame == null)
        {
            Console.WriteLine("[ResultsNav] resnavframe not found, cannot click Back to Results.");
            return;
        }

        var backLink = resNavFrame.Locator(
            "a[onclick*=\"executeSearchNav ('results')\"], a[onclick*=\"executeSearchNav('results')\"]");

        if (await backLink.CountAsync() == 0)
        {
            Console.WriteLine("[ResultsNav] Back to Results link not found.");
            return;
        }

        await backLink.First.ClickAsync();
        await Task.Delay(2000); // wait for result list to load
        Console.WriteLine("[ResultsNav] Clicked Back to Results.");
    }

    async Task ClearCookiesAndReloadAsync(IPage page)
    {
        if (_context != null)
        {
            await _context.ClearCookiesAsync();
        }

        await page.EvaluateAsync(@"() => {
            try { localStorage.clear(); } catch(e) {}
            try { sessionStorage.clear(); } catch(e) {}
        }");

        await page.GotoAsync(CountyLoginUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });
    }

    /// <summary>
    /// Disclaimer page displayed inside iframe name="bodyframe".
    /// Select that frame (if present), then wait and click the Accept button (id="accept").
    /// </summary>
    static async Task ClickDisclaimerAcceptAsync(IPage page)
    {
        await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

        // Use disclaimer frame (bodyframe) when present; fall back to main frame otherwise.
        var frame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "bodyframe", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("disclaimer", StringComparison.OrdinalIgnoreCase));
        var root = frame ?? page.MainFrame;

        await root.WaitForLoadStateAsync(LoadState.NetworkIdle);
        await Task.Delay(1000);

        var accept = root.Locator("input#accept[value='Accept']");
        await accept.WaitForAsync(new LocatorWaitForOptions { State = WaitForSelectorState.Visible, Timeout = 30_000 });
        await accept.ClickAsync();
        await root.WaitForLoadStateAsync(LoadState.NetworkIdle);
    }

    /// <summary>
    /// Welcome page displayed inside iframe name="bodyframe"; click "Search Public Records".
    /// Prefer the first datagrid row, but fall back to any element with that text.
    /// </summary>
    static async Task ClickSearchPublicRecordsAsync(IPage page)
    {
        await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

        var frame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "bodyframe", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("welcome", StringComparison.OrdinalIgnoreCase));
        var root = frame ?? page.MainFrame;

        await root.WaitForLoadStateAsync(LoadState.NetworkIdle);
        await Task.Delay(1000);

        // Prefer the first row in the "What would you like to do today?" list (text Search Public Records)
        var menuItem = root.Locator(".datagrid-cell-c1-text:has-text('Search Public Records')");
        if (await menuItem.CountAsync() > 0)
        {
            await menuItem.First.ClickAsync();
            await root.WaitForLoadStateAsync(LoadState.NetworkIdle);
            return;
        }

        // Fallback: any element containing text "Search Public Records" (top tab, link, etc.)
        var anySearchLink = root.GetByText("Search Public Records", new() { Exact = false });
        if (await anySearchLink.CountAsync() > 0)
        {
            await anySearchLink.First.ClickAsync();
            await root.WaitForLoadStateAsync(LoadState.NetworkIdle);
        }
    }

    /// <summary>
    /// Before clicking: wait for NetworkIdle + a short delay so GovOS can set cookies, then click "Login as Public".
    /// </summary>
    static async Task ClickLoginAsPublicAsync(IPage page)
    {
        await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
        await Task.Delay(1000);

        var loginAsPublic = page.Locator("input[value='Login as Public']");
        await loginAsPublic.WaitForAsync(new LocatorWaitForOptions { State = WaitForSelectorState.Visible });
        await loginAsPublic.ClickAsync();
        await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
    }

    /// <summary>
    /// Configure the Search Criteria page from input:
    /// - Header frame (dynSearchFrame): Search Types "All Names", Display (RECSPERPAGE), Document Types (instTree)
    /// - Deepest form frame (criteriaframe): Filter Results (DISTINCTRESULTS)
    ///
    /// Rules:
    /// - Do NOT rely on NetworkIdle; use explicit delays instead.
    /// - Use FrameLocator for the chain bodyframe -> dynSearchFrame -> criteriaframe.
    /// - Interact with each element via Locator.EvaluateAsync with a plain-DOM fallback if EasyUI/jQuery fails.
    /// </summary>
    static async Task SetupSearchPageAsync(IPage page, int display)
    {
        // 1) Get the two relevant iframe "universes"
        var dynSearchFrame = page
            .FrameLocator("iframe[name='bodyframe']")
            .FrameLocator("iframe[name='dynSearchFrame']");

        var criteriaFrame = dynSearchFrame
            .FrameLocator("iframe[name='criteriaframe']");

        // Give the page time to load EasyUI scripts
        await Task.Delay(2000);

        // 2) Search Types: select "All Names" in dynSearchFrame
        var allNames = dynSearchFrame.Locator(".SEARCHTYPE_datagrid-cell-c2-text span.base:has-text(\"All Names\")");
        if (await allNames.CountAsync() > 0)
        {
            await allNames.First.ClickAsync();
        }

        // 3) Display (RECSPERPAGE) trong dynSearchFrame
        var recsPerPage = dynSearchFrame.Locator("#RECSPERPAGE");
        if (await recsPerPage.CountAsync() > 0)
        {
            await recsPerPage.First.EvaluateAsync(
                @"(node, d) => {
                    try {
                        var w = window;
                        var jq = (w.$ || w.jQuery);
                        if (jq && jq(node).combobox) {
                            // EasyUI combobox API: updates select + textbox + hidden
                            jq(node).combobox('setValue', d.toString());
                            return;
                        }
                        // Plain DOM fallback
                        node.value = d.toString();
                        var span = node.nextElementSibling;
                        if (span) {
                            var visible = span.querySelector('input.textbox-text');
                            if (visible) visible.value = d.toString();
                            var hidden = span.querySelector(""input.textbox-value[name='RECSPERPAGE']"");
                            if (hidden) hidden.value = d.toString();
                        }
                    } catch (e) {}
                }",
                display);
        }

        // 4) Document Types: ensure 'All Document Types' is checked (instTree) inside dynSearchFrame
        var instTree = dynSearchFrame.Locator("#instTree");
        if (await instTree.CountAsync() > 0)
        {
            await instTree.First.EvaluateAsync(
                @"(node) => {
                    try {
                        var w = window;
                        var jq = (w.$ || w.jQuery);
                        if (!jq) return;
                        if (!jq(node).tree) return;
                        var rootNode = jq(node).tree('getRoot');
                        if (rootNode) {
                            jq(node).tree('check', rootNode.target);
                        }
                    } catch (e) {}
                }");
        }

        // 5) Filter Results: 'One Row per Document' (DISTINCTRESULTS) in the deepest criteriaframe
        var distinct = criteriaFrame.Locator("#DISTINCTRESULTS");
        if (await distinct.CountAsync() > 0)
        {
            await distinct.First.EvaluateAsync(
                @"(node) => {
                    try {
                        var w = window;
                        var jq = (w.$ || w.jQuery);
                        if (jq && jq(node).combobox) {
                            jq(node).combobox('setValue', 'true');
                            return;
                        }
                        // Fallback DOM: locate hidden textbox-value[name='DISTINCTRESULTS']
                        var span = node.nextElementSibling;
                        if (span) {
                            var hidden = span.querySelector(""input.textbox-value[name='DISTINCTRESULTS']"");
                            if (hidden) hidden.value = 'true';
                            var visible = span.querySelector('input.textbox-text');
                            if (visible) visible.value = 'true';
                        }
                    } catch (e) {}
                }");
        }

        // Give internal onChange/onLoad scripts time to run
        await Task.Delay(1500);
    }

    /// <summary>
    /// Get the iframe that contains the Search Criteria form.
    /// In GovOS this is bodyframe -> dynSearchFrame -> criteriaframe(dynCriteria.do).
    /// Here we only return the innermost iframe: criteriaframe / dyncriteria, not dynSearchFrame.
    /// </summary>
    static IFrame? GetSearchFrame(IPage page)
    {
        return page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "criteriaframe", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("/dyncriteria/dynCriteria", StringComparison.OrdinalIgnoreCase));
    }

    /// <summary>
    /// Wait until the Search Criteria iframe (criteriaframe/dynCriteria) appears.
    /// We do not fall back to mainFrame to avoid finding #FROMDATE in the wrong context.
    /// </summary>
    static async Task<IFrame> WaitForSearchFrameAsync(IPage page, int timeoutMs = 30000)
    {
        var start = DateTime.UtcNow;
        while (true)
        {
            var frame = GetSearchFrame(page);
            if (frame != null)
                return frame;

            if ((DateTime.UtcNow - start).TotalMilliseconds > timeoutMs)
                throw new TimeoutException("Search Criteria iframe (criteriaframe/dynCriteria) not found.");

            await Task.Delay(500);
        }
    }

   /// <summary>
    /// Traverse the nested GovOS iframe structure: bodyframe -> dynSearchFrame -> criteriaframe,
    /// then set the date values via JavaScript directly on the hidden input elements.
    /// </summary>
    static async Task SetDateRangeForDayAsync(IPage page, DateTime date)
    {
        var dateStr = date.ToString("MM/dd/yyyy");

        // 1. Traverse the iframe layers according to the DOM structure
        var frame = page.FrameLocator("iframe[name='bodyframe']")
                        .FrameLocator("iframe[name='dynSearchFrame']")
                        .FrameLocator("iframe[name='criteriaframe']");

        // 2. Locate the underlying hidden inputs directly
        var fromLocator = frame.Locator("#FROMDATE");
        var toLocator = frame.Locator("#TODATE");
        var rangeLocator = frame.Locator("#daterange_TODATE");

        // 3. Wait for the DOM to attach the element (use Attached because the element is hidden)
        await fromLocator.WaitForAsync(new LocatorWaitForOptions { State = WaitForSelectorState.Attached, Timeout = 15_000 });

        // 4. Execute JavaScript on the input itself (deepest iframe context)
        await fromLocator.EvaluateAsync(@"(node, d) => {
            if (typeof window.$ !== 'undefined' && window.$(node).datebox) {
                // EasyUI API: setValue automatically updates hidden input and visible text
                window.$(node).datebox('setValue', d);
            } else {
                // Plan B: manipulate plain DOM if EasyUI is not available
                node.value = d; 
                var visibleInput = node.nextElementSibling ? node.nextElementSibling.querySelector('input.textbox-text') : null;
                if (visibleInput) visibleInput.value = d;
                var hiddenVal = node.nextElementSibling ? node.nextElementSibling.querySelector('input.textbox-value') : null;
                if (hiddenVal) hiddenVal.value = d;
            }
        }", dateStr);

        // 5. Do the same for the To date (TODATE)
        await toLocator.EvaluateAsync(@"(node, d) => {
            if (typeof window.$ !== 'undefined' && window.$(node).datebox) {
                window.$(node).datebox('setValue', d);
            } else {
                node.value = d; 
                var visibleInput = node.nextElementSibling ? node.nextElementSibling.querySelector('input.textbox-text') : null;
                if (visibleInput) visibleInput.value = d;
                var hiddenVal = node.nextElementSibling ? node.nextElementSibling.querySelector('input.textbox-value') : null;
                if (hiddenVal) hiddenVal.value = d;
            }
        }", dateStr);

        // 6. Change dropdown to 'User Defined' to prevent the site from resetting dates
        if (await rangeLocator.CountAsync() > 0)
        {
            await rangeLocator.EvaluateAsync(@"(node) => {
                if (typeof window.$ !== 'undefined' && window.$(node).combobox) {
                    window.$(node).combobox('setValue', 'User Defined');
                }
            }");
        }

        // Wait 1.5 seconds for any internal onChange events to complete
        await Task.Delay(1500);
    }

    /// <summary>
    /// Click the Search button in the header (inside iframe dynSearchFrame, NOT criteriaframe).
    /// </summary>
    static async Task ClickSearchAsync(IPage page)
    {
        // Header search (Clear/Search + message) is in dynSearchFrame.
        var headerFrame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "dynSearchFrame", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("searchCriteria.do", StringComparison.OrdinalIgnoreCase));
        if (headerFrame == null) return;

        await headerFrame.WaitForLoadStateAsync(LoadState.NetworkIdle);
        await Task.Delay(500);

        var searchBtn = headerFrame.Locator("#imgSearch, a[onclick*=\"executeSearchCommand\"][onclick*=\"search\"]");
        if (await searchBtn.CountAsync() > 0)
        {
            await searchBtn.First.ClickAsync();
            await headerFrame.WaitForLoadStateAsync(LoadState.NetworkIdle);

            // After search: if dynSearchFrame displays the message "No documents were found..."
            // then print that message to the console.
            await PrintNoDocumentsMessageIfAnyAsync(page);
        }
    }

    /// <summary>
    /// If dynSearchFrame displays the message "No documents were found that match the specified criteria."
    /// then (wait up to a few seconds), print that message to the console and exit the program.
    /// </summary>
    static async Task PrintNoDocumentsMessageIfAnyAsync(IPage page)
    {
        var frame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "dynSearchFrame", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("searchCriteria.do", StringComparison.OrdinalIgnoreCase));
        if (frame == null) return;

        var msgSpan = frame.Locator("#msgDiv span#displayMsg");
        if (await msgSpan.CountAsync() == 0) return;

        var span = msgSpan.First;

        // Wait up to 10 seconds for the message to display after clicking Search
        try
        {
            await span.WaitForAsync(new LocatorWaitForOptions
            {
                State = WaitForSelectorState.Visible,
                Timeout = 10_000
            });
        }
        catch
        {
            // If the timeout is reached and the message is still not visible, consider it as no message to process.
            return;
        }

        var text = (await span.InnerTextAsync()).Trim();
        if (!string.IsNullOrEmpty(text))
        {
            Console.WriteLine(text);
            // No documents match the criteria -> exit the program.
            Environment.Exit(0);
        }
    }

    /// <summary>
    /// Search by day: from startDate to endDate (each day set From=To=day, then click Search).
    /// (Currently only a skeleton, not integrated with CSV export).
    /// </summary>
    static async Task RunDailySearchesAsync(IPage page, DateTime startDate, DateTime endDate)
    {
        var frame = GetSearchFrame(page);
        var root = frame ?? page.MainFrame;

        for (var d = startDate.Date; d <= endDate.Date; d = d.AddDays(1))
        {
            await SetDateRangeForDayAsync(page, d);
            await ClickSearchAsync(page);
            // TODO: wait for result frame to load and scrape the results
            await Task.Delay(2000);
        }
    }

    /// <summary>Open CSV stream once for the run (like OH-Montgomery). Call before ScrapeAllRecordsOnResultsPageAsync.</summary>
    void OpenCsvStreamForRun(DateTime searchDate, string outputDirectory)
    {
        if (_csvWriter != null) return;

        var baseDir = GetBaseDirForStorage(outputDirectory);
        var kvStoreDir = Path.Combine(baseDir, "apify_storage", "key_value_store");
        Directory.CreateDirectory(kvStoreDir);
        var fileName = $"OH-Clermont_{searchDate:MM-dd-yyyy}.csv";
        var filePath = Path.Combine(kvStoreDir, fileName);
        var fileExists = File.Exists(filePath);

        var config = ClermontRecord.CreateCsvConfiguration();
        config.HasHeaderRecord = !fileExists;
        _csvStreamWriter = new StreamWriter(filePath, append: fileExists);
        _csvWriter = new CsvWriter(_csvStreamWriter, config);
    }

    /// <summary>Write batch to the already-open CSV and flush (no extra allocations).</summary>
    void WriteBatchToCsvAndFlush(List<ClermontRecord> batch)
    {
        if (_csvWriter == null || _csvStreamWriter == null || batch.Count == 0) return;
        _csvWriter.WriteRecords(batch);
        _csvStreamWriter.Flush();
    }

    /// <summary>Close CSV stream. Call after scrape or in StopAsync.</summary>
    void CloseCsvStream()
    {
        try
        {
            _csvStreamWriter?.Flush();
            _csvWriter?.Dispose();
        }
        catch { }
        _csvWriter = null;
        try
        {
            _csvStreamWriter?.Dispose();
        }
        catch { }
        _csvStreamWriter = null;
    }

    static string GetBaseDirForStorage(string outputDirectory)
    {
        var baseDir = string.IsNullOrEmpty(outputDirectory) ? Directory.GetCurrentDirectory() : outputDirectory;
        var sep = Path.DirectorySeparatorChar;
        if (baseDir.Contains(sep + "bin" + sep, StringComparison.Ordinal))
        {
            var parts = baseDir.Split(sep);
            var binIdx = Array.FindLastIndex(parts, p => string.Equals(p, "bin", StringComparison.OrdinalIgnoreCase));
            if (binIdx > 0) baseDir = string.Join(sep, parts.Take(binIdx));
        }
        return baseDir;
    }

    /// <summary>
    /// Export the list of records to a CSV file (standalone; used when not using the single-stream flow).
    /// Prefer OpenCsvStreamForRun + WriteBatchToCsvAndFlush for the main run to avoid OOM.
    /// </summary>
    public Task ExportToCsvAsync(List<ClermontRecord> records, DateTime searchDate, string outputDirectory)
    {
        records ??= new List<ClermontRecord>();
        var baseDir = GetBaseDirForStorage(outputDirectory);
        var kvStoreDir = Path.Combine(baseDir, "apify_storage", "key_value_store");
        Directory.CreateDirectory(kvStoreDir);
        var fileName = $"OH-Clermont_{searchDate:MM-dd-yyyy}.csv";
        var filePath = Path.Combine(kvStoreDir, fileName);
        var fileExists = File.Exists(filePath);
        var config = ClermontRecord.CreateCsvConfiguration();
        config.HasHeaderRecord = !fileExists;
        using (var writer = new StreamWriter(filePath, append: fileExists))
        using (var csv = new CsvWriter(writer, config))
        {
            csv.WriteRecords(records);
        }
        return Task.CompletedTask;
    }

    /// <summary>
    /// Close browser/context and CSV stream (try to click "Log Out" if there is still a page).
    /// </summary>
    public async Task StopAsync()
    {
        CloseCsvStream();
        if (_context != null)
        {
            try
            {
                var pages = _context.Pages;
                if (pages.Count > 0)
                {
                    var page = pages[0];
                    var logOut = page.Locator("a:has-text('Log Out')");
                    if (await logOut.CountAsync() > 0)
                    {
                        await logOut.First.ClickAsync();
                        await Task.Delay(500);
                    }
                }
            }
            catch
            {
                // Ignore if Log Out is not found or the page is already closed
            }

            await _context.CloseAsync();
        }

        if (_browser != null) await _browser.CloseAsync();
        _playwright?.Dispose();
        _context = null;
        _browser = null;
        _playwright = null;
    }
}
