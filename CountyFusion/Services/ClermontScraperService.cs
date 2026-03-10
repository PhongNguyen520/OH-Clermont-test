using System.Collections.Generic;
using System.IO;
using System.Runtime;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using CountyFusion;
using CountyFusion.Models;
using CountyFusion.Utils;

namespace CountyFusion.Services;

/// <summary>Playwright-based scraper for Clermont County. Handles login, session errors, and public records search.</summary>
public class ClermontScraperService
{
    const string TimeoutErrorSnippet = "This form has already been processed or the session timed out";
    const string ActiveSessionSnippet = "Login Failed: Active Session.";

    IPlaywright? _playwright;
    IBrowser? _browser;
    IBrowserContext? _context;
    CsvExportHelper? _csvExportHelper;
    string _currentLoginUrl = "https://countyfusion2.govos.com/countyweb/loginDisplay.action?countyname=ClermontOH";
    int _effectiveDisplay = 500;

    /// <summary>Launches browser, opens login page, navigates to Search Criteria and fills from InputConfig.</summary>
    public async Task<IPage> LaunchAsync(InputConfig config)
    {
        config ??= new InputConfig();

        var searchType = (config.SearchType ?? "Date").Trim();
        var isInstrumentSearch = string.Equals(searchType, "Instrument", StringComparison.OrdinalIgnoreCase);

        await ApifyHelper.SetStatusMessageAsync("Starting CountyFusion scraper...");
        _currentLoginUrl = ResolveLoginUrl(config.CountyName);
        _effectiveDisplay = GetMaxDisplayForCounty(config.CountyName);

        DateTime fromDate;
        try
        {
            fromDate = DomHelper.ParseDate(config.FromDate, "fromDate");
            DomHelper.ParseDate(config.ToDate, "toDate");
        }
        catch (ArgumentException ex)
        {
            await ApifyHelper.SetStatusMessageAsync($"Validation Error: {ex.Message}", isTerminal: true);
            throw;
        }

        _playwright = await Playwright.CreateAsync();

        var isApify = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("APIFY_CONTAINER_PORT"));
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

        _context = await _browser.NewContextAsync(new BrowserNewContextOptions
        {
            IgnoreHTTPSErrors = true
        });

        var page = await _context.NewPageAsync();
        page.SetDefaultTimeout(30_000);

        await page.GotoAsync(_currentLoginUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

        if (await DomHelper.HasErrorSnippetAsync(page, TimeoutErrorSnippet))
        {
            await ClearCookiesAndReloadAsync(page);
        }

        await FormFiller.ClickLoginAsPublicAsync(page);

        if (await DomHelper.HasErrorSnippetAsync(page, ActiveSessionSnippet))
        {
            await ClearCookiesAndReloadAsync(page);
            await FormFiller.ClickLoginAsPublicAsync(page);
        }

        await FormFiller.ClickDisclaimerAcceptAsync(page);
        await FormFiller.ClickSearchPublicRecordsAsync(page);

        // Build instrument list if needed
        var instrumentList = new List<string>();
        if (isInstrumentSearch)
        {
            var rawList = config.InstrumentNumbers ?? string.Empty;
            instrumentList = rawList
                .Split(new[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries)
                .Select(s => s.Trim())
                .Where(s => !string.IsNullOrWhiteSpace(s))
                .ToList();

            if (instrumentList.Count == 0)
            {
                await ApifyHelper.SetStatusMessageAsync(
                    "SearchType is 'Instrument' but no InstrumentNumbers were provided. Falling back to Date search.",
                    isTerminal: false);
                isInstrumentSearch = false;
            }
        }

        if (isInstrumentSearch)
        {
            var outputDirectory = Directory.GetCurrentDirectory();
            var exportImages = config.ExportImages;
            if (!exportImages)
                Console.WriteLine("[Scrape] ExportImages=false: skipping image download to reduce memory.");

            _csvExportHelper = new CsvExportHelper();
            _csvExportHelper.OpenCsvStreamForRun(fromDate, outputDirectory);

            int totalAll = 0, succeededAll = 0, failedAll = 0;

            try
            {
                for (var idx = 0; idx < instrumentList.Count; idx++)
                {
                    var instrument = instrumentList[idx];

                    if (idx > 0)
                    {
                        // For subsequent instruments, start a fresh session on the search form
                        await ClearCookiesAndReloadAsync(page);
                        await FormFiller.ClickLoginAsPublicAsync(page);
                        await FormFiller.ClickDisclaimerAcceptAsync(page);
                        await FormFiller.ClickSearchPublicRecordsAsync(page);
                    }

                    await FormFiller.SetupInstrumentSearchAsync(page, instrument, _effectiveDisplay);

                    const int searchRetriesPerInstrument = 3;
                    var searchSuccessForInstrument = false;

                    for (int attempt = 1; attempt <= searchRetriesPerInstrument; attempt++)
                    {
                        try
                        {
                            if (attempt > 1)
                            {
                                await ApifyHelper.SetStatusMessageAsync(
                                    $"Instrument {instrument}: search attempt {attempt} of {searchRetriesPerInstrument}...");
                                await page.ReloadAsync(new PageReloadOptions { WaitUntil = WaitUntilState.NetworkIdle });
                                await Task.Delay(2000);
                                await FormFiller.SetupInstrumentSearchAsync(page, instrument, _effectiveDisplay);
                            }

                            await FormFiller.ClickSearchAsync(page);
                            searchSuccessForInstrument = true;
                            break;
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Instrument {instrument}] Attempt {attempt} failed: {ex.Message}");
                            if (attempt == searchRetriesPerInstrument)
                            {
                                await ApifyHelper.SetStatusMessageAsync(
                                    $"Fatal Error during search for instrument {instrument} after {searchRetriesPerInstrument} attempts: {ex.Message}",
                                    isTerminal: false);
                            }
                            await Task.Delay(5000);
                        }
                    }

                    if (!searchSuccessForInstrument)
                        continue;

                    var (total, succeeded, failed) = await ScrapeAllRecordsOnResultsPageAsync(
                        page,
                        fromDate,
                        outputDirectory,
                        exportImages,
                        config.FileFormat,
                        isInstrumentSearch: true,
                        instrumentNumber: instrument);

                    totalAll += total;
                    succeededAll += succeeded;
                    failedAll += failed;
                }

                await ApifyHelper.SetStatusMessageAsync(
                    $"Finished! Total {totalAll} requests: {succeededAll} succeeded, {failedAll} failed.",
                    isTerminal: true);
                Console.WriteLine("[Scrape] Done. Records were pushed to Dataset and appended to CSV.");
            }
            finally
            {
                _csvExportHelper.CloseCsvStream();
            }

            return page;
        }
        else
        {
            await FormFiller.SetupSearchPageAsync(page, _effectiveDisplay);

            int searchRetries = 3;
            bool searchSuccess = false;

            for (int attempt = 1; attempt <= searchRetries; attempt++)
            {
                try
                {
                    if (attempt > 1)
                    {
                        await ApifyHelper.SetStatusMessageAsync($"Search attempt {attempt} of {searchRetries}...");
                        await page.ReloadAsync(new PageReloadOptions { WaitUntil = WaitUntilState.NetworkIdle });
                        await Task.Delay(2000);
                        await FormFiller.SetupSearchPageAsync(page, _effectiveDisplay);
                    }

                    await FormFiller.SetDateRangeForDayAsync(page, fromDate);
                    await Task.Delay(1500);
                    await FormFiller.ClickSearchAsync(page);

                    searchSuccess = true;
                    break;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[Attempt {attempt}] Search failed: {ex.Message}");
                    if (attempt == searchRetries)
                    {
                        await ApifyHelper.SetStatusMessageAsync(
                            $"Fatal Error during search after {searchRetries} attempts: {ex.Message}",
                            isTerminal: true);
                        throw;
                    }
                    await Task.Delay(5000);
                }
            }

            if (!searchSuccess) return page;

            var outputDirectory = Directory.GetCurrentDirectory();
            var exportImages = config.ExportImages;
            if (!exportImages)
                Console.WriteLine("[Scrape] ExportImages=false: skipping image download to reduce memory.");

            _csvExportHelper = new CsvExportHelper();
            _csvExportHelper.OpenCsvStreamForRun(fromDate, outputDirectory);

            try
            {
                var (total, succeeded, failed) = await ScrapeAllRecordsOnResultsPageAsync(
                    page,
                    fromDate,
                    outputDirectory,
                    exportImages,
                    config.FileFormat);
                await ApifyHelper.SetStatusMessageAsync(
                    $"Finished! Total {total} requests: {succeeded} succeeded, {failed} failed.",
                    isTerminal: true);
                Console.WriteLine("[Scrape] Done. Records were pushed to Dataset and appended to CSV.");
            }
            finally
            {
                _csvExportHelper.CloseCsvStream();
            }

            return page;
        }
    }

    /// <summary>Scrape Instrument Info from DocumentInfoView detail page into ClermontRecord with retries until core labels are present.</summary>
    public static async Task<ClermontRecord> ScrapeInstrumentInfoAsync(IPage page)
    {
        const int maxAttempts = 3;
        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            var record = await ScrapeInstrumentInfoInternalAsync(page);
            if (!string.IsNullOrWhiteSpace(record.DocumentNumber))
                return record;

            if (attempt < maxAttempts)
                await Task.Delay(2000);
        }

        throw new InvalidOperationException("Failed to scrape Instrument Info after multiple attempts: Document Number is empty.");
    }

    static async Task<ClermontRecord> ScrapeInstrumentInfoInternalAsync(IPage page)
    {
        var docInfoFrame = page
            .FrameLocator("iframe[name='bodyframe']")
            .FrameLocator("iframe[name='documentFrame']")
            .FrameLocator("iframe[name='docInfoFrame']");

        await Task.Delay(2000);

        try
        {
            var instrumentNumberLabel = docInfoFrame.Locator("span.base:has-text(\"Instrument Number:\")");
            await instrumentNumberLabel.First.WaitForAsync(new LocatorWaitForOptions
            {
                State = WaitForSelectorState.Visible,
                Timeout = 10_000
            });
        }
        catch
        {
        }

        var record = new ClermontRecord
        {
            Amount = string.Empty,
            Remarks = string.Empty,
            ParcelNumber = string.Empty,
            PropertyAddress = string.Empty
        };

        for (var i = 0; i < 30; i++)
        {
            var raw = (await DomHelper.GetByLabelAsync(docInfoFrame, "Instrument Number:")).Trim();
            record.DocumentNumber = DomHelper.ExtractInstrumentNumber(raw);
            if (string.IsNullOrWhiteSpace(record.DocumentNumber) && raw.Length > 0 && raw.Length < 30 &&
                !raw.Contains("prevInstRowInfo", StringComparison.OrdinalIgnoreCase) && !raw.Contains('\n'))
                record.DocumentNumber = raw.Trim();
            if (!string.IsNullOrWhiteSpace(record.DocumentNumber))
                break;
            await Task.Delay(500);
        }
        record.BookType = (await DomHelper.GetByLabelAsync(docInfoFrame, "Book Type:")).Trim();

        var bookPage = await DomHelper.GetByLabelAsync(docInfoFrame, "Book / Page:");
        if (!string.IsNullOrWhiteSpace(bookPage))
        {
            var parts = bookPage.Split('/');
            if (parts.Length > 0)
                record.Book = parts[0].Trim();
            if (parts.Length > 1)
            {
                var rawPage = parts[1];
                var tokens = rawPage
                    .Split(new[] { ' ', '\n', '\r', '\t', '\u00A0' }, StringSplitOptions.RemoveEmptyEntries);
                var cleanPage = tokens.FirstOrDefault();
                record.Page = cleanPage?.Trim() ?? string.Empty;
            }
        }
        if (string.IsNullOrWhiteSpace(record.Book))
            record.Book = (await DomHelper.GetByLabelAsync(docInfoFrame, "Book:")).Trim();
        if (string.IsNullOrWhiteSpace(record.Page))
            record.Page = (await DomHelper.GetByLabelAsync(docInfoFrame, "Page:")).Trim();

        var recDateRaw = await DomHelper.GetByLabelAsync(docInfoFrame, "Recorded Date:");
        if (!string.IsNullOrWhiteSpace(recDateRaw))
        {
            var idx = recDateRaw.IndexOf(' ');
            record.RecordingDate = idx > 0 ? recDateRaw[..idx].Trim() : recDateRaw.Trim();
        }

        record.DocumentType = (await DomHelper.GetByLabelAsync(docInfoFrame, "Instrument Type:")).Trim();
        record.InstrumentDate = (await DomHelper.GetByLabelAsync(docInfoFrame, "Document Date:")).Trim();
        if (string.IsNullOrWhiteSpace(record.InstrumentDate))
            record.InstrumentDate = (await DomHelper.GetByLabelAsync(docInfoFrame, "Instrument Date:")).Trim();

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
        catch { }

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
        catch { }

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
        catch { }

        try
        {
            var documentFrame = page
                .FrameLocator("iframe[name='bodyframe']")
                .FrameLocator("iframe[name='documentFrame']");
            var tabsFrame = documentFrame.FrameLocator("iframe[name='tabs'], iframe#tabs");
            var legalTab = tabsFrame.Locator(".tabs-title:has-text(\"Legal Description\")");
            if (await legalTab.CountAsync() == 0)
                legalTab = tabsFrame.Locator(".tabs-title:has-text(\"Property / Legal\")");

            if (await legalTab.CountAsync() > 0)
            {
                await legalTab.First.ClickAsync();
                await Task.Delay(1500);
            }
            else
            {
                var tabsInDocInfo = docInfoFrame.FrameLocator("iframe[name='tabs'], iframe#tabs");
                legalTab = tabsInDocInfo.Locator(".tabs-title:has-text(\"Legal Description\")");
                if (await legalTab.CountAsync() == 0)
                    legalTab = tabsInDocInfo.Locator(".tabs-title:has-text(\"Property / Legal\")");
                if (await legalTab.CountAsync() > 0)
                {
                    await legalTab.First.ClickAsync();
                    await Task.Delay(1500);
                }
            }
        }
        catch { }

        for (var waitAttempt = 0; waitAttempt < 15; waitAttempt++)
        {
            try
            {
                var legalCells = docInfoFrame.Locator("td.basesm");
                if (await legalCells.CountAsync() > 0)
                {
                    var firstText = (await legalCells.First.InnerTextAsync()).Trim();
                    if (firstText.Length > 2)
                        break;
                }
            }
            catch { }
            await Task.Delay(400);
        }

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
        catch { }

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
        catch { }

        const int MaxFieldLength = 32768;
        if (record.Legal.Length > MaxFieldLength)
            record.Legal = record.Legal.Substring(0, MaxFieldLength) + "... [truncated]";
        if (record.LongDescription.Length > MaxFieldLength)
            record.LongDescription = record.LongDescription.Substring(0, MaxFieldLength) + "... [truncated]";

        return record;
    }

    /// <summary>
    /// Fallback: scrape a single result row from the results table (no detail view).
    /// Used when detail page keeps timing out – we still emit a partial record instead of skipping.
    /// </summary>
    static async Task<ClermontRecord?> TryScrapeResultRowSummaryAsync(IPage page, int index)
    {
        try
        {
            if (index < 0)
                return null;

            var bodyFrame = page.FrameLocator("iframe[name='bodyframe']");
            var resultFrame = bodyFrame.FrameLocator("iframe[name='resultFrame']");
            var resultListFrame = resultFrame.FrameLocator("iframe[name='resultListFrame']");

            var rowLocator = resultListFrame.Locator(
                "#instList .datagrid-view2 .datagrid-body .datagrid-btable tr.datagrid-row").Nth(index);

            if (await rowLocator.CountAsync() == 0)
                return null;

            static async Task<string> GetCellTextAsync(ILocator row, string fieldSelector)
            {
                var cell = row.Locator(fieldSelector);
                if (await cell.CountAsync() == 0)
                    return string.Empty;
                var text = (await cell.First.InnerTextAsync()) ?? string.Empty;
                return text.Replace("\u00A0", " ").Trim();
            }

            var record = new ClermontRecord();

            // Instrument # (Document Number)
            var instCell = rowLocator.Locator("td[field='2'] a");
            if (await instCell.CountAsync() > 0)
            {
                var instText = (await instCell.First.InnerTextAsync()) ?? string.Empty;
                instText = instText.Replace("\u00A0", " ").Trim();
                record.DocumentNumber = DomHelper.ExtractInstrumentNumber(instText);
                if (string.IsNullOrWhiteSpace(record.DocumentNumber))
                    record.DocumentNumber = instText;
            }

            // Book, Page, Document Type
            record.Book = await GetCellTextAsync(rowLocator, "td[field='3']");
            record.Page = await GetCellTextAsync(rowLocator, "td[field='4']");
            record.DocumentType = await GetCellTextAsync(rowLocator, "td[field='5']");

            // Names: map primary Name -> Grantor, Other Name -> Grantee (best effort)
            var grantorText = await GetCellTextAsync(rowLocator, "td[field='7'] span");
            var granteeText = await GetCellTextAsync(rowLocator, "td[field='9'] span");
            record.Grantor = grantorText;
            record.Grantee = granteeText;

            // Recorded date and Legal description
            record.RecordingDate = await GetCellTextAsync(rowLocator, "td[field='10']");
            record.Legal = await GetCellTextAsync(rowLocator, "td[field='11']");

            record.Remarks = "[PARTIAL] Detail page not accessible; data scraped from results list row only.";

            return record;
        }
        catch
        {
            return null;
        }
    }

    /// <summary>
    /// Scrape all records on results page: click first, then Next, flush after each record.
    /// When isInstrumentSearch = true, recovery logic will re-run Instrument search instead of Date search.
    /// </summary>
    public async Task<(int total, int succeeded, int failed)> ScrapeAllRecordsOnResultsPageAsync(
        IPage page,
        DateTime searchDate,
        string outputDirectory,
        bool exportImages = true,
        string fileFormat = "tif",
        bool isInstrumentSearch = false,
        string? instrumentNumber = null)
    {
        var batch = new List<ClermontRecord>(capacity: 1);

        var bodyFrame = page.FrameLocator("iframe[name='bodyframe']");
        var resultFrame = bodyFrame.FrameLocator("iframe[name='resultFrame']");
        var resultListFrame = resultFrame.FrameLocator("iframe[name='resultListFrame']");

        await Task.Delay(2000);

        const string instrumentSelector =
            "#instList .datagrid-view2 .datagrid-body .datagrid-btable tr.datagrid-row td[field='2'] a";

        int pageCount = 0;
        try
        {
            var navDisplay = resultFrame.Locator("td#navDisplay");
            if (await navDisplay.CountAsync() > 0)
            {
                var navText = (await navDisplay.First.InnerTextAsync()).Trim();
                pageCount = DomHelper.GetPageCountFromNavDisplay(navText);
            }
        }
        catch
        {
            pageCount = 0;
        }

        int currentPage = 0, totalPages = 0;
        try
        {
            var pageInfoCell = resultFrame.Locator("td.msg1:has-text(\"Page\")");
            if (await pageInfoCell.CountAsync() > 0)
            {
                var pageText = (await pageInfoCell.First.InnerTextAsync()).Trim();
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

        var linkCount = await resultListFrame.Locator(instrumentSelector).CountAsync();
        var totalToProcess = pageCount > 0 ? Math.Min(pageCount, linkCount) : linkCount;

        Console.WriteLine($"[Results] navDisplay pageCount = {pageCount}, instrumentLinks = {linkCount}, totalToProcess = {totalToProcess}, currentPage = {currentPage}, totalPages = {totalPages}");

        if (totalToProcess == 0)
            return (0, 0, 0);

        await ApifyHelper.SetStatusMessageAsync("Found records. Preparing to extract...");

        var pageSucceeded = 0;
        var pageFailed = 0;
        var recoveryAttemptedForIndex = -1;
        ClermontRecord? recoveryRowSummary = null;

        for (var i = 0; i < totalToProcess; i++)
        {
            var alreadyRecoveredForThisRecord = recoveryAttemptedForIndex == i;

            if ((i + 1) % 10 == 0 || pageFailed > 0)
            {
                var status = pageFailed > 0
                    ? $"Processing record {i + 1} of {totalToProcess}... {pageFailed} failed"
                    : $"Processing record {i + 1} of {totalToProcess} on current page...";
                await ApifyHelper.SetStatusMessageAsync(status);
            }

            try
            {
                try
                {
                    if (i == 0)
                    {
                        var firstLink = resultListFrame.Locator(instrumentSelector).First;
                        await firstLink.ScrollIntoViewIfNeededAsync();
                        await firstLink.ClickAsync();
                    }
                    else
                    {
                        var moved = await FormFiller.ClickNextDocumentAsync(page);
                        if (!moved)
                        {
                            var clicked = await FormFiller.ClickInstrumentAtIndexAsync(page, i);
                            if (!clicked)
                            {
                                pageFailed++;
                                await ApifyHelper.SetStatusMessageAsync($"Processing record {i + 1} of {totalToProcess}... {pageFailed} failed");
                                break;
                            }
                        }
                    }

                    var record = await ScrapeInstrumentInfoAsync(page);
                    batch.Add(record);

                    if (exportImages)
                        await ImageProcessor.ProcessImagesForCurrentRecordAsync(page, record, searchDate, i + 1, fileFormat);

                    pageSucceeded++;
                    recoveryAttemptedForIndex = -1;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CountyFusion] Error processing record {i + 1}: {ex.Message}");

                    // Capture summary info from the results table before any heavy recovery,
                    // so we can still emit a partial row if detail view keeps failing.
                    if (recoveryRowSummary == null || recoveryAttemptedForIndex != i)
                    {
                        recoveryRowSummary = await TryScrapeResultRowSummaryAsync(page, i);
                        if (recoveryRowSummary != null)
                        {
                            Console.WriteLine($"[CountyFusion] Captured summary info for record {i + 1} from results list for potential fallback.");
                        }
                    }

                    if (!alreadyRecoveredForThisRecord)
                    {
                        recoveryAttemptedForIndex = i;
                        Console.WriteLine($"[CountyFusion] Attempting recovery (once)...");
                        try
                        {
                            await ClearCookiesAndReloadAsync(page);
                            await FormFiller.ClickLoginAsPublicAsync(page);
                            await FormFiller.ClickDisclaimerAcceptAsync(page);
                            await FormFiller.ClickSearchPublicRecordsAsync(page);

                            if (isInstrumentSearch && !string.IsNullOrWhiteSpace(instrumentNumber))
                            {
                                await FormFiller.SetupInstrumentSearchAsync(page, instrumentNumber, _effectiveDisplay);
                                await FormFiller.ClickSearchAsync(page);
                            }
                            else
                            {
                                await FormFiller.SetupSearchPageAsync(page, _effectiveDisplay);
                                await FormFiller.SetDateRangeForDayAsync(page, searchDate);
                                await Task.Delay(1500);
                                await FormFiller.ClickSearchAsync(page);
                            }

                            bodyFrame = page.FrameLocator("iframe[name='bodyframe']");
                            resultFrame = bodyFrame.FrameLocator("iframe[name='resultFrame']");
                            resultListFrame = resultFrame.FrameLocator("iframe[name='resultListFrame']");

                        if (currentPage > 1 && totalPages >= currentPage)
                        {
                            for (var pageIndex = 1; pageIndex < currentPage; pageIndex++)
                            {
                                Console.WriteLine($"[Recovery] Navigating to page {pageIndex + 1} of {currentPage}...");
                                await FormFiller.ClickNextResultsPageAsync(page);
                            }
                        }

                            var links = resultListFrame.Locator(instrumentSelector);
                            var linkCountAfterRecovery = await links.CountAsync();
                            if (linkCountAfterRecovery > i)
                            {
                                var link = links.Nth(i);
                                await link.ScrollIntoViewIfNeededAsync();
                                await link.ClickAsync();
                                i--;
                            }
                            else
                            {
                                if (recoveryRowSummary != null && recoveryAttemptedForIndex == i)
                                {
                                    Console.WriteLine($"[CountyFusion] Unable to locate instrument link index {i} after recovery. Writing summary info from results list instead of skipping.");
                                    batch.Add(recoveryRowSummary);
                                    pageSucceeded++;
                                    recoveryRowSummary = null;
                                    recoveryAttemptedForIndex = -1;
                                }
                                else
                                {
                                    Console.WriteLine($"[CountyFusion] Unable to locate instrument link index {i} after recovery. Skipping.");
                                    pageFailed++;
                                }

                                await ApifyHelper.SetStatusMessageAsync(
                                    $"Processing record {i + 1} of {totalToProcess}... {pageFailed} failed");
                            }
                        }
                        catch (Exception recoveryEx)
                        {
                            Console.WriteLine($"[CountyFusion] Recovery failed for record {i + 1}: {recoveryEx.Message}. Skipping.");
                            pageFailed++;
                            await ApifyHelper.SetStatusMessageAsync($"Processing record {i + 1} of {totalToProcess}... {pageFailed} failed");
                        }
                    }
                    else
                    {
                        if (recoveryRowSummary != null && recoveryAttemptedForIndex == i)
                        {
                            Console.WriteLine($"[CountyFusion] Record {i + 1} still not accessible after recovery. Writing summary info from results list instead of skipping.");
                            batch.Add(recoveryRowSummary);
                            pageSucceeded++;
                            recoveryRowSummary = null;
                            recoveryAttemptedForIndex = -1;
                        }
                        else
                        {
                            Console.WriteLine($"[CountyFusion] Record {i + 1} still not accessible after recovery. Skipping.");
                            pageFailed++;
                        }

                        await ApifyHelper.SetStatusMessageAsync(
                            $"Processing record {i + 1} of {totalToProcess}... {pageFailed} failed");
                    }
                }
            }
            catch
            {
            }

            if (batch.Count >= 1 || i == totalToProcess - 1)
            {
                await ApifyHelper.PushDataAsync(batch);
                _csvExportHelper?.WriteBatchToCsvAndFlush(batch);
                batch.Clear();
            }
        }

        GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
        GC.Collect(2, GCCollectionMode.Forced);
        GC.WaitForPendingFinalizers();

        if (currentPage > 0 && totalPages > 0 && currentPage < totalPages)
        {
            await FormFiller.ClickBackToResultsAsync(page);
            await FormFiller.ClickNextResultsPageAsync(page);
            var (childTotal, childSucceeded, childFailed) = await ScrapeAllRecordsOnResultsPageAsync(
                page,
                searchDate,
                outputDirectory,
                exportImages,
                fileFormat,
                isInstrumentSearch,
                instrumentNumber);
            return (totalToProcess + childTotal, pageSucceeded + childSucceeded, pageFailed + childFailed);
        }

        return (totalToProcess, pageSucceeded, pageFailed);
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

        await page.GotoAsync(_currentLoginUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });
    }

    /// <summary>Resolves the GovOS login URL for the requested county.</summary>
    static string ResolveLoginUrl(string? countyName)
    {
        if (string.IsNullOrWhiteSpace(countyName) ||
            string.Equals(countyName.Trim(), "Clermont", StringComparison.OrdinalIgnoreCase))
            return "https://countyfusion2.govos.com/countyweb/loginDisplay.action?countyname=ClermontOH";

        if (string.Equals(countyName.Trim(), "Ross", StringComparison.OrdinalIgnoreCase))
            return "https://countyfusion10.govos.com/countyweb/loginDisplay.action?countyname=RossOH";

        if (string.Equals(countyName.Trim(), "Butler", StringComparison.OrdinalIgnoreCase))
            return "https://countyfusion13.govos.com/countyweb/loginDisplay.action?countyname=ButlerOH";

        throw new ArgumentException($"Unknown CountyName '{countyName}'. Supported: Clermont, Ross, Butler.");
    }

    static int GetMaxDisplayForCounty(string? countyName)
    {
        if (string.IsNullOrWhiteSpace(countyName) ||
            string.Equals(countyName.Trim(), "Clermont", StringComparison.OrdinalIgnoreCase))
            return 500;
        if (string.Equals(countyName.Trim(), "Ross", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(countyName.Trim(), "Butler", StringComparison.OrdinalIgnoreCase))
            return 250;
        return 500;
    }

    /// <summary>Close browser, context, CSV stream. Try Log Out if page still open.</summary>
    public async Task StopAsync()
    {
        _csvExportHelper?.CloseCsvStream();

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
            catch { }

            await _context.CloseAsync();
        }

        if (_browser != null) await _browser.CloseAsync();
        _playwright?.Dispose();
        _context = null;
        _browser = null;
        _playwright = null;
    }
}
