using System.Collections.Generic;
using System.IO;
using System.Runtime;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using OH_Clermont;
using OH_Clermont.Models;
using OH_Clermont.Utils;

namespace OH_Clermont.Services;

/// <summary>Playwright-based scraper for Clermont County. Handles login, session errors, and public records search.</summary>
public class ClermontScraperService
{
    const string CountyLoginUrl = "https://countyfusion2.govos.com/countyweb/loginDisplay.action?countyname=ClermontOH";
    const string TimeoutErrorSnippet = "This form has already been processed or the session timed out";
    const string ActiveSessionSnippet = "Login Failed: Active Session.";

    IPlaywright? _playwright;
    IBrowser? _browser;
    IBrowserContext? _context;
    CsvExportHelper? _csvExportHelper;

    /// <summary>Launches browser, opens login page, navigates to Search Criteria and fills from InputConfig.</summary>
    public async Task<IPage> LaunchAsync(InputConfig config)
    {
        config ??= new InputConfig();

        await ApifyHelper.SetStatusMessageAsync("Starting OH-Clermont scraper...");

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

        await page.GotoAsync(CountyLoginUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });

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

        var display = config.Display <= 0 ? 500 : config.Display;
        await FormFiller.SetupSearchPageAsync(page, display);

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
                    await FormFiller.SetupSearchPageAsync(page, display);
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
                    await ApifyHelper.SetStatusMessageAsync($"Fatal Error during search after {searchRetries} attempts: {ex.Message}", isTerminal: true);
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
            var (total, succeeded, failed) = await ScrapeAllRecordsOnResultsPageAsync(page, fromDate, outputDirectory, exportImages);
            await ApifyHelper.SetStatusMessageAsync($"Finished! Total {total} requests: {succeeded} succeeded, {failed} failed.", isTerminal: true);
            Console.WriteLine("[Scrape] Done. Records were pushed to Dataset and appended to CSV.");
        }
        finally
        {
            _csvExportHelper.CloseCsvStream();
        }

        return page;
    }

    /// <summary>Scrape Instrument Info from DocumentInfoView detail page into ClermontRecord.</summary>
    public static async Task<ClermontRecord> ScrapeInstrumentInfoAsync(IPage page)
    {
        var docInfoFrame = page
            .FrameLocator("iframe[name='bodyframe']")
            .FrameLocator("iframe[name='documentFrame']")
            .FrameLocator("iframe[name='docInfoFrame']");

        await Task.Delay(2000);

        var record = new ClermontRecord
        {
            Amount = string.Empty,
            Remarks = string.Empty,
            ParcelNumber = string.Empty,
            PropertyAddress = string.Empty
        };

        record.DocumentNumber = (await DomHelper.GetByLabelAsync(docInfoFrame, "Instrument Number:")).Trim();
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

        var recDateRaw = await DomHelper.GetByLabelAsync(docInfoFrame, "Recorded Date:");
        if (!string.IsNullOrWhiteSpace(recDateRaw))
        {
            var idx = recDateRaw.IndexOf(' ');
            record.RecordingDate = idx > 0 ? recDateRaw[..idx].Trim() : recDateRaw.Trim();
        }

        record.DocumentType = (await DomHelper.GetByLabelAsync(docInfoFrame, "Instrument Type:")).Trim();
        record.InstrumentDate = (await DomHelper.GetByLabelAsync(docInfoFrame, "Document Date:")).Trim();

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
            var tabsFrame = docInfoFrame.FrameLocator("iframe[name='tabs'], iframe#tabs");
            var legalTab = tabsFrame.Locator(".tabs-title:has-text(\"Legal Description\")");

            if (await legalTab.CountAsync() > 0)
            {
                await legalTab.First.ClickAsync();
                await Task.Delay(1500); // wait for Legal tab contents to load into docInfoFrame
            }
        }
        catch { }

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

    /// <summary>Scrape all records on results page: click first, then Next, flush after each record.</summary>
    public async Task<(int total, int succeeded, int failed)> ScrapeAllRecordsOnResultsPageAsync(
        IPage page,
        DateTime searchDate,
        string outputDirectory,
        bool exportImages = true)
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

        for (var i = 0; i < totalToProcess; i++)
        {
            if ((i + 1) % 10 == 0)
            {
                await ApifyHelper.SetStatusMessageAsync($"Processing record {i + 1} of {totalToProcess} on current page...");
            }

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
                        break;
                }

                var record = await ScrapeInstrumentInfoAsync(page);
                batch.Add(record);

                if (exportImages)
                    await ImageProcessor.ProcessImagesForCurrentRecordAsync(page, record, searchDate, i + 1);

                pageSucceeded++;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[OH_Clermont] Error processing record {i + 1}: {ex.Message}");
                pageFailed++;
            }

            GC.Collect();
            GC.WaitForPendingFinalizers();

            if (batch.Count >= 1 || i == totalToProcess - 1)
            {
                await ApifyHelper.PushDataAsync(batch);
                _csvExportHelper?.WriteBatchToCsvAndFlush(batch);
                batch.Clear();
                GCSettings.LargeObjectHeapCompactionMode = GCLargeObjectHeapCompactionMode.CompactOnce;
                GC.Collect(2, GCCollectionMode.Forced);
                GC.WaitForPendingFinalizers();
            }
        }

        if (currentPage > 0 && totalPages > 0 && currentPage < totalPages)
        {
            await FormFiller.ClickBackToResultsAsync(page);
            await FormFiller.ClickNextResultsPageAsync(page);
            var (childTotal, childSucceeded, childFailed) = await ScrapeAllRecordsOnResultsPageAsync(page, searchDate, outputDirectory, exportImages);
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

        await page.GotoAsync(CountyLoginUrl, new PageGotoOptions { WaitUntil = WaitUntilState.NetworkIdle });
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
