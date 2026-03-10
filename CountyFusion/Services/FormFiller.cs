using Microsoft.Playwright;

namespace CountyFusion.Services;

/// <summary>Form filling and navigation utilities for Clermont County public records search.</summary>
public static class FormFiller
{
    /// <summary>Click public/guest login button after NetworkIdle (supports Clermont/Ross/Butler layouts).</summary>
    public static async Task ClickLoginAsPublicAsync(IPage page)
    {
        await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
        await Task.Delay(1000);

        var loginAsPublic = page.Locator("input[value='Login as Public']");
        if (await loginAsPublic.CountAsync() > 0)
        {
            await loginAsPublic.First.WaitForAsync(new LocatorWaitForOptions
            {
                State = WaitForSelectorState.Visible,
                Timeout = 30_000
            });
            await loginAsPublic.First.ClickAsync();
        }
        else
        {
            var loginAsGuest = page.Locator("input[value='Login as Guest']");
            if (await loginAsGuest.CountAsync() == 0)
                throw new TimeoutException("Could not find either 'Login as Public' or 'Login as Guest' button on the login page.");

            await loginAsGuest.First.WaitForAsync(new LocatorWaitForOptions
            {
                State = WaitForSelectorState.Visible,
                Timeout = 30_000
            });
            await loginAsGuest.First.ClickAsync();
        }

        await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
    }

    /// <summary>Click Accept on disclaimer page (bodyframe iframe). Skips if no disclaimer (e.g. Butler/Ross).</summary>
    public static async Task ClickDisclaimerAcceptAsync(IPage page)
    {
        await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

        var frame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "bodyframe", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("disclaimer", StringComparison.OrdinalIgnoreCase));
        var root = frame ?? page.MainFrame;

        await root.WaitForLoadStateAsync(LoadState.NetworkIdle);
        await Task.Delay(1000);

        var accept = root.Locator("input#accept[value='Accept']");
        try
        {
            await accept.WaitForAsync(new LocatorWaitForOptions { State = WaitForSelectorState.Visible, Timeout = 8000 });
            await accept.First.ClickAsync();
            await root.WaitForLoadStateAsync(LoadState.NetworkIdle);
        }
        catch (TimeoutException)
        {
            // No disclaimer (Butler/Ross) - continue
        }
    }

    /// <summary>Click "Search Public Records" on welcome page.</summary>
    public static async Task ClickSearchPublicRecordsAsync(IPage page)
    {
        await page.WaitForLoadStateAsync(LoadState.NetworkIdle);

        var frame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "bodyframe", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("welcome", StringComparison.OrdinalIgnoreCase));
        var root = frame ?? page.MainFrame;

        await root.WaitForLoadStateAsync(LoadState.NetworkIdle);
        await Task.Delay(1000);

        var menuItem = root.Locator(".datagrid-cell-c1-text:has-text('Search Public Records')");
        if (await menuItem.CountAsync() > 0)
        {
            await menuItem.First.ClickAsync();
            await root.WaitForLoadStateAsync(LoadState.NetworkIdle);
            return;
        }

        var anySearchLink = root.GetByText("Search Public Records", new() { Exact = false });
        if (await anySearchLink.CountAsync() > 0)
        {
            await anySearchLink.First.ClickAsync();
            await root.WaitForLoadStateAsync(LoadState.NetworkIdle);
        }
    }

    /// <summary>Configure Search Criteria: All Names, Display, Document Types, One Row per Document.</summary>
    public static async Task SetupSearchPageAsync(IPage page, int display)
    {
        var dynSearchFrame = page
            .FrameLocator("iframe[name='bodyframe']")
            .FrameLocator("iframe[name='dynSearchFrame']");

        var criteriaFrame = dynSearchFrame
            .FrameLocator("iframe[name='criteriaframe']");

        await Task.Delay(2000);

        var allNames = dynSearchFrame.Locator(".SEARCHTYPE_datagrid-cell-c2-text span.base:has-text(\"All Names\")");
        if (await allNames.CountAsync() > 0)
        {
            await allNames.First.ClickAsync();
        }

        var recsPerPage = dynSearchFrame.Locator("#RECSPERPAGE");
        if (await recsPerPage.CountAsync() > 0)
        {
            await recsPerPage.First.EvaluateAsync(
                @"(node, d) => {
                    try {
                        var w = window;
                        var jq = (w.$ || w.jQuery);
                        if (jq && jq(node).combobox) {
                            jq(node).combobox('setValue', d.toString());
                        } else {
                            node.value = d.toString();
                            var span = node.nextElementSibling;
                            if (span) {
                                var visible = span.querySelector('input.textbox-text');
                                if (visible) visible.value = d.toString();
                                var hidden = span.querySelector(""input.textbox-value[name='RECSPERPAGE']"");
                                if (hidden) hidden.value = d.toString();
                            }
                        }

                        try {
                            var submitFrame = (w.getSubmitFrame ? w.getSubmitFrame() : null);
                            if (submitFrame && submitFrame.document) {
                                var form = submitFrame.document.getElementById('searchForm');
                                if (form && form.RECSPERPAGE) {
                                    form.RECSPERPAGE.value = d.toString();
                                }
                            }
                        } catch (e2) {}
                    } catch (e) {}
                }",
                display);
        }

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

        await Task.Delay(1500);
    }

    /// <summary>
    /// Configure Instrument Number search: select Instrument search type, set Records per page, and
    /// put the same instrumentNumber into both From and To fields.
    /// </summary>
    public static async Task SetupInstrumentSearchAsync(IPage page, string instrumentNumber, int display)
    {
        var dynSearchFrame = page
            .FrameLocator("iframe[name='bodyframe']")
            .FrameLocator("iframe[name='dynSearchFrame']");

        var criteriaFrame = dynSearchFrame
            .FrameLocator("iframe[name='criteriaframe']");

        await Task.Delay(2000);

        // 1) Select "Instrument Number" search type in the left menu
        var instrumentRow = dynSearchFrame.Locator(".SEARCHTYPE_datagrid-cell-c2-text span.base:has-text(\"Instrument Number\")");
        if (await instrumentRow.CountAsync() > 0)
        {
            // Ensure the search types grid is ready
            await instrumentRow.First.WaitForAsync(new LocatorWaitForOptions
            {
                State = WaitForSelectorState.Visible,
                Timeout = 15_000
            });
            await instrumentRow.First.ClickAsync();
        }

        // Also tell EasyUI datalist to select the Instrument Number row explicitly
        await dynSearchFrame.Locator("body").EvaluateAsync(@"
            () => {
                var jq = window.$ || window.jQuery;
                if (!jq) return;
                try {
                    if (jq('#SEARCHTYPE').length && jq('#SEARCHTYPE').datalist) {
                        // 0: All Names, 1: Instrument Number, 2: Book / Page
                        jq('#SEARCHTYPE').datalist('selectRow', 1);
                    }
                } catch (e) { console.error('Error selecting SEARCHTYPE Instrument Number:', e); }
            }");

        // 2) Set records per page (reuse the same logic as SetupSearchPageAsync)
        var recsPerPage = dynSearchFrame.Locator("#RECSPERPAGE");
        if (await recsPerPage.CountAsync() > 0)
        {
            await recsPerPage.First.EvaluateAsync(
                @"(node, d) => {
                    try {
                        var w = window;
                        var jq = (w.$ || w.jQuery);
                        if (jq && jq(node).combobox) {
                            jq(node).combobox('setValue', d.toString());
                        } else {
                            node.value = d.toString();
                            var span = node.nextElementSibling;
                            if (span) {
                                var visible = span.querySelector('input.textbox-text');
                                if (visible) visible.value = d.toString();
                                var hidden = span.querySelector(""input.textbox-value[name='RECSPERPAGE']"");
                                if (hidden) hidden.value = d.toString();
                            }
                        }

                        try {
                            var submitFrame = (w.getSubmitFrame ? w.getSubmitFrame() : null);
                            if (submitFrame && submitFrame.document) {
                                var form = submitFrame.document.getElementById('searchForm');
                                if (form && form.RECSPERPAGE) {
                                    form.RECSPERPAGE.value = d.toString();
                                }
                            }
                        } catch (e2) {}
                    } catch (e) {}
                }",
                display);
        }

        // 3) Wait for network to settle so inner frames can load
        await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
        await Task.Delay(2000);

        // Execute a recursive script from the top-level page to find and fill the EasyUI textboxes
        await page.EvaluateAsync(@"
            (instrumentValue) => {
                function setValInWindow(win) {
                    try {
                        var jq = (win.$ || win.jQuery);
                        if (jq) {
                            var fromEl = jq('#INSTNUM');
                            var toEl = jq('#INSTNUMEND');
                            
                            var success = false;
                            
                            if (fromEl.length > 0 && typeof fromEl.textbox === 'function') {
                                fromEl.textbox('setValue', instrumentValue);
                                success = true;
                            }
                            if (toEl.length > 0 && typeof toEl.textbox === 'function') {
                                toEl.textbox('setValue', instrumentValue);
                                success = true;
                            }
                            
                            if (success) return true;
                        }
                        
                        // Fallback: check native DOM just in case EasyUI isn't used
                        var natFrom = win.document.getElementById('INSTNUM');
                        var natTo = win.document.getElementById('INSTNUMEND');
                        if (natFrom) natFrom.value = instrumentValue;
                        if (natTo) natTo.value = instrumentValue;
                        if (natFrom || natTo) return true;

                    } catch (e) {
                        // ignore cross-origin or other errors
                    }

                    // Recursively check inner frames
                    for (var i = 0; i < win.frames.length; i++) {
                        if (setValInWindow(win.frames[i])) return true;
                    }
                    return false;
                }

                setValInWindow(window);
            }
        ", instrumentNumber);

        await Task.Delay(1000);
    }

    /// <summary>Set date range in criteriaframe via JavaScript on hidden inputs.</summary>
    public static async Task SetDateRangeForDayAsync(IPage page, DateTime date)
    {
        var dateStr = date.ToString("MM/dd/yyyy");

        var frame = page.FrameLocator("iframe[name='bodyframe']")
                        .FrameLocator("iframe[name='dynSearchFrame']")
                        .FrameLocator("iframe[name='criteriaframe']");

        var fromLocator = frame.Locator("#FROMDATE");
        var toLocator = frame.Locator("#TODATE");
        var rangeLocator = frame.Locator("#daterange_TODATE");

        await fromLocator.WaitForAsync(new LocatorWaitForOptions { State = WaitForSelectorState.Attached, Timeout = 15_000 });

        await fromLocator.EvaluateAsync(@"(node, d) => {
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

        if (await rangeLocator.CountAsync() > 0)
        {
            await rangeLocator.EvaluateAsync(@"(node) => {
                if (typeof window.$ !== 'undefined' && window.$(node).combobox) {
                    window.$(node).combobox('setValue', 'User Defined');
                }
            }");
        }

        await Task.Delay(1500);
    }

    /// <summary>Click Search button in dynSearchFrame header.</summary>
    public static async Task ClickSearchAsync(IPage page)
    {
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
            await PrintNoDocumentsMessageIfAnyAsync(page);
        }
    }

    /// <summary>If "No documents found" message appears, print and exit.</summary>
    public static async Task PrintNoDocumentsMessageIfAnyAsync(IPage page)
    {
        var frame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "dynSearchFrame", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("searchCriteria.do", StringComparison.OrdinalIgnoreCase));
        if (frame == null) return;

        var msgSpan = frame.Locator("#msgDiv span#displayMsg");
        if (await msgSpan.CountAsync() == 0) return;

        var span = msgSpan.First;

        try
        {
            await span.WaitForAsync(new LocatorWaitForOptions
            {
                State = WaitForSelectorState.Visible,
                Timeout = 10_000
            });
        }
        catch { return; }

        var text = (await span.InnerTextAsync()).Trim();
        if (!string.IsNullOrEmpty(text))
        {
            Console.WriteLine(text);
            Environment.Exit(0);
        }
    }

    /// <summary>Click Next in DocumentInfoView to move to next record.</summary>
    public static async Task<bool> ClickNextDocumentAsync(IPage page)
    {
        var docFrame = page.Frames.FirstOrDefault(f =>
            string.Equals(f.Name, "documentFrame", StringComparison.OrdinalIgnoreCase) ||
            f.Url.Contains("DocumentInfoView", StringComparison.OrdinalIgnoreCase));

        if (docFrame == null)
            return false;

        var nextLink = docFrame.Locator("a[onclick*=\"navToDocument('next')\"]");

        if (await nextLink.CountAsync() == 0)
        {
            var parentOfImg = docFrame.Locator("img#nextimg").Locator("xpath=..");
            if (await parentOfImg.CountAsync() == 0)
                return false;

            nextLink = parentOfImg;
        }

        await nextLink.First.ClickAsync();
        await Task.Delay(2000);
        return true;
    }

    /// <summary>Click the instrument link at the given zero-based index in the result list.</summary>
    public static async Task<bool> ClickInstrumentAtIndexAsync(IPage page, int index)
    {
        if (index < 0)
            return false;

        var bodyFrame = page.FrameLocator("iframe[name='bodyframe']");
        var resultFrame = bodyFrame.FrameLocator("iframe[name='resultFrame']");
        var resultListFrame = resultFrame.FrameLocator("iframe[name='resultListFrame']");

        const string instrumentSelector =
            "#instList .datagrid-view2 .datagrid-body .datagrid-btable tr.datagrid-row td[field='2'] a";

        var links = resultListFrame.Locator(instrumentSelector);
        var count = await links.CountAsync();
        if (index >= count)
            return false;

        var link = links.Nth(index);
        await link.ScrollIntoViewIfNeededAsync();
        await link.ClickAsync();
        await page.WaitForLoadStateAsync(LoadState.NetworkIdle);
        await Task.Delay(1000);
        return true;
    }

    /// <summary>Click "Go to next result page" in subnav iframe.</summary>
    public static async Task ClickNextResultsPageAsync(IPage page)
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
        try
        {
            await page.WaitForLoadStateAsync(
                LoadState.NetworkIdle,
                new PageWaitForLoadStateOptions { Timeout = 15_000 });
        }
        catch
        {
        }
        await Task.Delay(1000);
        Console.WriteLine("[ResultsNav] Clicked Go to next result page.");
    }

    /// <summary>Click "Back to Results" in resnavframe to return to result list.</summary>
    public static async Task ClickBackToResultsAsync(IPage page)
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
        await Task.Delay(2000);
        Console.WriteLine("[ResultsNav] Clicked Back to Results.");
    }
}
