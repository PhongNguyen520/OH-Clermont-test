using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;

namespace OH_Clermont.Utils;

/// <summary>DOM extraction and parsing utilities for Clermont County scraper.</summary>
public static class DomHelper
{
    /// <summary>Parse date string (MM/dd/yyyy). Throws ArgumentException if invalid.</summary>
    public static DateTime ParseDate(string value, string fieldName)
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

    /// <summary>Parse "Displaying 1-100 of 112 Items" and return the displayed count (100).</summary>
    public static int GetPageCountFromNavDisplay(string? text)
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

    /// <summary>Check if page contains error snippet in tr.error td cells.</summary>
    public static async Task<bool> HasErrorSnippetAsync(IPage page, string snippet)
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

    /// <summary>Get value from sibling cell of td containing label in docInfoFrame.</summary>
    public static async Task<string> GetByLabelAsync(IFrameLocator docInfoFrame, string label)
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
}
