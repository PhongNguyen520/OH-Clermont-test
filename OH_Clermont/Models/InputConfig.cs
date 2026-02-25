namespace OH_Clermont.Models;

/// <summary>
/// Input configuration loaded from JSON (local input.json or Apify input).
/// Controls the search form: Display, Name, FromDate, ToDate.
/// </summary>
public class InputConfig
{
    /// <summary>
    /// Number of records per page. The crawler expects 500, but the field is kept for input mapping.
    /// </summary>
    public int Display { get; set; } = 500;

    /// <summary>
    /// Optional name to search. <c>null</c> or empty string means the Name field is left blank.
    /// </summary>
    public string? Name { get; set; }

    /// <summary>
    /// Start date, required. Supports "yyyy-MM-dd" or "MM/dd/yyyy" formats.
    /// </summary>
    public string FromDate { get; set; } = "";

    /// <summary>
    /// End date, required. Supports "yyyy-MM-dd" or "MM/dd/yyyy" formats.
    /// </summary>
    public string ToDate { get; set; } = "";

    /// <summary>
    /// When false, skip downloading/saving images to avoid OOM on memory-limited Apify runs.
    /// Set to false if you only need CSV/Dataset data.
    /// </summary>
    public bool ExportImages { get; set; } = true;
}
