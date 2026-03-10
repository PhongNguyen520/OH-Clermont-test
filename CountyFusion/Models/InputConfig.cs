namespace CountyFusion.Models;

/// <summary>
/// Input configuration loaded from JSON (local input.json or Apify input).
/// Controls the search form: FromDate, ToDate. Records per page set to county max.</summary>
public class InputConfig
{
    /// <summary>County name used to select the GovOS environment (default: Clermont).</summary>
    public string? CountyName { get; set; }

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

    /// <summary>
    /// Search mode: "Date" (use FromDate/ToDate) or "Instrument" (use InstrumentNumbers list).
    /// </summary>
    public string SearchType { get; set; } = "Date";

    /// <summary>
    /// Comma-separated list of instrument numbers when SearchType == "Instrument".
    /// </summary>
    public string InstrumentNumbers { get; set; } = "";

    /// <summary>
    /// Output file format for images: "tif" (default) or "pdf".
    /// </summary>
    public string FileFormat { get; set; } = "tif";
}
