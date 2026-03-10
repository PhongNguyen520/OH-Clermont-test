using System.Globalization;
using CsvHelper.Configuration;
using CsvHelper.Configuration.Attributes;

namespace CountyFusion.Models;

/// <summary>
/// Data model for one CSV row (16 columns exactly as required).
/// </summary>
public class ClermontRecord
{
    [Name("Document Number")]
    public string DocumentNumber { get; set; } = string.Empty;

    [Name("Book")]
    public string Book { get; set; } = string.Empty;

    [Name("Page")]
    public string Page { get; set; } = string.Empty;

    [Name("Recording Date")]
    public string RecordingDate { get; set; } = string.Empty;

    [Name("Book Type")]
    public string BookType { get; set; } = string.Empty;

    [Name("Document Type")]
    public string DocumentType { get; set; } = string.Empty;

    [Name("Amount")]
    public string Amount { get; set; } = string.Empty;

    [Name("Grantor")]
    public string Grantor { get; set; } = string.Empty;

    [Name("Grantee")]
    public string Grantee { get; set; } = string.Empty;

    [Name("Reference")]
    public string Reference { get; set; } = string.Empty;

    [Name("Remarks")]
    public string Remarks { get; set; } = string.Empty;

    [Name("Parcel Number")]
    public string ParcelNumber { get; set; } = string.Empty;

    [Name("Legal")]
    public string Legal { get; set; } = string.Empty;

    [Name("Property Address")]
    public string PropertyAddress { get; set; } = string.Empty;

    [Name("Instrument Date")]
    public string InstrumentDate { get; set; } = string.Empty;

    [Name("Long Description")]
    public string LongDescription { get; set; } = string.Empty;

    [Name("Images")]
    public string Images { get; set; } = string.Empty;

    /// <summary>
    /// CsvHelper configuration: '|' delimiter and always quote values.
    /// </summary>
    public static CsvConfiguration CreateCsvConfiguration()
        => new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            Delimiter = "|",
            HasHeaderRecord = true,
            ShouldQuote = _ => true
        };
}
