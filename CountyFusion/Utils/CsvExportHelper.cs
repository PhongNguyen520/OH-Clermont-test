using System.Collections.Generic;
using System.IO;
using CsvHelper;
using CountyFusion.Models;

namespace CountyFusion.Utils;

/// <summary>CSV export with streamed writes for Clermont records.</summary>
public class CsvExportHelper
{
    StreamWriter? _csvStreamWriter;
    CsvWriter? _csvWriter;

    /// <summary>Open CSV stream once for the run. Call before ScrapeAllRecordsOnResultsPageAsync.</summary>
    public void OpenCsvStreamForRun(DateTime searchDate, string outputDirectory)
    {
        if (_csvWriter != null) return;

        var baseDir = GetBaseDirForStorage(outputDirectory);
        var kvStoreDir = Path.Combine(baseDir, "apify_storage", "key_value_store");
        Directory.CreateDirectory(kvStoreDir);
        var fileName = $"CountyFusion_{searchDate:MM-dd-yyyy}.csv";
        var filePath = Path.Combine(kvStoreDir, fileName);
        var fileExists = File.Exists(filePath);

        var config = ClermontRecord.CreateCsvConfiguration();
        config.HasHeaderRecord = !fileExists;
        _csvStreamWriter = new StreamWriter(filePath, append: fileExists);
        _csvWriter = new CsvWriter(_csvStreamWriter, config);
    }

    /// <summary>Write batch to the already-open CSV and flush (no extra allocations).</summary>
    public void WriteBatchToCsvAndFlush(List<ClermontRecord> batch)
    {
        if (_csvWriter == null || _csvStreamWriter == null || batch.Count == 0) return;
        _csvWriter.WriteRecords(batch);
        _csvStreamWriter.Flush();
    }

    /// <summary>Close CSV stream. Call after scrape or in StopAsync.</summary>
    public void CloseCsvStream()
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

    /// <summary>Get base directory for storage (strip bin/ from path if present).</summary>
    public static string GetBaseDirForStorage(string outputDirectory)
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

    /// <summary>Export records to CSV file (standalone). Prefer OpenCsvStreamForRun for main run.</summary>
    public static Task ExportToCsvAsync(List<ClermontRecord> records, DateTime searchDate, string outputDirectory)
    {
        records ??= new List<ClermontRecord>();
        var baseDir = GetBaseDirForStorage(outputDirectory);
        var kvStoreDir = Path.Combine(baseDir, "apify_storage", "key_value_store");
        Directory.CreateDirectory(kvStoreDir);
        var fileName = $"CountyFusion_{searchDate:MM-dd-yyyy}.csv";
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
}
