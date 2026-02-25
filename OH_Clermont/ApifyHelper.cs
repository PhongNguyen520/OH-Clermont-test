using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;

namespace OH_Clermont;

/// <summary>
/// Helper for Apify Actor lifecycle. Reads input from apify_storage/input.json and writes dataset.
/// On Apify platform: pushes via REST API. Locally: writes to apify_storage/dataset/default.ndjson.
/// </summary>
public static class ApifyHelper
{
    const string DefaultInputPath = "apify_storage/input.json";
    const string DefaultDatasetPath = "apify_storage/dataset/default.ndjson";
    static readonly HttpClient HttpClient = new();

    static readonly JsonSerializerOptions InputJsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase
    };

    /// <summary>Get Actor input. Reads from apify_storage/input.json, storage/.../INPUT.json, input.json, or APIFY_INPUT_VALUE env.</summary>
    public static T GetInput<T>() where T : new()
    {
        var json = GetInputJson();
        if (string.IsNullOrWhiteSpace(json))
        {
            Console.WriteLine("[ApifyHelper] No input JSON found; using defaults.");
            return new T();
        }
        var preview = json.Length > 500 ? json[..500] + "..." : json;
        Console.WriteLine($"[ApifyHelper] Input JSON (first 500 chars): {preview}");
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            JsonElement inputProp;
            if (root.ValueKind == JsonValueKind.Object &&
                (root.TryGetProperty("input", out inputProp) || root.TryGetProperty("Input", out inputProp)) &&
                inputProp.ValueKind == JsonValueKind.Object)
            {
                var unwrapped = inputProp.GetRawText();
                return JsonSerializer.Deserialize<T>(unwrapped, InputJsonOptions) ?? new T();
            }
            return JsonSerializer.Deserialize<T>(json, InputJsonOptions) ?? new T();
        }
        catch (JsonException ex)
        {
            Console.WriteLine($"[ApifyHelper] JSON parse error: {ex.Message}");
            return new T();
        }
    }

    static string? GetInputJson()
    {
        var envValue = Environment.GetEnvironmentVariable("APIFY_INPUT_VALUE");
        if (!string.IsNullOrWhiteSpace(envValue))
            return envValue.Trim();

        var cwd = Directory.GetCurrentDirectory();
        var baseDir = AppContext.BaseDirectory;
        var paths = new[]
        {
            Path.Combine(cwd, DefaultInputPath),
            Path.Combine(baseDir, DefaultInputPath),
            Path.Combine(cwd, "storage", "key_value_stores", "default", "INPUT.json"),
            Path.Combine(cwd, "input.json"),
            Path.Combine(baseDir, "input.json"),
            Path.Combine(cwd, "..", "input.json"),
            Path.Combine(cwd, "..", "..", "input.json")
        };
        foreach (var p in paths)
        {
            if (File.Exists(p))
                return File.ReadAllText(p);
        }

        return FetchInputFromApifyApi();
    }

    static string? FetchInputFromApifyApi()
    {
        var storeId = Environment.GetEnvironmentVariable("ACTOR_DEFAULT_KEY_VALUE_STORE_ID")
            ?? Environment.GetEnvironmentVariable("APIFY_DEFAULT_KEY_VALUE_STORE_ID");
        var token = Environment.GetEnvironmentVariable("APIFY_TOKEN");
        var inputKey = Environment.GetEnvironmentVariable("ACTOR_INPUT_KEY")
            ?? Environment.GetEnvironmentVariable("APIFY_INPUT_KEY")?.Trim()
            ?? "INPUT";

        if (string.IsNullOrEmpty(storeId) || string.IsNullOrEmpty(token))
        {
            Console.WriteLine($"[ApifyHelper] Skipping KV fetch: storeId={(string.IsNullOrEmpty(storeId) ? "(not set)" : "[set]")}, token={(string.IsNullOrEmpty(token) ? "(not set)" : "[set]")}");
            return null;
        }

        Console.WriteLine($"[ApifyHelper] Fetching input from KV store (key={inputKey})...");
        try
        {
            var apiBase = Environment.GetEnvironmentVariable("APIFY_API_PUBLIC_BASE_URL") ?? "https://api.apify.com";
            var url = $"{apiBase.TrimEnd('/')}/v2/key-value-stores/{storeId}/records/{Uri.EscapeDataString(inputKey)}";
            using var request = new HttpRequestMessage(HttpMethod.Get, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

            using var response = HttpClient.SendAsync(request).GetAwaiter().GetResult();
            if (!response.IsSuccessStatusCode)
            {
                Console.WriteLine($"[ApifyHelper] KV Store fetch failed: {(int)response.StatusCode} {response.StatusCode}");
                return null;
            }
            var json = response.Content.ReadAsStringAsync().GetAwaiter().GetResult();
            if (!string.IsNullOrWhiteSpace(json))
                Console.WriteLine("[ApifyHelper] Fetched input from Apify Key-Value Store API.");
            return json;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ApifyHelper] KV Store fetch error: {ex.Message}");
            return null;
        }
    }

    /// <summary>Push a single item to the default dataset.</summary>
    public static async Task PushSingleDataAsync<T>(T item, CancellationToken ct = default)
    {
        await PushDataAsync(new[] { item }, ct);
    }

    /// <summary>Push multiple items to the default dataset. Uses Apify API when running on platform.</summary>
    public static async Task PushDataAsync<T>(IEnumerable<T> items, CancellationToken ct = default)
    {
        var list = items.ToList();
        if (list.Count == 0) return;

        var datasetId = Environment.GetEnvironmentVariable("APIFY_DEFAULT_DATASET_ID");
        var token = Environment.GetEnvironmentVariable("APIFY_TOKEN");

        if (!string.IsNullOrEmpty(datasetId) && !string.IsNullOrEmpty(token))
        {
            var apiBase = Environment.GetEnvironmentVariable("APIFY_API_PUBLIC_BASE_URL") ?? "https://api.apify.com";
            var url = $"{apiBase.TrimEnd('/')}/v2/datasets/{datasetId}/items";
            using var request = new HttpRequestMessage(HttpMethod.Post, url);
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
            request.Content = new StringContent(
                JsonSerializer.Serialize(list),
                Encoding.UTF8,
                "application/json");

            using var response = await HttpClient.SendAsync(request, ct);
            if (!response.IsSuccessStatusCode)
            {
                var body = await response.Content.ReadAsStringAsync(ct);
                Console.WriteLine($"[ApifyHelper] Dataset push failed. Status: {(int)response.StatusCode} {response.StatusCode}");
                response.EnsureSuccessStatusCode();
            }
            return;
        }

        var dir = Path.GetDirectoryName(DefaultDatasetPath);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(Path.Combine(Directory.GetCurrentDirectory(), dir));
        var path = Path.Combine(Directory.GetCurrentDirectory(), DefaultDatasetPath);
        var lines = list.Select(item => JsonSerializer.Serialize(item) + "\n");
        await File.AppendAllLinesAsync(path, lines, ct);
    }

    /// <summary>Upload a key-value record (e.g. CSV file) to Apify Key-Value Store via API. When not on Apify, writes to local apify_storage/key_value_store.</summary>
    public static async Task SaveKeyValueRecordAsync(string key, byte[] data, string contentType = "application/octet-stream", CancellationToken ct = default)
    {
        var storeId = Environment.GetEnvironmentVariable("APIFY_DEFAULT_KEY_VALUE_STORE_ID")
            ?? Environment.GetEnvironmentVariable("ACTOR_DEFAULT_KEY_VALUE_STORE_ID");
        var token = Environment.GetEnvironmentVariable("APIFY_TOKEN");

        if (!string.IsNullOrEmpty(storeId) && !string.IsNullOrEmpty(token))
        {
            var sanitizedKey = SanitizeKeyForApify(key);
            var apiBase = Environment.GetEnvironmentVariable("APIFY_API_PUBLIC_BASE_URL") ?? "https://api.apify.com";
            var url = $"{apiBase.TrimEnd('/')}/v2/key-value-stores/{storeId}/records/{Uri.EscapeDataString(sanitizedKey)}";
            try
            {
                // Use only the main MIME type (e.g. "text/csv"); Apify API can reject "text/csv; charset=utf-8"
                var mediaType = contentType.Split(';')[0].Trim();
                Console.WriteLine($"[ApifyHelper] Uploading to Key-Value Store: key={sanitizedKey}, size={data.Length} bytes");
                using var request = new HttpRequestMessage(HttpMethod.Put, url);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                request.Content = new ByteArrayContent(data);
                request.Content.Headers.ContentType = new MediaTypeHeaderValue(mediaType);
                using var response = await HttpClient.SendAsync(request, ct);
                if (response.IsSuccessStatusCode)
                    Console.WriteLine($"[ApifyHelper] Key-Value Store upload OK: {sanitizedKey}");
                else
                {
                    var body = await response.Content.ReadAsStringAsync(ct);
                    Console.WriteLine($"[ApifyHelper] Key-Value Store upload failed. Status: {(int)response.StatusCode} {response.StatusCode}");
                    Console.WriteLine($"[ApifyHelper] Response body: {body}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ApifyHelper] Key-Value Store upload error: {ex.Message}");
            }
            return;
        }

        var baseDir = Directory.GetCurrentDirectory();
        var sep = Path.DirectorySeparatorChar;
        if (baseDir.Contains(sep + "bin" + sep))
        {
            var parts = baseDir.Split(sep);
            var binIdx = Array.FindLastIndex(parts, p => string.Equals(p, "bin", StringComparison.OrdinalIgnoreCase));
            if (binIdx > 0) baseDir = string.Join(sep, parts.Take(binIdx));
        }
        var kvDir = Path.Combine(baseDir, "apify_storage", "key_value_store");
        var fullPath = Path.Combine(kvDir, key.Replace('/', Path.DirectorySeparatorChar));
        var dir = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        await File.WriteAllBytesAsync(fullPath, data, ct);
    }

    /// <summary>Save image bytes to storage. On Apify: key-value store via API. Locally: apify_storage/key_value_store/...</summary>
    public static async Task SaveImageAsync(string key, byte[] imageBytes, CancellationToken ct = default)
    {
        var storeId = Environment.GetEnvironmentVariable("APIFY_DEFAULT_KEY_VALUE_STORE_ID")
            ?? Environment.GetEnvironmentVariable("ACTOR_DEFAULT_KEY_VALUE_STORE_ID");
        var token = Environment.GetEnvironmentVariable("APIFY_TOKEN");

        if (!string.IsNullOrEmpty(storeId) && !string.IsNullOrEmpty(token))
        {
            var sanitizedKey = SanitizeKeyForApify(key);
            var contentType = GetContentTypeFromKey(sanitizedKey);
            var apiBase = Environment.GetEnvironmentVariable("APIFY_API_PUBLIC_BASE_URL") ?? "https://api.apify.com";
            var url = $"{apiBase.TrimEnd('/')}/v2/key-value-stores/{storeId}/records/{Uri.EscapeDataString(sanitizedKey)}";
            try
            {
                using var request = new HttpRequestMessage(HttpMethod.Put, url);
                request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);
                request.Content = new ByteArrayContent(imageBytes);
                request.Content.Headers.ContentType = new MediaTypeHeaderValue(contentType);
                using var response = await HttpClient.SendAsync(request, ct);
                if (!response.IsSuccessStatusCode)
                {
                    var body = await response.Content.ReadAsStringAsync(ct);
                    Console.WriteLine($"[ApifyHelper] Key-Value Store upload failed. Status: {(int)response.StatusCode}");
                }
            }
            catch (Exception ex) { Console.WriteLine($"[ApifyHelper] Upload error: {ex.Message}"); }
            return;
        }

        var baseDir = Directory.GetCurrentDirectory();
        var sep = Path.DirectorySeparatorChar;
        if (baseDir.Contains(sep + "bin" + sep))
        {
            var parts = baseDir.Split(sep);
            var binIdx = Array.FindLastIndex(parts, p => string.Equals(p, "bin", StringComparison.OrdinalIgnoreCase));
            if (binIdx > 0) baseDir = string.Join(sep, parts.Take(binIdx));
        }
        var kvDir = Path.Combine(baseDir, "apify_storage", "key_value_store");
        var fullPath = Path.Combine(kvDir, key.Replace('/', Path.DirectorySeparatorChar));
        var dir = Path.GetDirectoryName(fullPath);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        await File.WriteAllBytesAsync(fullPath, imageBytes, ct);
    }

    /// <summary>Get the URL or path for a key-value store record.</summary>
    public static string GetRecordUrl(string key)
    {
        var sanitized = SanitizeKeyForApify(key);
        var storeId = Environment.GetEnvironmentVariable("APIFY_DEFAULT_KEY_VALUE_STORE_ID")
            ?? Environment.GetEnvironmentVariable("ACTOR_DEFAULT_KEY_VALUE_STORE_ID");
        if (!string.IsNullOrEmpty(storeId))
        {
            var apiBase = Environment.GetEnvironmentVariable("APIFY_API_PUBLIC_BASE_URL") ?? "https://api.apify.com";
            return $"{apiBase.TrimEnd('/')}/v2/key-value-stores/{storeId}/records/{Uri.EscapeDataString(sanitized)}?disableRedirect=true";
        }
        var localKey = key.Replace('/', Path.DirectorySeparatorChar).Replace('\\', Path.DirectorySeparatorChar);
        return Path.Combine("apify_storage", "key_value_store", localKey);
    }

    internal static string SanitizeKeyForApify(string key)
    {
        if (string.IsNullOrEmpty(key)) return "unnamed";
        var s = key.Replace("/", "__").Replace("\\", "__");
        s = Regex.Replace(s, @"[^a-zA-Z0-9_.\-]", "_");
        s = s.Trim('.', '-', '_');
        return string.IsNullOrEmpty(s) ? "unnamed" : s;
    }

    static string GetContentTypeFromKey(string key)
    {
        var ext = Path.GetExtension(key).ToLowerInvariant();
        return ext switch
        {
            ".png" => "image/png",
            ".tif" or ".tiff" => "image/tiff",
            ".jpg" or ".jpeg" => "image/jpeg",
            ".gif" => "image/gif",
            ".webp" => "image/webp",
            ".bmp" => "image/bmp",
            ".pdf" => "application/pdf",
            _ => "application/octet-stream"
        };
    }
}
