using OH_Clermont;
using OH_Clermont.Models;
using OH_Clermont.Services;

class Program
{
    static async Task Main(string[] args)
    {
        var isApify = !string.IsNullOrEmpty(Environment.GetEnvironmentVariable("APIFY_CONTAINER_PORT"));

        Console.WriteLine("Installing Chromium (if needed)...");
        Microsoft.Playwright.Program.Main(["install", "chromium"]);

        // Read input from input.json / Apify (unwrap "input" property if present)
        var config = ApifyHelper.GetInput<InputConfig>();

        var service = new ClermontScraperService();
        try
        {
            Console.WriteLine("Launching browser and running search with input.json...");
            await service.LaunchAsync(config);
            Console.WriteLine("Done.");
            if (!isApify)
            {
                Console.WriteLine("Press Enter to exit...");
                Console.ReadLine();
            }
        }
        finally
        {
            await service.StopAsync();
        }
    }
}
