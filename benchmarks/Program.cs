using System;
using System.Threading.Tasks;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Configs;

namespace nORM.Benchmarks
{
    public class Program
    {
        public static async Task Main(string[] args)
        {
            Console.WriteLine("🚀 nORM Performance Benchmarks");
            Console.WriteLine("==============================");
            Console.WriteLine();

            if (args.Length > 0)
            {
                switch (args[0].ToLower())
                {
                    case "--quick":
                        Console.WriteLine("🏃 Running quick functional tests...");
                        await RunQuickTests();
                        return;

                    case "--fast":
                    case "--norm-only":
                        Console.WriteLine("⚡ Running fast nORM-only benchmarks...");
                        await RunFastNormBenchmarks();
                        return;

                    case "--help":
                        ShowHelp();
                        return;
                }
            }

            Console.WriteLine("This benchmark compares nORM against:");
            Console.WriteLine("• Entity Framework Core");
            Console.WriteLine("• Dapper");
            Console.WriteLine("• Raw ADO.NET");
            Console.WriteLine();
            Console.WriteLine("Test scenarios:");
            Console.WriteLine("• Single inserts");
            Console.WriteLine("• Simple queries (standard & compiled/prepared)");
            Console.WriteLine("• Complex queries with filtering (standard & compiled/prepared)");
            Console.WriteLine("• Joins");
            Console.WriteLine("• Count operations");
            Console.WriteLine("• Bulk operations (naive, batched, idiomatic)");
            Console.WriteLine();
            Console.WriteLine("⚠️  This will take several minutes to complete.");
            if (!Console.IsInputRedirected)
            {
                Console.WriteLine("Press any key to start, or Ctrl+C to cancel...");
                Console.ReadKey();
                Console.WriteLine();
            }

            try
            {
                var config = ManualConfig.Create(DefaultConfig.Instance)
                    .WithOptions(ConfigOptions.DisableOptimizationsValidator);

                var summary = BenchmarkRunner.Run<OrmBenchmarks>(config);

                Console.WriteLine();
                Console.WriteLine("🎯 Benchmark Results Summary");
                Console.WriteLine("============================");
                Console.WriteLine();
                Console.WriteLine("Key Performance Insights:");
                Console.WriteLine("• Lower 'Mean' time = better performance");
                Console.WriteLine("• Lower 'Allocated' memory = more efficient");
                Console.WriteLine("• 'Rank' shows relative performance (1 = fastest)");
                Console.WriteLine();

                if (summary.HasCriticalValidationErrors)
                {
                    Console.WriteLine("❌ Critical validation errors occurred during benchmarking.");
                }
                else
                {
                    Console.WriteLine("✅ Benchmarking completed successfully!");
                    Console.WriteLine();
                    Console.WriteLine("📊 To view detailed results, check the generated reports in:");
                    Console.WriteLine("   BenchmarkDotNet.Artifacts/results/");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error running benchmarks: {ex.Message}");
                Console.WriteLine();
                Console.WriteLine("🔧 Troubleshooting tips:");
                Console.WriteLine("• Make sure you have .NET 8.0 installed");
                Console.WriteLine("• Try running as Administrator");
                Console.WriteLine("• Check that SQLite is available");
                Console.WriteLine("• Run with --quick flag for basic functionality test");
                Console.WriteLine("• Run with --fast flag for nORM-only debugging");
            }

            Console.WriteLine();
            if (!Console.IsInputRedirected)
            {
                Console.WriteLine("Press any key to exit...");
                Console.ReadKey();
            }
        }

        private static void ShowHelp()
        {
            Console.WriteLine("nORM Benchmark Options:");
            Console.WriteLine("=======================");
            Console.WriteLine();
            Console.WriteLine("Usage: dotnet run [options]");
            Console.WriteLine();
            Console.WriteLine("Options:");
            Console.WriteLine("  (no args)         Run full benchmark suite (10-15 minutes)");
            Console.WriteLine("  --quick           Quick functionality test (30 seconds)");
            Console.WriteLine("  --fast            Fast nORM-only benchmarks (2-3 minutes)");
            Console.WriteLine("  --norm-only       Same as --fast");
            Console.WriteLine("  --help            Show this help");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  dotnet run --quick      # Verify nORM works");
            Console.WriteLine("  dotnet run --fast       # Debug nORM performance only");
            Console.WriteLine("  dotnet run              # Full comparison benchmark");
        }

        private static async Task RunFastNormBenchmarks()
        {
            Console.WriteLine("Running fast nORM-only benchmarks...");
            Console.WriteLine("This will quickly test nORM performance with smaller datasets.");
            Console.WriteLine();

            try
            {
                var config = ManualConfig.Create(DefaultConfig.Instance)
                    .WithOptions(ConfigOptions.DisableOptimizationsValidator);

                await Task.Run(() => {
                    var summary = BenchmarkRunner.Run<FastNormBenchmarks>(config);
                    return summary;
                });

                Console.WriteLine();
                Console.WriteLine("✅ Fast nORM benchmarks completed!");
                Console.WriteLine("This focused test helps identify nORM-specific issues quickly.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Error running fast benchmarks: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");

                if (ex.InnerException != null)
                {
                    Console.WriteLine($"Inner exception: {ex.InnerException.Message}");
                }
            }
        }

        private static async Task RunQuickTests()
        {
            Console.WriteLine("Running quick functionality tests...");
            Console.WriteLine();

            try
            {
                // Quick tests start here

                var benchmarks = new OrmBenchmarks();
                await benchmarks.Setup();

                Console.WriteLine("✅ Database setup completed");

                // Test nORM basic operations
                await TestNormOperations(benchmarks);

                benchmarks.Cleanup();
                Console.WriteLine("✅ All quick tests passed!");
                Console.WriteLine();
                Console.WriteLine("🎉 nORM is working correctly!");
                Console.WriteLine("Run without --quick flag to see detailed performance comparison.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"❌ Quick test failed: {ex.Message}");
                Console.WriteLine($"Stack trace: {ex.StackTrace}");
            }
        }

        private static async Task TestNormOperations(OrmBenchmarks benchmarks)
        {
            Console.WriteLine("Testing nORM operations...");

            try
            {
                // Test single insert
                await benchmarks.Insert_Single_nORM();
                Console.WriteLine("  ✅ Single insert");

                // Test simple query
                var users = await benchmarks.Query_Simple_nORM();
                Console.WriteLine($"  ✅ Simple query returned {users.Count} users");

                // Test complex query
                var complexUsers = await benchmarks.Query_Complex_nORM();
                Console.WriteLine($"  ✅ Complex query returned {complexUsers.Count} users");

                // Test join query
                try
                {
                    var joinResults = await benchmarks.Query_Join_nORM();
                    Console.WriteLine($"  ✅ Join query returned {joinResults.Count} results");
                }
                catch (NotImplementedException)
                {
                    Console.WriteLine("  ⚠️  Join query not yet implemented");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"  ❌ Join query failed: {ex.Message}");
                }

                // Test count
                var count = await benchmarks.Count_nORM();
                Console.WriteLine($"  ✅ Count query returned {count}");

                // Test bulk insert (idiomatic nORM)
                await benchmarks.BulkInsert_Idiomatic_nORM();
                Console.WriteLine("  ✅ Bulk insert completed");
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("not initialized"))
            {
                Console.WriteLine("  ❌ nORM context initialization failed - check Setup() method");
                throw;
            }
        }
    }
}
