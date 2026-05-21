using System;
using System.Collections;
using System.Linq;
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
                        await RunFastNormBenchmarks(BuildFastBenchmarkArgs(args.Skip(1).ToArray()));
                        return;

                    case "--complex":
                        Console.WriteLine("Running focused complex-query benchmarks...");
                        RunFilteredBenchmarks(new[] { "--filter", "nORM.Benchmarks.OrmBenchmarks.Query_Complex*" });
                        return;

                    case "--provider-matrix":
                        Console.WriteLine("Running SQLite/SQL Server/PostgreSQL provider matrix benchmarks...");
                        RunProviderMatrixBenchmarks(args.Skip(1).ToArray());
                        return;

                    case "--filter":
                        RunFilteredBenchmarks(args.Length == 2
                            ? new[] { "--filter", args[1] }
                            : args.Skip(1).ToArray());
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

                if (HasBenchmarkFailures(summary))
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
            Console.WriteLine("  --fast <filter>   Fast nORM-only benchmarks matching a filter");
            Console.WriteLine("  --norm-only       Same as --fast");
            Console.WriteLine("  --complex         Focused complex-query comparison");
            Console.WriteLine("  --provider-matrix Run full provider matrix comparison across SQLite, SQL Server, PostgreSQL");
            Console.WriteLine("  --filter <pattern> Run benchmarks matching a BenchmarkDotNet filter");
            Console.WriteLine("  --help            Show this help");
            Console.WriteLine();
            Console.WriteLine("Examples:");
            Console.WriteLine("  dotnet run --quick      # Verify nORM works");
            Console.WriteLine("  dotnet run --fast       # Debug nORM performance only");
            Console.WriteLine("  dotnet run --fast Query_Complex_Compiled");
            Console.WriteLine("  dotnet run --complex    # Compare complex-query implementations");
            Console.WriteLine("  dotnet run -- --provider-matrix --filter *Query_Complex*");
            Console.WriteLine("  dotnet run              # Full comparison benchmark");
        }

        private static void RunFilteredBenchmarks(string[] benchmarkArgs)
        {
            var config = ManualConfig.Create(DefaultConfig.Instance)
                .WithOptions(ConfigOptions.DisableOptimizationsValidator);

            BenchmarkSwitcher
                .FromAssembly(typeof(Program).Assembly)
                .Run(benchmarkArgs, config);
        }

        private static string[] BuildFastBenchmarkArgs(string[] args)
        {
            if (args.Length == 0)
                return args;

            return args[0].StartsWith("-", StringComparison.Ordinal)
                ? args
                : new[] { "--filter", $"*{args[0]}*" }.Concat(args.Skip(1)).ToArray();
        }

        private static async Task RunFastNormBenchmarks(string[] benchmarkArgs)
        {
            Console.WriteLine("Running fast nORM-only benchmarks...");
            Console.WriteLine("This will quickly test nORM performance with smaller datasets.");
            Console.WriteLine();

            try
            {
                var config = ManualConfig.Create(DefaultConfig.Instance)
                    .WithOptions(ConfigOptions.DisableOptimizationsValidator);

                var argsToRun = benchmarkArgs.Length == 0
                    ? new[] { "--filter", "*FastNormBenchmarks*" }
                    : benchmarkArgs;

                await Task.Run(() => {
                    var summaries = BenchmarkSwitcher
                        .FromTypes(new[] { typeof(FastNormBenchmarks) })
                        .Run(argsToRun, config);
                    if (summaries.Any(HasBenchmarkFailures))
                        throw new InvalidOperationException("Fast nORM benchmark validation failed.");
                    return summaries;
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

        private static void RunProviderMatrixBenchmarks(string[] benchmarkArgs)
        {
            var config = ManualConfig.Create(DefaultConfig.Instance)
                .WithOptions(ConfigOptions.DisableOptimizationsValidator);

            var argsToRun = benchmarkArgs.Length == 0
                ? new[] { "--filter", "*ProviderMatrixBenchmarks*" }
                : benchmarkArgs;

            var summaries = BenchmarkSwitcher
                .FromTypes(new[] { typeof(ProviderMatrixBenchmarks) })
                .Run(argsToRun, config);

            if (summaries.Any(HasBenchmarkFailures))
                throw new InvalidOperationException("Provider matrix benchmark validation failed.");
        }

        private static bool HasBenchmarkFailures(object summary)
        {
            if (summary.GetType().GetProperty("HasCriticalValidationErrors")?.GetValue(summary) is true)
                return true;

            if (summary.GetType().GetProperty("Reports")?.GetValue(summary) is not IEnumerable reports)
                return false;

            foreach (var report in reports)
            {
                if (report == null)
                    return true;

                if (report.GetType().GetProperty("Success")?.GetValue(report) is bool success && !success)
                    return true;

                var resultStatistics = report.GetType().GetProperty("ResultStatistics")?.GetValue(report);
                if (resultStatistics == null)
                    return true;

                if (report.GetType().GetProperty("ExecuteResults")?.GetValue(report) is not IEnumerable executeResults)
                    continue;

                var sawExecuteResult = false;
                foreach (var executeResult in executeResults)
                {
                    sawExecuteResult = true;
                    if (executeResult?.GetType().GetProperty("ExitCode")?.GetValue(executeResult) is int exitCode && exitCode != 0)
                        return true;
                }

                if (!sawExecuteResult)
                    return true;
            }

            return false;
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
                Console.WriteLine($"❌ Quick test failed: {ex}");
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
