using System;
using System.Threading.Tasks;
using System.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Providers;
using nORM.Sample.Store;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class SampleStoreScenarioTests
{
    [Fact]
    public async Task Sample_store_sqlite_scenario_proves_tenant_temporal_and_provider_swap_contract()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        var result = await StoreScenario.RunAsync(
            connection,
            new SqliteProvider(),
            new StoreProvider(StoreProviderKind.Sqlite, "sqlite"));

        Assert.True(result.Success, result.Summary);
    }

    [Fact]
    public async Task Sample_store_certify_provider_swap_writes_machine_readable_report()
    {
        var reportPath = Path.Combine(Path.GetTempPath(), "norm-provider-swap-" + Path.GetRandomFileName() + ".json");
        try
        {
            var exitCode = await StoreSampleProgram.RunAsync(new[]
            {
                "certify-provider-swap",
                "--providers",
                "sqlite,sqlite",
                "--report",
                reportPath
            });

            Assert.Equal(0, exitCode);
            Assert.True(File.Exists(reportPath));

            var report = JsonSerializer.Deserialize<ProviderSwapCertificationReport>(
                await File.ReadAllTextAsync(reportPath),
                ReportJsonOptions());

            Assert.NotNull(report);
            Assert.Equal("nORM-provider-mobility-v1", report!.Contract);
            Assert.True(report.Strict);
            Assert.Null(report.ScanPath);
            Assert.Equal("NotRequested", report.ScanStatus);
            Assert.Equal(0, report.ErrorCount);
            Assert.True(report.WarningCount > 0);
            Assert.Empty(report.Findings);
            Assert.Contains(report.Recommendations, recommendation =>
                recommendation.Kind == "provider-target-BulkInsert" &&
                recommendation.Priority == "P2");
            Assert.Contains(report.Recommendations, recommendation =>
                recommendation.Kind == "provider-target-DecimalComparisonNormalization" &&
                recommendation.Priority == "P2");
            var provider = Assert.Single(report.Providers);
            Assert.Equal("sqlite", provider.Provider);
            Assert.Equal("PASS", provider.Status);
            Assert.All(provider.CapabilityProfile, decision => Assert.NotNull(decision.ActualServerVersion));
            Assert.Contains(provider.CapabilityProfile, decision =>
                decision.Feature == ProviderMobilityProviderFeature.ServerVersion &&
                decision.Support == ProviderMobilitySupport.Portable &&
                decision.ActualServerVersion != null);
            Assert.Contains(provider.CapabilityProfile, decision =>
                decision.Feature == ProviderMobilityProviderFeature.BulkInsert &&
                decision.Support == ProviderMobilitySupport.Emulated);
            Assert.Contains("strict-provider-mobility-mode", provider.Checks);
            Assert.Contains("linq-where-select-dto-orderby-skip-take", provider.Checks);
            Assert.Contains("temporal-tenant-isolation", provider.Checks);
            Assert.Equal(StoreProviderKind.MySql, StoreProvider.Parse("mariadb")!.Kind);
            Assert.Equal("mysql", StoreProvider.Parse("mariadb")!.Name);
        }
        finally
        {
            if (File.Exists(reportPath))
                File.Delete(reportPath);
        }
    }

    [Fact]
    public async Task Sample_store_certification_scan_flags_provider_bound_usage()
    {
        var root = Path.Combine(Path.GetTempPath(), "norm-provider-scan-" + Path.GetRandomFileName());
        var reportPath = Path.Combine(root, "report.json");
        Directory.CreateDirectory(root);
        try
        {
            await File.WriteAllTextAsync(Path.Combine(root, "Repository.cs"), """
                using Npgsql;
                using nORM.Core;
                public sealed class Repository
                {
                    public async System.Threading.Tasks.Task Run(DbContext db)
                    {
                        await db.ExecuteStoredProcedureAsync<Row>("dbo.GetRows");
                        await db.FromSqlRawAsync<Row>("SELECT * FROM dbo.Rows");
                        await db.CreateCompiledQueryCommandAsync();
                        _ = db.Connection;
                        _ = db.Provider;
                        _ = db.Query("Rows");
                        _ = db.Database.CurrentTransaction;
                        await db.CreateSavepointAsync(null!, "sp");
                        System.Data.Common.DbCommand? command = null;
                        System.Data.Common.DbConnection? connection = null;
                        System.Data.Common.DbTransaction? transaction = null;
                        Microsoft.Data.SqlClient.SqlConnection? sqlConnection = null;
                        db.Options.CommandInterceptors.Add(null!);
                        _ = command;
                        _ = connection;
                        _ = transaction;
                        _ = sqlConnection;
                    }
                }
                [nORM.Query.SqlFunction("SOUNDEX({0})")]
                public static string CustomSoundex(string value) => value;
                [nORM.SourceGeneration.CompileTimeQuery("SELECT * FROM Rows")]
                public static partial System.Threading.Tasks.Task<System.Collections.Generic.List<Row>> RawGenerated(DbContext db);
                public sealed class Row { public int Id { get; set; } }
                """);
            await File.WriteAllTextAsync(Path.Combine(root, "Procedures.sql"), """
                CREATE PROCEDURE dbo.GetRows
                AS
                SELECT TOP (10) * FROM dbo.Rows WITH (NOLOCK) WHERE CreatedAt > DATEADD(day, -7, GETDATE());
                """);
            await File.WriteAllTextAsync(Path.Combine(root, "LegacyApp.csproj"), """
                <Project Sdk="Microsoft.NET.Sdk">
                  <ItemGroup>
                    <PackageReference
                      Include='Dapper'
                      Version='2.1.66' />
                    <PackageReference Update='Microsoft.EntityFrameworkCore.SqlServer' Version='9.0.0' />
                  </ItemGroup>
                </Project>
                """);
            await File.WriteAllTextAsync(Path.Combine(root, "Directory.Packages.props"), """
                <Project>
                  <ItemGroup>
                    <PackageVersion Include='Pomelo.EntityFrameworkCore.MySql' Version='9.0.0' />
                  </ItemGroup>
                </Project>
                """);

            var exitCode = await StoreSampleProgram.RunAsync(new[]
            {
                "certify-provider-swap",
                "--providers",
                "sqlite",
                "--scan-path",
                root,
                "--report",
                reportPath
            });

            Assert.Equal(1, exitCode);
            var report = JsonSerializer.Deserialize<ProviderSwapCertificationReport>(
                await File.ReadAllTextAsync(reportPath),
                ReportJsonOptions());

            Assert.NotNull(report);
            Assert.Equal(Path.GetFullPath(root), report!.ScanPath);
            Assert.Equal("Fail", report.ScanStatus);
            Assert.True(report.ErrorCount > 0);
            Assert.True(report.WarningCount > 0);
            Assert.Contains(report.Recommendations, recommendation => recommendation.Priority == "P0");
            Assert.Contains(report.Findings, f => f.Kind == "stored-procedure");
            Assert.Contains(report.Findings, f => f.Kind == "raw-sql");
            Assert.Contains(report.Findings, f => f.Kind == "custom-sql-function");
            Assert.Contains(report.Findings, f => f.Kind == "compile-time-raw-sql");
            Assert.Contains(report.Findings, f => f.Kind == "direct-connection");
            Assert.Contains(report.Findings, f => f.Kind == "direct-provider-access");
            Assert.Contains(report.Findings, f => f.Kind == "dynamic-table-query");
            Assert.Contains(report.Findings, f => f.Kind == "direct-transaction-access");
            Assert.Contains(report.Findings, f => f.Kind == "direct-command-access");
            Assert.Contains(report.Findings, f => f.Kind == "command-interceptor");
            Assert.Contains(report.Findings, f => f.Kind == "provider-bootstrap-connection");
            Assert.Contains(report.Findings, f => f.Kind == "provider-specific-package");
            Assert.Contains(report.Findings, f => f.Kind == "stored-procedure-definition");
            Assert.Contains(report.Findings, f => f.Kind == "sql-server-specific-sql");
            Assert.Contains(report.Findings, f => f.Kind == "provider-bootstrap-connection" && f.Severity == "Warning");
            Assert.Contains(report.Findings, f => f.Kind == "provider-specific-package" && f.Severity == "Warning");
            Assert.Contains(report.Findings, f => f.Kind == "raw-sql" && f.Severity == "Error");
            Assert.Contains(report.Findings, f =>
                f.Path.EndsWith("LegacyApp.csproj", StringComparison.OrdinalIgnoreCase) &&
                f.Kind == "raw-sql");
            Assert.Contains(report.Findings, f =>
                f.Path.EndsWith("Directory.Packages.props", StringComparison.OrdinalIgnoreCase) &&
                f.Kind == "provider-specific-package");
        }
        finally
        {
            if (Directory.Exists(root))
                Directory.Delete(root, recursive: true);
        }
    }

    [Fact]
    public async Task Sample_store_certify_provider_swap_reports_provider_open_failure_without_crashing()
    {
        var previous = Environment.GetEnvironmentVariable("NORM_SAMPLE_SQLSERVER");
        var reportPath = Path.Combine(Path.GetTempPath(), "norm-provider-open-failure-" + Path.GetRandomFileName() + ".json");
        try
        {
            Environment.SetEnvironmentVariable("NORM_SAMPLE_SQLSERVER", "not a valid connection string");

            var exitCode = await StoreSampleProgram.RunAsync(new[]
            {
                "certify-provider-swap",
                "--providers",
                "sqlserver",
                "--report",
                reportPath
            });

            Assert.Equal(1, exitCode);
            var report = JsonSerializer.Deserialize<ProviderSwapCertificationReport>(
                await File.ReadAllTextAsync(reportPath),
                ReportJsonOptions());

            Assert.NotNull(report);
            var provider = Assert.Single(report!.Providers);
            Assert.Equal("sqlserver", provider.Provider);
            Assert.Equal("FAIL", provider.Status);
            Assert.Contains("open failed", provider.Summary, StringComparison.OrdinalIgnoreCase);
            Assert.NotEmpty(provider.CapabilityProfile);
            Assert.True(report.ErrorCount > 0);
            Assert.Contains(report.Recommendations, recommendation =>
                recommendation.Kind == "provider-target-open" &&
                recommendation.Priority == "P1");
        }
        finally
        {
            Environment.SetEnvironmentVariable("NORM_SAMPLE_SQLSERVER", previous);
            if (File.Exists(reportPath))
                File.Delete(reportPath);
        }
    }

    private static JsonSerializerOptions ReportJsonOptions()
    {
        var options = new JsonSerializerOptions { PropertyNameCaseInsensitive = true };
        options.Converters.Add(new JsonStringEnumConverter());
        return options;
    }
}
