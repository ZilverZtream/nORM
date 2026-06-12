using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using nORM.Cli;
using nORM.Migration;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
    [Fact]
    public void Portability_certify_sqlite_connection_runs_live_target_floor_probes()
    {
        var root = FindRepositoryRoot();
        var scanPath = Path.Combine(Path.GetTempPath(), "norm_certify_scan_" + Guid.NewGuid().ToString("N"));
        var reportPath = Path.Combine(Path.GetTempPath(), "norm_certify_report_" + Guid.NewGuid().ToString("N") + ".json");

        try
        {
            Directory.CreateDirectory(scanPath);
            File.WriteAllText(Path.Combine(scanPath, "Repository.cs"), "public sealed class Repository { }", Encoding.UTF8);

            var result = RunCli(
                $"portability certify --scan-path {Quote(scanPath)} --providers sqlite --sqlite-connection {Quote("Data Source=:memory:")} --report {Quote(reportPath)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("Provider mobility certification PASS", result.Stdout, StringComparison.Ordinal);
            Assert.True(File.Exists(reportPath));

            using var report = JsonDocument.Parse(File.ReadAllText(reportPath));
            Assert.Equal("PASS", report.RootElement.GetProperty("Status").GetString());
            var sqliteTarget = report.RootElement.GetProperty("ProviderTargets").EnumerateArray()
                .Single(target => target.GetProperty("Provider").GetString() == "SQLite");
            Assert.Contains(sqliteTarget.GetProperty("Decisions").EnumerateArray(), decision =>
                decision.GetProperty("Feature").GetString() == "ServerVersion" &&
                decision.GetProperty("ActualServerVersion").ValueKind != JsonValueKind.Null);
            Assert.DoesNotContain(report.RootElement.GetProperty("Findings").EnumerateArray(), finding =>
                finding.GetProperty("Kind").GetString() == "provider-target-capability");
        }
        finally
        {
            TryDeleteDirectory(scanPath);
            try { File.Delete(reportPath); } catch { }
        }
    }

    [Fact]
    public void Portability_certify_accepts_ef_provider_package_target_aliases()
    {
        var root = FindRepositoryRoot();
        var scanPath = Path.Combine(Path.GetTempPath(), "norm_certify_alias_scan_" + Guid.NewGuid().ToString("N"));
        var reportPath = Path.Combine(Path.GetTempPath(), "norm_certify_alias_report_" + Guid.NewGuid().ToString("N") + ".json");
        const string providerAliases = "Microsoft.EntityFrameworkCore.SqlServer,Microsoft.EntityFrameworkCore.Sqlite,Npgsql.EntityFrameworkCore.PostgreSQL,Pomelo.EntityFrameworkCore.MySql";

        try
        {
            Directory.CreateDirectory(scanPath);
            File.WriteAllText(Path.Combine(scanPath, "Repository.cs"), "public sealed class Repository { }", Encoding.UTF8);

            var result = RunCli(
                $"portability certify --scan-path {Quote(scanPath)} --providers {Quote(providerAliases)} --report {Quote(reportPath)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("Provider mobility certification PASS", result.Stdout, StringComparison.Ordinal);
            Assert.True(File.Exists(reportPath));

            using var report = JsonDocument.Parse(File.ReadAllText(reportPath));
            Assert.Equal("PASS", report.RootElement.GetProperty("Status").GetString());
            Assert.Equal(
                new[] { "MySQL", "PostgreSQL", "SQL Server", "SQLite" },
                report.RootElement.GetProperty("ProviderTargets")
                    .EnumerateArray()
                    .Select(static target => target.GetProperty("Provider").GetString())
                    .OrderBy(static provider => provider, StringComparer.Ordinal)
                    .ToArray());
            Assert.DoesNotContain(report.RootElement.GetProperty("Findings").EnumerateArray(), finding =>
                finding.GetProperty("Kind").GetString() == "provider-target-unknown");
        }
        finally
        {
            TryDeleteDirectory(scanPath);
            try { File.Delete(reportPath); } catch { }
        }
    }

}
