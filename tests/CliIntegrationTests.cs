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

[Xunit.Trait("Category", "Fast")]
public class CliIntegrationTests
{
    private static readonly TimeSpan ProcessTimeout = TimeSpan.FromMinutes(3);

    [Fact]
    public void Database_update_missing_assembly_returns_nonzero_without_leaking_connection_secret()
    {
        var root = FindRepositoryRoot();
        var secret = "TopSecret123!";
        var connection = $"Server=localhost;Database=norm;User ID=sa;Password={secret};Encrypt=True;TrustServerCertificate=True";
        var missingAssembly = Path.Combine(root, "missing-migrations.dll");

        var result = RunCli(
            $"database update --connection {Quote(connection)} --provider sqlserver --assembly {Quote(missingAssembly)}",
            root);

        Assert.Equal(2, result.ExitCode);
        Assert.Contains("not found", result.Stderr, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(secret, result.Stdout, StringComparison.Ordinal);
        Assert.DoesNotContain(secret, result.Stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Database_drop_sqlite_requires_yes_or_dry_run()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_drop_guard_" + Guid.NewGuid().ToString("N") + ".db");
        File.WriteAllText(dbFile, "not-empty", Encoding.UTF8);

        try
        {
            var result = RunCli(
                $"database drop --connection {Quote($"Data Source={dbFile}")} --provider sqlite",
                root);

            Assert.Equal(3, result.ExitCode);
            Assert.Contains("--yes", result.Stderr, StringComparison.Ordinal);
            Assert.True(File.Exists(dbFile));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
        }
    }

    [Fact]
    public void Database_drop_sqlite_dry_run_does_not_delete_file()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_drop_dry_" + Guid.NewGuid().ToString("N") + ".db");
        File.WriteAllText(dbFile, "not-empty", Encoding.UTF8);

        try
        {
            var result = RunCli(
                $"database drop --connection {Quote($"Data Source={dbFile}")} --provider sqlite --dry-run",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("Would delete", result.Stdout, StringComparison.Ordinal);
            Assert.True(File.Exists(dbFile));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
        }
    }

    [Fact]
    public void Database_drop_sqlite_yes_deletes_file()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_drop_yes_" + Guid.NewGuid().ToString("N") + ".db");
        File.WriteAllText(dbFile, "not-empty", Encoding.UTF8);

        var result = RunCli(
            $"database drop --connection {Quote($"Data Source={dbFile}")} --provider sqlite --yes",
            root);

        Assert.Equal(0, result.ExitCode);
        Assert.Contains("deleted", result.Stdout, StringComparison.OrdinalIgnoreCase);
        Assert.False(File.Exists(dbFile));
    }

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

    [Fact]
    public void Scaffold_help_describes_bounded_contract_and_warning_reports()
    {
        var root = FindRepositoryRoot();

        var result = RunCli("scaffold --help", root);

        Assert.Equal(0, result.ExitCode);
        Assert.Contains("bounded v1 nORM model", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("nORM.ScaffoldWarnings.md/json", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--fail-on-warnings", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--dry-run", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--context-dir", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--context-namespace", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--project", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--startup-project", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("-s", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--framework", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--configuration", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--runtime", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--msbuildprojectextensionspath", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--no-build", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--json", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--verbose", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--no-color", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--prefix-output", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--schema", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--table", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--no-pluralize", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--use-database-names", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--no-onconfiguring", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--output-dir", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--data-annotations", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--force", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("EF Core provider package name", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--emit-routine-stubs", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--emit-view-entities", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--emit-query-artifacts", result.Stdout, StringComparison.Ordinal);

        var rootHelp = RunCli("--help", root);
        Assert.Equal(0, rootHelp.ExitCode);
        Assert.Contains("dbcontext", rootHelp.Stdout, StringComparison.Ordinal);
        Assert.Contains("EF-style DbContext command aliases", rootHelp.Stdout, StringComparison.Ordinal);

        var efStyleResult = RunCli("dbcontext scaffold --help", root);
        Assert.Equal(0, efStyleResult.ExitCode);
        Assert.Contains("bounded v1 nORM model", efStyleResult.Stdout, StringComparison.Ordinal);
        Assert.Contains("--context-dir", efStyleResult.Stdout, StringComparison.Ordinal);

        var efStyleGroupHelp = RunCli("dbcontext --help", root);
        Assert.Equal(0, efStyleGroupHelp.ExitCode);
        Assert.Contains("bounded v1 nORM model", efStyleGroupHelp.Stdout, StringComparison.Ordinal);

        var unsupportedEfSubcommand = RunCli("dbcontext list", root);
        Assert.NotEqual(0, unsupportedEfSubcommand.ExitCode);
        Assert.Contains("dbcontext", unsupportedEfSubcommand.Stderr + unsupportedEfSubcommand.Stdout, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Scaffold_json_outputs_machine_readable_summary()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_json_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_json_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --connection {Quote($"Data Source={dbFile}")} --provider sqlite --output-dir {Quote(output)} --namespace CliScaffolded --context CliCtx --json --verbose --no-color --prefix-output",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.DoesNotContain("Scaffolding completed", result.Stdout, StringComparison.Ordinal);

            using var document = JsonDocument.Parse(result.Stdout);
            var json = document.RootElement;
            Assert.Equal("succeeded", json.GetProperty("status").GetString());
            Assert.False(json.GetProperty("dryRun").GetBoolean());
            Assert.Equal(Path.GetFullPath(output), json.GetProperty("outputDirectory").GetString());
            Assert.False(json.GetProperty("warnings").GetProperty("hasDiagnostics").GetBoolean());
            Assert.False(json.GetProperty("warnings").GetProperty("reportsWritten").GetBoolean());
            Assert.Equal(0, json.GetProperty("warnings").GetProperty("totalWarnings").GetInt32());
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_json_outputs_machine_readable_validation_failure()
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} --json",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.True(string.IsNullOrWhiteSpace(result.Stderr), result.Stderr);

        using var document = JsonDocument.Parse(result.Stdout);
        var json = document.RootElement;
        Assert.Equal("failed", json.GetProperty("status").GetString());
        Assert.False(json.GetProperty("dryRun").GetBoolean());
        Assert.False(json.GetProperty("warnings").GetProperty("hasDiagnostics").GetBoolean());
        Assert.Equal(0, json.GetProperty("warnings").GetProperty("totalWarnings").GetInt32());
        Assert.Contains("Scaffold requires a database provider", json.GetProperty("error").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void Scaffold_repeatable_table_filter_preserves_literal_commas()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_table_filter_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_table_filter_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE "Keep,Me" (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    CREATE TABLE SkipMe (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --table {Quote("Keep,Me")}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var generatedText = string.Join(
                Environment.NewLine,
                Directory.EnumerateFiles(output, "*.cs", SearchOption.AllDirectories)
                    .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
                    .Select(File.ReadAllText));

            Assert.Contains("\"Keep,Me\"", generatedText, StringComparison.Ordinal);
            Assert.DoesNotContain("\"SkipMe\"", generatedText, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_blank_table_filter_fails_without_generating_all_tables()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_table_filter_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_table_filter_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE KeepOne (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                    CREATE TABLE KeepTwo (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --table {Quote(" ")}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("Scaffold --table values must be non-empty", result.Stderr, StringComparison.Ordinal);
            Assert.False(Directory.Exists(output));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_empty_schema_csv_entry_fails_without_generating_all_tables()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_empty_schema_filter_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_empty_schema_filter_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE KeepOne (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                    CREATE TABLE KeepTwo (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --schemas {Quote("main,,")}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("Scaffold --schemas must not contain empty filter entries", result.Stderr, StringComparison.Ordinal);
            Assert.False(Directory.Exists(output));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_table_filter_accepts_multiple_values_after_single_option()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_table_multi_value_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_table_multi_value_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE KeepOne (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    CREATE TABLE KeepTwo (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    CREATE TABLE SkipMe (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --table KeepOne KeepTwo",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, "KeepOne.cs")));
            Assert.True(File.Exists(Path.Combine(output, "KeepTwo.cs")));
            Assert.False(File.Exists(Path.Combine(output, "SkipMe.cs")));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_table_filter_view_generates_query_artifact_without_emit_switch()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_view_filter_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_view_filter_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Existing (
                        Id INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL
                    );
                    CREATE VIEW ExistingView AS SELECT Id, Name FROM Existing;
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --table ExistingView",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var viewCode = File.ReadAllText(Path.Combine(output, "ExistingView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"ExistingView\")]", viewCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<ExistingView> ExistingViews", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "Existing.cs")));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_schema_qualified_table_filter_view_generates_query_artifact_without_emit_switch()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_qualified_view_filter_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_qualified_view_filter_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Existing (
                        Id INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL
                    );
                    CREATE VIEW ExistingView AS SELECT Id, Name FROM Existing;
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --table main.ExistingView",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var viewCode = File.ReadAllText(Path.Combine(output, "ExistingView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"ExistingView\")]", viewCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<ExistingView> ExistingViews", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "Existing.cs")));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_schema_filter_view_generates_query_artifact_without_emit_switch()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_view_filter_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_view_filter_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Existing (
                        Id INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL
                    );
                    CREATE VIEW ExistingView AS SELECT Id, Name FROM Existing;
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --schema main",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var tableCode = File.ReadAllText(Path.Combine(output, "Existing.cs"));
            var viewCode = File.ReadAllText(Path.Combine(output, "ExistingView.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("[Table(\"Existing\")]", tableCode, StringComparison.Ordinal);
            Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"ExistingView\")]", viewCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Existing> Existings", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<ExistingView> ExistingViews", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_unfiltered_view_generates_read_only_query_artifact_by_default()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_default_view_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_default_view_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Product (
                        Id INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL
                    );
                    CREATE VIEW ProductReport AS SELECT Id, Name FROM Product;
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, "Product.cs")));
            var viewCode = File.ReadAllText(Path.Combine(output, "ProductReport.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            var warnings = File.ReadAllText(Path.Combine(output, "nORM.ScaffoldWarnings.md"));

            Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"ProductReport\")]", viewCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<ProductReport> ProductReports", contextCode, StringComparison.Ordinal);
            Assert.Contains("MissingPrimaryKey", warnings, StringComparison.Ordinal);
            Assert.DoesNotContain("Skipped Database Objects", warnings, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_ef_style_aliases_generate_requested_table()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_aliases_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_aliases_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Customer (
                        Id INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL
                    );
                    CREATE TABLE Ignored (
                        Id INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output-dir {Quote(output)} -n CliScaffolded -c CliCtx -t Customer -d -f",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            Assert.False(File.Exists(Path.Combine(output, "Ignored.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace CliScaffolded;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_ef_style_positional_connection_and_provider_generate_model()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_positional_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_positional_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} sqlite --output-dir {Quote(output)} -n CliScaffolded -c CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace CliScaffolded;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_dbcontext_scaffold_alias_generates_model()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_dbcontext_alias_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_dbcontext_alias_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"dbcontext scaffold {Quote($"Data Source={dbFile}")} Microsoft.EntityFrameworkCore.Sqlite --output-dir {Quote(output)} -n CliScaffolded -c CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace CliScaffolded;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliCtx", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_ef_provider_package_name_generates_model()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_provider_package_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_provider_package_out_" + Guid.NewGuid().ToString("N"));
        var msbuildProjectExtensionsPath = Path.Combine(Path.GetTempPath(), "norm_scaffold_provider_package_obj_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} Microsoft.EntityFrameworkCore.Sqlite --msbuildprojectextensionspath {Quote(msbuildProjectExtensionsPath)} --output-dir {Quote(output)} -n CliScaffolded -c CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
            TryDeleteDirectory(msbuildProjectExtensionsPath);
        }
    }

    [Fact]
    public void Scaffold_project_option_resolves_relative_output_and_project_namespace()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_project_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "project.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "ProjectApp.csproj");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                """
                <Project>
                  <PropertyGroup>
                    <RootNamespace>Inherited.Project.Namespace</RootNamespace>
                    <AssemblyName>InheritedProject</AssemblyName>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Project.Default.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --connection {Quote($"Data Source={dbFile}")} --provider sqlite --project {Quote(projectPath)} --startup-project {Quote(projectPath)} --no-build --framework net8.0 --configuration Release --runtime win-x64 --output-dir Models --context-dir Data/Contexts --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var entityCode = File.ReadAllText(Path.Combine(output, "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(projectDir, "Data", "Contexts", "CliCtx.cs"));
            Assert.Contains("namespace Project.Default.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Project.Default.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Project.Default.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_project_inherits_directory_build_props_metadata()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_project_props_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "props.db");
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "PropsApp.csproj");
        var userSecretsId = "norm-test-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForTest(userSecretsId);

        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                $$"""
                <Project>
                  <PropertyGroup>
                    <RootNamespace>Inherited.Project.Namespace</RootNamespace>
                    <UserSecretsId>{{userSecretsId}}</UserSecretsId>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Inherited.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
        }
    }

    [Fact]
    public void Scaffold_project_nullable_disable_generates_nullable_disabled_code()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_project_nullable_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "nullable.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NullableDisabledProject.csproj");
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                """
                <Project>
                  <PropertyGroup>
                    <Nullable>enable</Nullable>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
            File.WriteAllText(
                projectPath,
                $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <Nullable>disable</Nullable>
                    <WarningsAsErrors>CS8618;CS8632</WarningsAsErrors>
                    <ImplicitUsings>disable</ImplicitUsings>
                  </PropertyGroup>
                  <ItemGroup>
                    <Reference Include="nORM">
                      <HintPath>{{normAssembly}}</HintPath>
                    </Reference>
                  </ItemGroup>
                </Project>
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Customer (
                        Id INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL,
                        Notes TEXT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --connection {Quote($"Data Source={dbFile}")} --provider sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(projectDir, "Models", "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(projectDir, "Models", "CliCtx.cs"));
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string Name { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string Notes { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("string? Notes", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("= default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("DbContextOptions options = null", contextCode, StringComparison.Ordinal);

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_project_inherits_nullable_enable_from_directory_build_props()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_project_nullable_props_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "nullable.db");
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "PropsEnabledProject.csproj");
        var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                """
                <Project>
                  <PropertyGroup>
                    <Nullable>enable</Nullable>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
            File.WriteAllText(
                projectPath,
                $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <ImplicitUsings>disable</ImplicitUsings>
                  </PropertyGroup>
                  <ItemGroup>
                    <Reference Include="nORM">
                      <HintPath>{{normAssembly}}</HintPath>
                    </Reference>
                  </ItemGroup>
                </Project>
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Customer (
                        Id INTEGER PRIMARY KEY,
                        Name TEXT NOT NULL,
                        Notes TEXT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --connection {Quote($"Data Source={dbFile}")} --provider sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(projectDir, "Models", "Customer.cs"));
            Assert.Contains("#nullable enable", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string Name { get; set; } = default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string? Notes { get; set; }", entityCode, StringComparison.Ordinal);

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_current_directory_project_is_inferred_when_project_is_omitted()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_current_project_" + Guid.NewGuid().ToString("N"));
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "CurrentApp.csproj");
        var dbFile = Path.Combine(projectDir, "current.db");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Current.Project.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --connection {Quote($"Data Source={dbFile}")} --provider sqlite --output-dir Models --context-dir Data/Contexts --context CliCtx",
                projectDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityCode = File.ReadAllText(Path.Combine(output, "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(projectDir, "Data", "Contexts", "CliCtx.cs"));
            Assert.Contains("namespace Current.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Current.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Current.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_supplies_project_and_context_defaults()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "ConfiguredApp.csproj");
        var dbFile = Path.Combine(tempRoot, "configured.db");

        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "project": "src/App",
                  "context": "Configured.Contexts.ConfiguredCtx",
                  "framework": "net8.0",
                  "configuration": "Release",
                  "runtime": "win-x64",
                  "verbose": true,
                  "noColor": true,
                  "prefixOutput": false
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Configured.Project.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} Microsoft.EntityFrameworkCore.Sqlite --output-dir Models",
                workDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityCode = File.ReadAllText(Path.Combine(output, "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "ConfiguredCtx.cs"));
            Assert.Contains("namespace Configured.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Configured.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Configured.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class ConfiguredCtx", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_startup_project_resolves_named_connection()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_startup_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var modelProjectDir = Path.Combine(tempRoot, "src", "Model");
        var startupProjectDir = Path.Combine(tempRoot, "src", "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupApp.csproj");
        var dbFile = Path.Combine(tempRoot, "startup-config.db");

        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "project": "src/Model",
                  "startupProject": "src/Startup",
                  "context": "ConfiguredCtx"
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                modelProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Configured.Model.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Configured.Startup.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --output-dir Models",
                workDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "ConfiguredCtx.cs"));
            Assert.Contains("namespace Configured.Model.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_rejects_non_boolean_common_flags()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_bool_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");

        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "verbose": "true"
                }
                """,
                Encoding.UTF8);

            var result = RunCli(
                $"scaffold {Quote("Data Source=:memory:")} sqlite --output-dir Models",
                workDir);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("EF tool configuration property 'verbose' must be a boolean", result.Stderr, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_lowercase_named_connection_shorthand_from_project_appsettings_generates_model()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_named_connection_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "named.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedApp.csproj");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Named.Connection.App</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("name=AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Named.Connection.App.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_named_connection_environment_variable_overrides_project_appsettings()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_named_envvar_" + Guid.NewGuid().ToString("N"));
        var envDbFile = Path.Combine(tempRoot, "env.db");
        var appsettingsDbFile = Path.Combine(tempRoot, "appsettings.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedEnvVarApp.csproj");
        var connectionName = "EnvDb" + Guid.NewGuid().ToString("N");
        var environmentKey = "ConnectionStrings__" + connectionName;

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Named.EnvVar.App</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Data Source={{appsettingsDbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={envDbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE EnvCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={appsettingsDbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE AppsettingsCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            Environment.SetEnvironmentVariable(environmentKey, $"Data Source={envDbFile}");
            var result = RunCli(
                $"scaffold {Quote("Name=" + connectionName)} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "EnvCustomer.cs")));
            Assert.False(File.Exists(Path.Combine(output, "AppsettingsCustomer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Named.EnvVar.App.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<EnvCustomer> EnvCustomers", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("AppsettingsCustomer", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable(environmentKey, null);
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_named_connection_project_user_secrets_override_same_directory_startup_appsettings()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_secret_precedence_" + Guid.NewGuid().ToString("N"));
        var projectDir = Path.Combine(tempRoot, "App");
        var modelProjectPath = Path.Combine(projectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(projectDir, "StartupApp.csproj");
        var secretDbFile = Path.Combine(tempRoot, "secret.db");
        var appsettingsDbFile = Path.Combine(tempRoot, "appsettings.db");
        var userSecretsId = "norm-test-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForTest(userSecretsId);

        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            File.WriteAllText(
                modelProjectPath,
                $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Secrets.Override.Model</RootNamespace>
                    <UserSecretsId>{{userSecretsId}}</UserSecretsId>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Secrets.Override.Startup</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{appsettingsDbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{secretDbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={secretDbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE SecretCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={appsettingsDbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE AppsettingsCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(modelProjectPath)} --startup-project {Quote(startupProjectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "SecretCustomer.cs")));
            Assert.False(File.Exists(Path.Combine(output, "AppsettingsCustomer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Secrets.Override.Model.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<SecretCustomer> SecretCustomers", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("AppsettingsCustomer", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
        }
    }

    [Fact]
    public void Scaffold_named_connection_uses_pass_through_environment_for_appsettings()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_named_environment_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "named_env.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedEnvironmentApp.csproj");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Named.Environment.App</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.Production.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx -- --environment Production",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Named.Environment.App.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_named_connection_uses_ambient_environment_for_appsettings()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_named_ambient_environment_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "named_ambient_env.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedAmbientEnvironmentApp.csproj");
        var previousAspNetCoreEnvironment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT");
        var previousDotnetEnvironment = Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT");

        try
        {
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Named.Ambient.Environment.App</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.Staging.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", "Staging");
            Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", null);

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Named.Ambient.Environment.App.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            Environment.SetEnvironmentVariable("ASPNETCORE_ENVIRONMENT", previousAspNetCoreEnvironment);
            Environment.SetEnvironmentVariable("DOTNET_ENVIRONMENT", previousDotnetEnvironment);
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_pass_through_environment_without_value_fails()
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} sqlite -- --environment",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("EF-style application argument '--environment' requires a value", result.Stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Scaffold_named_connection_from_project_user_secrets_generates_model()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_user_secrets_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "secret.db");
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "SecretApp.csproj");
        var userSecretsId = "norm-test-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForTest(userSecretsId);

        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            File.WriteAllText(
                projectPath,
                $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>User.Secrets.App</RootNamespace>
                    <UserSecretsId>{{userSecretsId}}</UserSecretsId>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace User.Secrets.App.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
        }
    }

    [Fact]
    public void Scaffold_ignores_ef_style_application_args_after_double_dash()
    {
        var root = FindRepositoryRoot();
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_app_args_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(output, "app_args.db");

        try
        {
            Directory.CreateDirectory(output);
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} sqlite --output-dir {Quote(output)} -- --environment Production --tenant Contoso",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
        }
        finally
        {
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_unmatched_option_before_double_dash_fails()
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} sqlite --conection typo",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Unrecognized scaffold argument(s)", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("accepted only after '--'", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("'--environment' is used for named-connection appsettings lookup", result.Stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Scaffold_unmatched_option_before_double_dash_is_not_masked_by_matching_app_arg()
    {
        var root = FindRepositoryRoot();

        var result = RunCli(
            $"scaffold {Quote("Data Source=:memory:")} sqlite --bad -- --bad",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Unrecognized scaffold argument(s)", result.Stderr, StringComparison.Ordinal);
        Assert.Contains("accepted only after '--'", result.Stderr, StringComparison.Ordinal);
    }

    [Fact]
    public void Scaffold_startup_project_short_alias_resolves_named_connection()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_named_startup_short_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "startup.db");
        var projectDir = Path.Combine(tempRoot, "ModelProject");
        var startupDir = Path.Combine(tempRoot, "StartupProject");
        var projectPath = Path.Combine(projectDir, "ModelProject.csproj");
        var startupProjectPath = Path.Combine(startupDir, "StartupProject.csproj");

        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(startupDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Startup.Short.Model</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Startup.Short.Host</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "AppDb": "Data Source={{dbFile.Replace("\\", "\\\\")}}"
                  }
                }
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Name=ConnectionStrings:AppDb")} Microsoft.EntityFrameworkCore.Sqlite --project {Quote(projectPath)} -s {Quote(startupProjectPath)} --output-dir Models --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace Startup.Short.Model.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_connection_option_and_positional_provider_generate_model()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_mixed_positional_provider_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_mixed_positional_provider_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --connection {Quote($"Data Source={dbFile}")} sqlite --output-dir {Quote(output)} -n CliScaffolded -c CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_explicit_connection_and_provider_options_override_positionals()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_positional_override_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_positional_override_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote("Data Source=does-not-exist.db")} invalidprovider --connection {Quote($"Data Source={dbFile}")} --provider sqlite --output {Quote(output)} --namespace CliScaffolded --context CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            Assert.True(File.Exists(Path.Combine(output, "Customer.cs")));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
            try { File.Delete(Path.Combine(root, "does-not-exist.db")); } catch { }
        }
    }

    [Fact]
    public void Scaffold_existing_output_requires_force_by_default()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_default_no_overwrite_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_default_no_overwrite_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            Directory.CreateDirectory(output);
            var existingFile = Path.Combine(output, "Customer.cs");
            File.WriteAllText(existingFile, "// keep me");

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("already exists", result.Stderr + result.Stdout, StringComparison.OrdinalIgnoreCase);
            Assert.Equal("// keep me", File.ReadAllText(existingFile));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_force_and_no_overwrite_conflict_fails_before_writing()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_force_conflict_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_force_conflict_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} -o {Quote(output)} --namespace CliScaffolded --context CliCtx --force --no-overwrite",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("--force", result.Stderr, StringComparison.Ordinal);
            Assert.Contains("--no-overwrite", result.Stderr, StringComparison.Ordinal);
            Assert.False(Directory.Exists(output));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_schema_filter_generates_matching_schema_tables()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE SchemaFiltered (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --schema main",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "SchemaFiltered.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("[Table(\"SchemaFiltered\")]", entityCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<SchemaFiltered> SchemaFilteredRows", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_schema_filter_accepts_multiple_values_after_single_option()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_multi_value_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_schema_multi_value_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE SchemaFiltered (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --schema main main",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "SchemaFiltered.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("[Table(\"SchemaFiltered\")]", entityCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<SchemaFiltered> SchemaFilteredRows", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_no_pluralize_generates_singular_query_properties()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_no_pluralize_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_no_pluralize_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Category (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --no-pluralize",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("IQueryable<Category> Category", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("IQueryable<Category> Categories", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_context_dir_and_namespace_generate_split_context()
    {
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_out_" + Guid.NewGuid().ToString("N"));
        var workDir = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_work_" + Guid.NewGuid().ToString("N"));

        try
        {
            Directory.CreateDirectory(workDir);
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE ContextPlaced (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded.Entities --context CliCtx --context-dir Data/Contexts --context-namespace CliScaffolded.Contexts",
                workDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "ContextPlaced.cs"));
            var contextCode = File.ReadAllText(Path.Combine(workDir, "Data", "Contexts", "CliCtx.cs"));
            Assert.Contains("namespace CliScaffolded.Entities;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace CliScaffolded.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using CliScaffolded.Entities;", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<ContextPlaced> ContextPlacedRows", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
            TryDeleteDirectory(workDir);
        }
    }

    [Fact]
    public void Scaffold_context_dir_absolute_path_fails_before_writing()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_absolute_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_absolute_out_" + Guid.NewGuid().ToString("N"));
        var absoluteContextDir = Path.Combine(Path.GetTempPath(), "norm_scaffold_context_dir_absolute_ctx_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --context-dir {Quote(absoluteContextDir)}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("Scaffold --context-dir must be a relative path", result.Stderr, StringComparison.Ordinal);
            Assert.False(Directory.Exists(output));
            Assert.False(Directory.Exists(absoluteContextDir));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
            TryDeleteDirectory(absoluteContextDir);
        }
    }

    [Fact]
    public void Scaffold_invalid_namespace_fails_before_writing()
    {
        var root = FindRepositoryRoot();
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_invalid_namespace_out_" + Guid.NewGuid().ToString("N"));

        var result = RunCli(
            $"scaffold --provider sqlite --connection {Quote("Data Source=:memory:")} --output {Quote(output)} --namespace Bad-Name --context CliCtx",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Scaffold --namespace 'Bad-Name' is not a valid C# namespace", result.Stderr, StringComparison.Ordinal);
        Assert.False(Directory.Exists(output));
    }

    [Fact]
    public void Scaffold_blank_explicit_string_options_fail_before_writing()
    {
        var root = FindRepositoryRoot();
        var cases = new[]
        {
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_namespace_" + Guid.NewGuid().ToString("N")))} --namespace {Quote(" ")} --context CliCtx",
                Message: "Scaffold --namespace must not be blank."
            ),
            (
                Arguments: $"--output {Quote(Path.Combine(Path.GetTempPath(), "norm_scaffold_blank_context_" + Guid.NewGuid().ToString("N")))} --namespace CliScaffolded --context {Quote(" ")}",
                Message: "Scaffold --context must not be blank."
            ),
            (
                Arguments: $"--output {Quote(" ")} --namespace CliScaffolded --context CliCtx",
                Message: "Scaffold --output must not be blank."
            )
        };

        foreach (var (arguments, message) in cases)
        {
            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote("Data Source=:memory:")} {arguments}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains(message, result.Stderr, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void Scaffold_invalid_context_namespace_fails_before_writing()
    {
        var root = FindRepositoryRoot();
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_invalid_context_namespace_out_" + Guid.NewGuid().ToString("N"));

        var result = RunCli(
            $"scaffold --provider sqlite --connection {Quote("Data Source=:memory:")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --context-namespace Bad-Name",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Scaffold --context-namespace 'Bad-Name' is not a valid C# namespace", result.Stderr, StringComparison.Ordinal);
        Assert.False(Directory.Exists(output));
    }

    [Fact]
    public void Scaffold_invalid_explicit_context_name_fails_before_writing()
    {
        var root = FindRepositoryRoot();
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_invalid_context_name_out_" + Guid.NewGuid().ToString("N"));

        var result = RunCli(
            $"scaffold --provider sqlite --connection {Quote("Data Source=:memory:")} --output {Quote(output)} --namespace CliScaffolded --context Bad-Name",
            root);

        Assert.NotEqual(0, result.ExitCode);
        Assert.Contains("Scaffold --context class name 'Bad-Name' is not a valid C# type identifier", result.Stderr, StringComparison.Ordinal);
        Assert.False(Directory.Exists(output));
    }

    [Fact]
    public void Scaffold_qualified_context_name_derives_context_namespace()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_qualified_context_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_qualified_context_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE ContextQualified (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded.Entities --context CliScaffolded.Contexts.CliCtx",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "ContextQualified.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("namespace CliScaffolded.Entities;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace CliScaffolded.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using CliScaffolded.Entities;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliCtx", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_omitted_context_name_derives_from_sqlite_database_file()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_default_context_" + Guid.NewGuid().ToString("N"));
        var dbFile = Path.Combine(tempRoot, "customer-orders.db");
        var output = Path.Combine(tempRoot, "Models");

        try
        {
            Directory.CreateDirectory(tempRoot);
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CustomerOrdersContext.cs"));
            Assert.Contains("public partial class CustomerOrdersContext", contextCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customers", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_use_database_names_preserves_legal_clr_names()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_database_names_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_database_names_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE order_line (
                        order_id INTEGER PRIMARY KEY,
                        SKU TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --use-database-names",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "order_line.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("public partial class order_line", entityCode, StringComparison.Ordinal);
            Assert.Contains("public long order_id { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string SKU { get; set; } = default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<order_line> order_lines", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("OrderLine", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("OrderId", entityCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_no_onconfiguring_is_accepted_without_emitting_onconfiguring()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_no_onconfiguring_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_no_onconfiguring_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --no-onconfiguring",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.DoesNotContain("OnConfiguring", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Data Source=", contextCode, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_fail_on_warnings_returns_nonzero_after_writing_report()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_warn_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_warn_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE WarningRow (
                        Status TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --fail-on-warnings",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("Scaffolding produced warnings", result.Stderr + result.Stdout, StringComparison.Ordinal);
            Assert.Contains("Scaffolding warning summary", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("SCF116=1", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("table-shape=1", result.Stdout, StringComparison.Ordinal);
            var warningFile = Path.Combine(output, "nORM.ScaffoldWarnings.md");
            Assert.True(File.Exists(warningFile));
            Assert.True(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));
            Assert.Contains("MissingPrimaryKey", File.ReadAllText(warningFile), StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_json_fail_on_warnings_outputs_machine_readable_summary()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_json_warn_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_json_warn_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE WarningRow (
                        Status TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --fail-on-warnings --json",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.True(string.IsNullOrWhiteSpace(result.Stderr), result.Stderr);

            using var document = JsonDocument.Parse(result.Stdout);
            var json = document.RootElement;
            var warnings = json.GetProperty("warnings");
            Assert.Equal("failed", json.GetProperty("status").GetString());
            Assert.Contains("Scaffolding produced warnings", json.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.True(warnings.GetProperty("hasDiagnostics").GetBoolean());
            Assert.True(warnings.GetProperty("reportsWritten").GetBoolean());
            Assert.Equal(1, warnings.GetProperty("totalWarnings").GetInt32());
            Assert.Equal(1, warnings.GetProperty("codes").GetProperty("SCF116").GetInt32());
            Assert.Equal(1, warnings.GetProperty("categories").GetProperty("table-shape").GetInt32());
            Assert.True(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.True(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_with_warnings_returns_zero_and_prints_warning_paths()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_warn_ok_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_warn_ok_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE WarningRow (
                        Status TEXT NOT NULL
                    );
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx",
                root);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("Scaffolding completed", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("nORM.ScaffoldWarnings.md", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("nORM.ScaffoldWarnings.json", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("Scaffolding warning summary", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("SCF116=1", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("table-shape=1", result.Stdout, StringComparison.Ordinal);
            Assert.True(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.True(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_dry_run_does_not_write_output_files()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_dry_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_dry_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE DryRunRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --dry-run",
                root);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("Scaffolding dry run completed", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("No files were written", result.Stdout, StringComparison.Ordinal);
            Assert.False(Directory.Exists(output));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_dry_run_prints_warning_summary_without_writing_output_files()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_dry_warn_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_dry_warn_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE WarningRow (Status TEXT NOT NULL DEFAULT 'new');";
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --dry-run",
                root);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("Scaffolding warning summary", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("SCF116=1", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("table-shape=1", result.Stdout, StringComparison.Ordinal);
            Assert.Contains("Scaffolding dry run completed", result.Stdout, StringComparison.Ordinal);
            Assert.False(Directory.Exists(output));
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_no_overwrite_with_stale_warning_report_does_not_print_stale_summary()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_stale_warn_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_stale_warn_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE CleanRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            Directory.CreateDirectory(output);
            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.json"), """{"summary":{"totalWarnings":99},"providerOwnedSchemaFeatures":[]}""");

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --no-overwrite",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("stale scaffold warning report", result.Stderr + result.Stdout, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Scaffolding warning summary", result.Stdout, StringComparison.Ordinal);
            Assert.DoesNotContain("99", result.Stdout, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_clean_run_removes_stale_warning_reports_without_printing_summary()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_clean_stale_warn_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_clean_stale_warn_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE TABLE CleanRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);";
                cmd.ExecuteNonQuery();
            }

            Directory.CreateDirectory(output);
            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.md"), "# stale");
            File.WriteAllText(Path.Combine(output, "nORM.ScaffoldWarnings.json"), """{"summary":{"totalWarnings":99},"providerOwnedSchemaFeatures":[]}""");

            var result = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --force",
                root);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("Scaffolding completed", result.Stdout, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));
            Assert.DoesNotContain("Scaffolding warning summary", result.Stdout, StringComparison.Ordinal);
            Assert.DoesNotContain("totalWarnings", result.Stdout, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_sqlite_output_builds_as_consumer_project()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_compile_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_compile_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    PRAGMA foreign_keys=ON;
                    CREATE TABLE Author (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL,
                        "bad""col\name<&>
                    line" TEXT NOT NULL
                    );
                    CREATE TABLE Book (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Author_Id INTEGER NOT NULL,
                        Title TEXT NOT NULL,
                        CONSTRAINT FK_Book_Author FOREIGN KEY (Author_Id) REFERENCES Author(Id)
                    );
                    CREATE TABLE Label (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL
                    );
                    CREATE TABLE BookLabel (
                        BookId INTEGER NOT NULL,
                        LabelId INTEGER NOT NULL,
                        PRIMARY KEY (BookId, LabelId),
                        CONSTRAINT FK_BookLabel_Book FOREIGN KEY (BookId) REFERENCES Book(Id),
                        CONSTRAINT FK_BookLabel_Label FOREIGN KEY (LabelId) REFERENCES Label(Id)
                    );
                    CREATE TABLE Address (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Line1 TEXT NOT NULL
                    );
                    CREATE TABLE Shipment (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        BillingAddressId INTEGER NOT NULL,
                        ShippingAddressId INTEGER NOT NULL,
                        CONSTRAINT FK_Shipment_BillingAddress FOREIGN KEY (BillingAddressId) REFERENCES Address(Id),
                        CONSTRAINT FK_Shipment_ShippingAddress FOREIGN KEY (ShippingAddressId) REFERENCES Address(Id)
                    );
                    CREATE TABLE "audit.events" (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        "value.part" TEXT NOT NULL
                    );
                    CREATE INDEX IX_Book_Author_Title ON Book(Author_Id, Title);
                    CREATE INDEX "IX_Author_Bad""Col
                    Line" ON Author("bad""col\name<&>
                    line");
                    """;
                cmd.ExecuteNonQuery();
            }

            var scaffold = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
            Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
            File.WriteAllText(Path.Combine(output, "CliScaffolded.csproj"), $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <Nullable>enable</Nullable>
                    <ImplicitUsings>disable</ImplicitUsings>
                  </PropertyGroup>
                  <ItemGroup>
                    <Reference Include="nORM">
                      <HintPath>{{normAssembly}}</HintPath>
                    </Reference>
                  </ItemGroup>
                </Project>
                """, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", output);

            var scaffoldAssembly = Path.Combine(output, "bin", "Release", "net8.0", "CliScaffolded.dll");
            var migrationsDir = Path.Combine(output, "Migrations");
            var migration = RunCli(
                $"migrations add ScaffoldedInitial --provider sqlite --assembly {Quote(scaffoldAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.True(migration.ExitCode == 0,
                $"Migration CLI failed with exit code {migration.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{migration.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{migration.Stderr}");

            var generatedMigration = Directory.EnumerateFiles(migrationsDir, "Migration_*_ScaffoldedInitial.cs").Single();
            var migrationSource = File.ReadAllText(generatedMigration);
            Assert.Contains("CREATE INDEX \\\"IX_Book_Author_Title\\\" ON \\\"Book\\\" (\\\"Author_Id\\\", \\\"Title\\\")", migrationSource, StringComparison.Ordinal);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Scaffold_emit_query_artifacts_generates_read_only_view_and_builds()
    {
        var root = FindRepositoryRoot();
        var dbFile = Path.Combine(Path.GetTempPath(), "norm_scaffold_query_artifact_" + Guid.NewGuid().ToString("N") + ".db");
        var output = Path.Combine(Path.GetTempPath(), "norm_scaffold_query_artifact_out_" + Guid.NewGuid().ToString("N"));

        try
        {
            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Product (
                        Id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT NOT NULL,
                        Price NUMERIC NOT NULL
                    );
                    CREATE VIEW ProductReport AS
                        SELECT Id, Name, Price FROM Product;
                    """;
                cmd.ExecuteNonQuery();
            }

            var scaffold = RunCli(
                $"scaffold --provider sqlite --connection {Quote($"Data Source={dbFile}")} --output {Quote(output)} --namespace CliScaffolded --context CliCtx --table ProductReport --emit-query-artifacts",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var viewCode = File.ReadAllText(Path.Combine(output, "ProductReport.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliCtx.cs"));
            Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"ProductReport\")]", viewCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<ProductReport> ProductReports", contextCode, StringComparison.Ordinal);
            Assert.Contains("SCF116=1", scaffold.Stdout, StringComparison.Ordinal);

            var normAssembly = Path.Combine(root, "src", "bin", "Release", "net8.0", "nORM.dll");
            Assert.True(File.Exists(normAssembly), $"Expected built nORM assembly at {normAssembly}. Run dotnet build nORM.sln -c Release first.");
            File.WriteAllText(Path.Combine(output, "CliScaffolded.csproj"), $$"""
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <Nullable>enable</Nullable>
                    <ImplicitUsings>disable</ImplicitUsings>
                  </PropertyGroup>
                  <ItemGroup>
                    <Reference Include="nORM">
                      <HintPath>{{normAssembly}}</HintPath>
                    </Reference>
                  </ItemGroup>
                </Project>
                """, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try { File.Delete(dbFile); } catch { }
            TryDeleteDirectory(output);
        }
    }

    [Fact]
    public void Migrations_add_generates_compilable_literals_for_special_sql_text()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), WeirdModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add WeirdMigration --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)} --attribute-only",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_WeirdMigration.cs").Single();
            Assert.Contains("\\n", File.ReadAllText(generated), StringComparison.Ordinal);

            RunDotNet("build -c Release --no-restore --nologo", tempRoot);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_without_context_requires_explicit_attribute_only()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_no_context_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), WeirdModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add NeedsExplicitMode --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("No DbContext type was found", result.Stderr, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("--attribute-only", result.Stderr, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_requires_force_for_destructive_column_drop()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_destructive_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), RenameModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            Directory.CreateDirectory(migrationsDir);
            var oldSnapshot = new SchemaSnapshot
            {
                Tables =
                {
                    new TableSchema
                    {
                        Name = "Orders",
                        Columns =
                        {
                            new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsNullable = false, IsPrimaryKey = true },
                            new ColumnSchema { Name = "TotalCost", ClrType = typeof(decimal).FullName!, IsNullable = true }
                        }
                    }
                }
            };
            File.WriteAllText(
                Path.Combine(migrationsDir, "schema.snapshot.json"),
                JsonSerializer.Serialize(oldSnapshot, new JsonSerializerOptions { WriteIndented = true }),
                Encoding.UTF8);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var blocked = RunCli(
                $"migrations add RenameTotal --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)} --attribute-only",
                root);

            Assert.Equal(3, blocked.ExitCode);
            Assert.Contains("Destructive schema changes detected", blocked.Stderr, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("Orders.TotalCost", blocked.Stderr, StringComparison.Ordinal);
            Assert.Empty(Directory.EnumerateFiles(migrationsDir, "Migration_*_RenameTotal.cs"));

            var forced = RunCli(
                $"migrations add RenameTotal --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)} --attribute-only --force",
                root);

            Assert.Equal(0, forced.ExitCode);
            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_RenameTotal.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("WARNING: destructive schema changes", source, StringComparison.Ordinal);
            Assert.Contains("Possible rename candidate", source, StringComparison.Ordinal);
            Assert.Contains("Orders.TotalAmount", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_uses_design_time_factory_for_fluent_model()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_factory_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), DesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add FactoryModel --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.Equal(0, result.ExitCode);
            Assert.Contains("design-time DbContext factory", result.Stdout, StringComparison.OrdinalIgnoreCase);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_FactoryModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("FactoryTable", source, StringComparison.Ordinal);
            Assert.Contains("display_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_redacts_design_time_factory_failure_details()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_factory_secret_" + Guid.NewGuid().ToString("N"));
        const string secret = "FactorySecret123!";
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), FailingDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add FactorySecretFailure --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("Design-time factory", result.Stderr, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("FailingDesignTimeContext", result.Stderr, StringComparison.Ordinal);
            Assert.Contains("Password=[REDACTED]", result.Stderr, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(secret, result.Stdout, StringComparison.Ordinal);
            Assert.DoesNotContain(secret, result.Stderr, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_resolves_project_output_using_configuration_and_passes_environment()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_project_config_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            var projectPath = Path.Combine(tempRoot, "CliModel.csproj");
            File.WriteAllText(projectPath, ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), EnvironmentAwareDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);

            var debugAssembly = Path.Combine(tempRoot, "bin", "Debug", "net8.0", "CliModel.dll");
            Assert.False(File.Exists(debugAssembly), "The test must prove --configuration Release instead of accidentally loading Debug output.");

            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add ReleaseProjectModel --provider sqlite --project {Quote(projectPath)} --configuration Release --target-framework net8.0 --environment Staging --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("design-time DbContext factory", result.Stdout, StringComparison.OrdinalIgnoreCase);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_ReleaseProjectModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("StagingTable", source, StringComparison.Ordinal);
            Assert.Contains("Staging_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_resolves_multitargeted_project_output_using_framework_alias()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_multi_tfm_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            var projectPath = Path.Combine(tempRoot, "CliModel.csproj");
            File.WriteAllText(projectPath, MultiTargetModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), EnvironmentAwareDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build CliModel.csproj -c Release -f net8.0 --nologo", tempRoot);
            Assert.True(File.Exists(Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll")));
            Assert.False(File.Exists(Path.Combine(tempRoot, "bin", "Release", "netstandard2.1", "CliModel.dll")));

            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add MultiTargetProjectModel --provider sqlite --project {Quote(projectPath)} --framework net8.0 --configuration Release --environment MultiTarget --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_MultiTargetProjectModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("MultiTargetTable", source, StringComparison.Ordinal);
            Assert.Contains("MultiTarget_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_uses_startup_project_as_design_time_host()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_startup_host_" + Guid.NewGuid().ToString("N"));
        var targetDir = Path.Combine(tempRoot, "TargetModel");
        var startupDir = Path.Combine(tempRoot, "StartupHost");
        Directory.CreateDirectory(targetDir);
        Directory.CreateDirectory(startupDir);

        try
        {
            var targetProjectPath = Path.Combine(targetDir, "TargetModel.csproj");
            var startupProjectPath = Path.Combine(startupDir, "StartupHost.csproj");
            File.WriteAllText(targetProjectPath, SimpleNet8ProjectXml, Encoding.UTF8);
            File.WriteAllText(startupProjectPath, StartupHostProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(startupDir, "Model.cs"), EnvironmentAwareDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build StartupHost.csproj -c Release --nologo", startupDir);
            Assert.False(File.Exists(Path.Combine(targetDir, "bin", "Release", "net8.0", "TargetModel.dll")));

            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add StartupHostModel --provider sqlite --project {Quote(targetProjectPath)} --startup-project {Quote(startupProjectPath)} --configuration Release --target-framework net8.0 --environment StartupHost --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_StartupHostModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("StartupHostTable", source, StringComparison.Ordinal);
            Assert.Contains("StartupHost_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_resolves_design_time_assembly_dependencies()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_deps_" + Guid.NewGuid().ToString("N"));
        var dependencyDir = Path.Combine(tempRoot, "DependencyLib");
        Directory.CreateDirectory(dependencyDir);

        try
        {
            var dependencyProject = Path.Combine(dependencyDir, "DependencyLib.csproj");
            File.WriteAllText(dependencyProject, DependencyProjectXml, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dependencyDir, "ModelNames.cs"), DependencyModelNamesSource, Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectWithDependencyXml(root, dependencyProject), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), DependencyBackedDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build CliModel.csproj -c Release --nologo", tempRoot);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add DependencyModel --provider sqlite --assembly {Quote(modelAssembly)} --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("design-time DbContext factory", result.Stdout, StringComparison.OrdinalIgnoreCase);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_DependencyModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("DependentTable", source, StringComparison.Ordinal);
            Assert.Contains("dependency_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_uses_explicit_deps_probe_directory_for_design_time_dependencies()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_explicit_deps_" + Guid.NewGuid().ToString("N"));
        var dependencyDir = Path.Combine(tempRoot, "DependencyLib");
        Directory.CreateDirectory(dependencyDir);

        try
        {
            var dependencyProject = Path.Combine(dependencyDir, "DependencyLib.csproj");
            File.WriteAllText(dependencyProject, DependencyProjectXml, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dependencyDir, "ModelNames.cs"), DependencyModelNamesSource, Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectWithDependencyXml(root, dependencyProject), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), DependencyBackedDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build CliModel.csproj -c Release --nologo", tempRoot);

            var outputDir = Path.Combine(tempRoot, "bin", "Release", "net8.0");
            var explicitDepsDir = Path.Combine(tempRoot, "ExplicitDeps");
            Directory.CreateDirectory(explicitDepsDir);

            var dependencyAssembly = Path.Combine(outputDir, "DependencyLib.dll");
            var explicitDependencyDir = Path.Combine(explicitDepsDir, "lib", "net8.0");
            Directory.CreateDirectory(explicitDependencyDir);
            var explicitDependencyAssembly = Path.Combine(explicitDependencyDir, "DependencyLib.dll");
            File.Copy(dependencyAssembly, explicitDependencyAssembly);
            File.Delete(dependencyAssembly);

            var outputDeps = Path.Combine(outputDir, "CliModel.deps.json");
            var explicitDeps = Path.Combine(explicitDepsDir, "CliModel.deps.json");
            var depsJson = File.ReadAllText(outputDeps).Replace("\"DependencyLib.dll\"", "\"lib/net8.0/DependencyLib.dll\"", StringComparison.Ordinal);
            File.WriteAllText(explicitDeps, depsJson, Encoding.UTF8);

            var modelAssembly = Path.Combine(outputDir, "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add ExplicitDepsModel --provider sqlite --assembly {Quote(modelAssembly)} --deps {Quote(explicitDeps)} --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("design-time DbContext factory", result.Stdout, StringComparison.OrdinalIgnoreCase);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_ExplicitDepsModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("DependentTable", source, StringComparison.Ordinal);
            Assert.Contains("dependency_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_uses_runtimeconfig_additional_probing_paths_for_deps_packages()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_runtimeconfig_probe_" + Guid.NewGuid().ToString("N"));
        var dependencyDir = Path.Combine(tempRoot, "DependencyLib");
        Directory.CreateDirectory(dependencyDir);

        try
        {
            var dependencyProject = Path.Combine(dependencyDir, "DependencyLib.csproj");
            File.WriteAllText(dependencyProject, DependencyProjectXml, Encoding.UTF8);
            File.WriteAllText(Path.Combine(dependencyDir, "ModelNames.cs"), DependencyModelNamesSource, Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectWithDependencyXml(root, dependencyProject), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), DependencyBackedDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build CliModel.csproj -c Release --nologo", tempRoot);

            var outputDir = Path.Combine(tempRoot, "bin", "Release", "net8.0");
            var explicitDepsDir = Path.Combine(tempRoot, "ExplicitDeps");
            var packageRoot = Path.Combine(tempRoot, "PackageRoot");
            var packageDependencyDir = Path.Combine(packageRoot, "dependencylib", "1.0.0", "lib", "net8.0");
            Directory.CreateDirectory(explicitDepsDir);
            Directory.CreateDirectory(packageDependencyDir);

            var dependencyAssembly = Path.Combine(outputDir, "DependencyLib.dll");
            File.Copy(dependencyAssembly, Path.Combine(packageDependencyDir, "DependencyLib.dll"));
            File.Delete(dependencyAssembly);

            var outputDeps = Path.Combine(outputDir, "CliModel.deps.json");
            var explicitDeps = Path.Combine(explicitDepsDir, "CliModel.deps.json");
            var depsJson = File.ReadAllText(outputDeps).Replace("\"DependencyLib.dll\"", "\"lib/net8.0/DependencyLib.dll\"", StringComparison.Ordinal);
            File.WriteAllText(explicitDeps, depsJson, Encoding.UTF8);

            var runtimeConfig = Path.Combine(explicitDepsDir, "CliModel.runtimeconfig.json");
            var escapedPackageRoot = packageRoot.Replace("\\", "\\\\", StringComparison.Ordinal);
            File.WriteAllText(runtimeConfig, $$"""
                {
                  "runtimeOptions": {
                    "tfm": "net8.0",
                    "additionalProbingPaths": [
                      "{{escapedPackageRoot}}"
                    ]
                  }
                }
                """, Encoding.UTF8);

            var modelAssembly = Path.Combine(outputDir, "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add RuntimeConfigProbeModel --provider sqlite --assembly {Quote(modelAssembly)} --deps {Quote(explicitDeps)} --runtimeconfig {Quote(runtimeConfig)} --output {Quote(migrationsDir)}",
                root);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
            Assert.Contains("design-time DbContext factory", result.Stdout, StringComparison.OrdinalIgnoreCase);

            var generated = Directory.EnumerateFiles(migrationsDir, "Migration_*_RuntimeConfigProbeModel.cs").Single();
            var source = File.ReadAllText(generated);
            Assert.Contains("DependentTable", source, StringComparison.Ordinal);
            Assert.Contains("dependency_name", source, StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Migrations_add_uses_explicit_deps_native_runtime_assets()
    {
        if (!OperatingSystem.IsWindows())
            return;

        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_cli_native_deps_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "CliModel.csproj"), ModelProjectXml(root), Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Model.cs"), NativeProbeDesignTimeFactoryModelSource, Encoding.UTF8);

            RunDotNet("build CliModel.csproj -c Release --nologo", tempRoot);

            var explicitDepsDir = Path.Combine(tempRoot, "ExplicitDeps");
            var nativeDir = Path.Combine(explicitDepsDir, "runtimes", "win-x64", "native");
            Directory.CreateDirectory(nativeDir);
            File.Copy(
                Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.System), "kernel32.dll"),
                Path.Combine(nativeDir, "NativeProbe.dll"));
            File.WriteAllText(Path.Combine(explicitDepsDir, "CliModel.deps.json"), NativeProbeDepsJson, Encoding.UTF8);

            var modelAssembly = Path.Combine(tempRoot, "bin", "Release", "net8.0", "CliModel.dll");
            var migrationsDir = Path.Combine(tempRoot, "Migrations");
            var result = RunCli(
                $"migrations add NativeDepsModel --provider sqlite --assembly {Quote(modelAssembly)} --deps {Quote(Path.Combine(explicitDepsDir, "CliModel.deps.json"))} --output {Quote(migrationsDir)}",
                root);

            Assert.NotEqual(0, result.ExitCode);
            Assert.Contains("NormMissingNativeEntryPoint", result.Stderr, StringComparison.Ordinal);
            Assert.DoesNotContain("Unable to load DLL 'NativeProbe'", result.Stderr, StringComparison.OrdinalIgnoreCase);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    [Xunit.Trait("Category", TestCategory.AdversarialConcurrency)]
    public void MigrationCodeWriter_generated_source_compiles_and_roundtrips_adversarial_sql()
    {
        var root = FindRepositoryRoot();
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_migration_writer_" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempRoot);

        var preUp = "PRAGMA foreign_keys=OFF; -- \"quoted\" path C:\\temp\\up";
        var up = """"
            CREATE FUNCTION public.norm_test()
            RETURNS trigger
            LANGUAGE plpgsql
            AS $norm$
            BEGIN
                RAISE NOTICE 'quote " and backslash \ and raw delimiter """';
                RETURN NEW;
            END;
            $norm$;
            """";
        var up2 = "CREATE TRIGGER `trg_norm` BEFORE INSERT ON `Odd` FOR EACH ROW SET NEW.`Path` = 'C:\\\\norm\\nline';";
        var down = "DROP FUNCTION IF EXISTS public.norm_test();\r\n-- line after CRLF";
        var postDown = "PRAGMA foreign_keys=ON; -- trailing \u001f control";
        var migrationSql = new MigrationSqlStatements(
            Up: new[] { up, up2 },
            Down: new[] { down },
            PreTransactionUp: new[] { preUp },
            PostTransactionDown: new[] { postDown });

        try
        {
            File.WriteAllText(Path.Combine(tempRoot, "RoundTrip.csproj"), RoundTripProjectXml(root), Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(tempRoot, "Migration_202605220001_Adversarial.cs"),
                MigrationCodeWriter.WriteMigrationSource("Migration_202605220001_Adversarial", 202605220001, "Adversarial", migrationSql),
                Encoding.UTF8);
            File.WriteAllText(Path.Combine(tempRoot, "Program.cs"), RoundTripProgramSource, Encoding.UTF8);

            RunDotNet("build -c Release --nologo", tempRoot);
            var roundTripPath = Path.Combine(tempRoot, "roundtrip.txt");
            var appDll = Path.Combine(tempRoot, "bin", "Release", "net8.0", "RoundTrip.dll");
            var runResult = RunProcess("dotnet", $"{Quote(appDll)} {Quote(roundTripPath)}", tempRoot);
            Assert.True(runResult.ExitCode == 0,
                $"RoundTrip.dll failed with exit code {runResult.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{runResult.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{runResult.Stderr}");

            var actual = File.ReadAllLines(roundTripPath)
                .Select(line => Encoding.UTF8.GetString(Convert.FromBase64String(line)))
                .ToArray();

            Assert.Equal(new[] { preUp, up, up2, down, postDown }, actual);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    private static string ModelProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private static string ModelProjectWithDependencyXml(string root, string dependencyProject)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
                <ProjectReference Include="{{dependencyProject}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private static string MultiTargetModelProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFrameworks>net8.0;netstandard2.1</TargetFrameworks>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup Condition="'$(TargetFramework)' == 'net8.0'">
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private static string StartupHostProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private const string SimpleNet8ProjectXml = """
        <Project Sdk="Microsoft.NET.Sdk">
          <PropertyGroup>
            <TargetFramework>net8.0</TargetFramework>
            <Nullable>enable</Nullable>
          </PropertyGroup>
        </Project>
        """;

    private const string DependencyProjectXml = """
        <Project Sdk="Microsoft.NET.Sdk">
          <PropertyGroup>
            <TargetFramework>net8.0</TargetFramework>
            <Nullable>enable</Nullable>
          </PropertyGroup>
        </Project>
        """;

    private static string RoundTripProjectXml(string root)
    {
        var projectReference = Path.Combine(root, "src", "nORM.csproj");
        return $$"""
            <Project Sdk="Microsoft.NET.Sdk">
              <PropertyGroup>
                <OutputType>Exe</OutputType>
                <TargetFramework>net8.0</TargetFramework>
                <Nullable>enable</Nullable>
                <UseAppHost>false</UseAppHost>
              </PropertyGroup>
              <ItemGroup>
                <ProjectReference Include="{{projectReference}}" />
              </ItemGroup>
            </Project>
            """;
    }

    private const string WeirdModelSource = """
        using System.ComponentModel.DataAnnotations;
        using System.ComponentModel.DataAnnotations.Schema;

        [Table("Odd\"Table\\Line\nBreak")]
        public sealed class WeirdEntity
        {
            [Key]
            public int Id { get; set; }

            [Column("Value\"Column\\Line\nBreak")]
            public string Value { get; set; } = "";
        }
        """;

    private const string RenameModelSource = """
        using System.ComponentModel.DataAnnotations;
        using System.ComponentModel.DataAnnotations.Schema;

        [Table("Orders")]
        public sealed class Order
        {
            [Key]
            public int Id { get; set; }

            public decimal? TotalAmount { get; set; }
        }
        """;

    private const string DesignTimeFactoryModelSource = """
        using System.Data.Common;
        using Microsoft.Data.Sqlite;
        using nORM.Configuration;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class FactoryEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }

        public sealed class FactoryContext(DbConnection connection, DatabaseProvider provider, DbContextOptions? options = null)
            : DbContext(connection, provider, options);

        public sealed class FactoryDesignTimeContext : INormDesignTimeDbContextFactory<FactoryContext>
        {
            public FactoryContext CreateDbContext(string[] args)
            {
                var connection = new SqliteConnection("Data Source=:memory:");
                connection.Open();
                var options = new DbContextOptions
                {
                    OnModelCreating = mb => mb.Entity<FactoryEntity>()
                        .ToTable("FactoryTable")
                        .HasKey(e => e.Id)
                        .Property(e => e.Name)
                        .HasColumnName("display_name")
                };
                return new FactoryContext(connection, new SqliteProvider(), options);
            }
        }
        """;

    private const string EnvironmentAwareDesignTimeFactoryModelSource = """
        using System;
        using System.Data.Common;
        using System.Linq;
        using Microsoft.Data.Sqlite;
        using nORM.Configuration;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class EnvironmentEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }

        public sealed class EnvironmentContext(DbConnection connection, DatabaseProvider provider, DbContextOptions? options = null)
            : DbContext(connection, provider, options);

        public sealed class EnvironmentDesignTimeContext : INormDesignTimeDbContextFactory<EnvironmentContext>
        {
            public EnvironmentContext CreateDbContext(string[] args)
            {
                var environment = args.SkipWhile(arg => arg != "--environment").Skip(1).FirstOrDefault()
                    ?? Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")
                    ?? Environment.GetEnvironmentVariable("DOTNET_ENVIRONMENT")
                    ?? "Missing";

                var connection = new SqliteConnection("Data Source=:memory:");
                connection.Open();
                var options = new DbContextOptions
                {
                    OnModelCreating = mb => mb.Entity<EnvironmentEntity>()
                        .ToTable(environment + "Table")
                        .HasKey(e => e.Id)
                        .Property(e => e.Name)
                        .HasColumnName(environment + "_name")
                };
                return new EnvironmentContext(connection, new SqliteProvider(), options);
            }
        }
        """;

    private const string FailingDesignTimeFactoryModelSource = """
        using System;
        using System.Data.Common;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class FailingEntity
        {
            public int Id { get; set; }
        }

        public sealed class FailingContext(DbConnection connection, DatabaseProvider provider)
            : DbContext(connection, provider);

        public sealed class FailingDesignTimeContext : INormDesignTimeDbContextFactory<FailingContext>
        {
            public FailingContext CreateDbContext(string[] args)
            {
                throw new InvalidOperationException("Could not open Server=localhost;Database=norm;User ID=sa;Password=FactorySecret123!;Encrypt=True;");
            }
        }
        """;

    private const string NativeProbeDesignTimeFactoryModelSource = """
        using System.Data.Common;
        using System.Runtime.InteropServices;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class NativeProbeEntity
        {
            public int Id { get; set; }
        }

        public sealed class NativeProbeContext(DbConnection connection, DatabaseProvider provider)
            : DbContext(connection, provider);

        public sealed class NativeProbeDesignTimeContext : INormDesignTimeDbContextFactory<NativeProbeContext>
        {
            public NativeProbeContext CreateDbContext(string[] args)
            {
                NativeProbe.Missing();
                throw new System.InvalidOperationException("Native probe unexpectedly returned.");
            }
        }

        internal static class NativeProbe
        {
            [DllImport("NativeProbe", EntryPoint = "NormMissingNativeEntryPoint")]
            public static extern int Missing();
        }
        """;

    private const string NativeProbeDepsJson = """
        {
          "runtimeTarget": {
            "name": ".NETCoreApp,Version=v8.0/win-x64",
            "signature": ""
          },
          "targets": {
            ".NETCoreApp,Version=v8.0/win-x64": {
              "NativeProbe/1.0.0": {
                "runtimeTargets": {
                  "runtimes/win-x64/native/NativeProbe.dll": {
                    "rid": "win-x64",
                    "assetType": "native"
                  }
                }
              }
            }
          },
          "libraries": {
            "NativeProbe/1.0.0": {
              "type": "package",
              "serviceable": false,
              "sha512": ""
            }
          }
        }
        """;

    private const string DependencyModelNamesSource = """
        namespace DependencyLib;

        public static class ModelNames
        {
            public const string TableName = "DependentTable";
            public const string NameColumn = "dependency_name";
        }
        """;

    private const string DependencyBackedDesignTimeFactoryModelSource = """
        using System.Data.Common;
        using DependencyLib;
        using Microsoft.Data.Sqlite;
        using nORM.Configuration;
        using nORM.Core;
        using nORM.Migration;
        using nORM.Providers;

        public sealed class DependentEntity
        {
            public int Id { get; set; }
            public string Name { get; set; } = "";
        }

        public sealed class DependentContext(DbConnection connection, DatabaseProvider provider, DbContextOptions? options = null)
            : DbContext(connection, provider, options);

        public sealed class DependentDesignTimeContext : INormDesignTimeDbContextFactory<DependentContext>
        {
            public DependentContext CreateDbContext(string[] args)
            {
                var connection = new SqliteConnection("Data Source=:memory:");
                connection.Open();
                var options = new DbContextOptions
                {
                    OnModelCreating = mb => mb.Entity<DependentEntity>()
                        .ToTable(ModelNames.TableName)
                        .HasKey(e => e.Id)
                        .Property(e => e.Name)
                        .HasColumnName(ModelNames.NameColumn)
                };
                return new DependentContext(connection, new SqliteProvider(), options);
            }
        }
        """;

    private const string RoundTripProgramSource = """
        using System;
        using System.Collections.Generic;
        using System.Data;
        using System.Data.Common;
        using System.IO;
        using System.Linq;
        using System.Text;

        var connection = new RecordingConnection();
        var transaction = new RecordingTransaction(connection);
        var migration = new Migration_202605220001_Adversarial();
        migration.Up(connection, transaction);
        migration.Down(connection, transaction);
        File.WriteAllLines(args[0], connection.Commands.Select(sql => Convert.ToBase64String(Encoding.UTF8.GetBytes(sql))));

        internal sealed class RecordingConnection : DbConnection
        {
            public List<string> Commands { get; } = new();
            public override string ConnectionString { get; set; } = "";
            public override string Database => "Test";
            public override string DataSource => "Test";
            public override string ServerVersion => "1";
            public override ConnectionState State => ConnectionState.Open;
            public override void ChangeDatabase(string databaseName) { }
            public override void Close() { }
            public override void Open() { }
            protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => new RecordingTransaction(this);
            protected override DbCommand CreateDbCommand() => new RecordingCommand(this);
        }

        internal sealed class RecordingTransaction(RecordingConnection connection) : DbTransaction
        {
            public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
            protected override DbConnection DbConnection => connection;
            public override void Commit() { }
            public override void Rollback() { }
        }

        internal sealed class RecordingCommand(RecordingConnection connection) : DbCommand
        {
            private readonly DbParameterCollection _parameters = new RecordingParameterCollection();
            public override string CommandText { get; set; } = "";
            public override int CommandTimeout { get; set; }
            public override CommandType CommandType { get; set; }
            public override bool DesignTimeVisible { get; set; }
            public override UpdateRowSource UpdatedRowSource { get; set; }
            protected override DbConnection DbConnection { get; set; } = connection;
            protected override DbParameterCollection DbParameterCollection => _parameters;
            protected override DbTransaction? DbTransaction { get; set; }
            public override void Cancel() { }
            public override int ExecuteNonQuery() { connection.Commands.Add(CommandText); return 0; }
            public override object? ExecuteScalar() => null;
            public override void Prepare() { }
            protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => throw new NotSupportedException();
        }

        internal sealed class RecordingParameterCollection : DbParameterCollection
        {
            private readonly List<object> _items = new();
            public override int Count => _items.Count;
            public override object SyncRoot => this;
            public override int Add(object value) { _items.Add(value); return _items.Count - 1; }
            public override void AddRange(Array values) { foreach (var value in values) Add(value!); }
            public override void Clear() => _items.Clear();
            public override bool Contains(object value) => _items.Contains(value);
            public override bool Contains(string value) => false;
            public override void CopyTo(Array array, int index) => _items.ToArray().CopyTo(array, index);
            public override System.Collections.IEnumerator GetEnumerator() => _items.GetEnumerator();
            public override int IndexOf(object value) => _items.IndexOf(value);
            public override int IndexOf(string parameterName) => -1;
            public override void Insert(int index, object value) => _items.Insert(index, value);
            public override void Remove(object value) => _items.Remove(value);
            public override void RemoveAt(int index) => _items.RemoveAt(index);
            public override void RemoveAt(string parameterName) { }
            protected override DbParameter GetParameter(int index) => (DbParameter)_items[index];
            protected override DbParameter GetParameter(string parameterName) => throw new IndexOutOfRangeException(parameterName);
            protected override void SetParameter(int index, DbParameter value) => _items[index] = value;
            protected override void SetParameter(string parameterName, DbParameter value) => Add(value);
        }
        """;

    private static CliResult RunCli(string arguments, string workingDirectory)
    {
        var root = FindRepositoryRoot();
        var toolPath = Path.Combine(root, "src", "dotnet-norm", "bin", "Release", "net8.0", "dotnet-norm.dll");
        Assert.True(File.Exists(toolPath), $"CLI tool was not built at {toolPath}.");

        return RunProcess("dotnet", $"{Quote(toolPath)} {arguments}", workingDirectory);
    }

    private static void RunDotNet(string arguments, string workingDirectory)
    {
        var result = RunProcess("dotnet", AddBuildStabilitySwitches(arguments), workingDirectory);
        Assert.True(result.ExitCode == 0,
            $"dotnet {arguments} failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");
    }

    private static string AddBuildStabilitySwitches(string arguments)
    {
        if (!arguments.StartsWith("build", StringComparison.OrdinalIgnoreCase) ||
            arguments.Contains("--disable-build-servers", StringComparison.Ordinal))
        {
            return arguments;
        }

        return arguments + " --disable-build-servers";
    }

    private static CliResult RunProcess(string fileName, string arguments, string workingDirectory)
    {
        var startInfo = new ProcessStartInfo(fileName, arguments)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false
        };

        using var process = Process.Start(startInfo) ?? throw new InvalidOperationException($"Failed to start {fileName}.");
        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();
        if (!process.WaitForExit(ProcessTimeout))
        {
            try
            {
                process.Kill(entireProcessTree: true);
            }
            catch
            {
                // The process may exit between timeout detection and Kill.
            }

            process.WaitForExit();
            var timedOutStdout = stdoutTask.GetAwaiter().GetResult();
            var timedOutStderr = stderrTask.GetAwaiter().GetResult();
            throw new TimeoutException(
                $"{fileName} {arguments} did not exit within {ProcessTimeout.TotalSeconds:N0} seconds.{Environment.NewLine}STDOUT:{Environment.NewLine}{timedOutStdout}{Environment.NewLine}STDERR:{Environment.NewLine}{timedOutStderr}");
        }

        process.WaitForExit();
        var stdout = stdoutTask.GetAwaiter().GetResult();
        var stderr = stderrTask.GetAwaiter().GetResult();
        return new CliResult(process.ExitCode, stdout, stderr);
    }

    private static string Quote(string value) => "\"" + value.Replace("\"", "\\\"", StringComparison.Ordinal) + "\"";

    private static string GetUserSecretsFilePathForTest(string userSecretsId)
    {
        if (OperatingSystem.IsWindows())
        {
            var appData = Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);
            Assert.False(string.IsNullOrWhiteSpace(appData));
            return Path.Combine(appData, "Microsoft", "UserSecrets", userSecretsId, "secrets.json");
        }

        var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
        Assert.False(string.IsNullOrWhiteSpace(home));
        return Path.Combine(home, ".microsoft", "usersecrets", userSecretsId, "secrets.json");
    }

    private static string FindRepositoryRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory != null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "nORM.sln")))
                return directory.FullName;
            directory = directory.Parent;
        }

        throw new DirectoryNotFoundException("Could not locate repository root containing nORM.sln.");
    }

    private static void TryDeleteDirectory(string path)
    {
        try
        {
            if (Directory.Exists(path))
                Directory.Delete(path, recursive: true);
        }
        catch
        {
            // Best-effort cleanup; failed deletion only leaves a temp directory.
        }
    }

    private sealed record CliResult(int ExitCode, string Stdout, string Stderr);
}
