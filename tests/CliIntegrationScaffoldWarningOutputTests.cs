using System;
using System.IO;
using System.Text.Json;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
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
}
