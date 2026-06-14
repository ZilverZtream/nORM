using System;
using System.IO;
using System.Linq;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
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
}
