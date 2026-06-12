using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using Xunit;

namespace nORM.Tests;

public partial class CliIntegrationTests
{
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
        Assert.Contains("--target-framework", result.Stdout, StringComparison.Ordinal);
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
}
