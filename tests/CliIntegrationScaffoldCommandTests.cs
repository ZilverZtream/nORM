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
        Assert.Contains("Compatibility alias for --emit-query-artifacts", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("--emit-query-artifacts", result.Stdout, StringComparison.Ordinal);
        Assert.Contains("Generate bounded read-oriented query artifacts", result.Stdout, StringComparison.Ordinal);

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
