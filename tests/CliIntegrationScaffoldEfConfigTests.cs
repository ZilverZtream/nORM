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
                  "targetFramework": "net8.0",
                  "configuration": "Release",
                  "runtime": "win-x64",
                  "msbuildProjectExtensionsPath": "obj/custom",
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
    public void Scaffold_dotnet_ef_config_supplies_output_filter_and_naming_defaults()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_output_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "ConfiguredOutputApp.csproj");
        var dbFile = Path.Combine(tempRoot, "configured-output.db");

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
                  "outputDir": "Generated/Entities",
                  "namespace": "Configured.Entities",
                  "context": "ConfiguredCtx",
                  "contextDir": "Generated/Contexts",
                  "contextNamespace": "Configured.Contexts",
                  "tables": [ "Customer" ],
                  "noPluralize": true,
                  "useDatabaseNames": true,
                  "force": true
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Ignored.Project.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                    CREATE TABLE Ignored (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                    """;
                cmd.ExecuteNonQuery();
            }

            var entityOutput = Path.Combine(projectDir, "Generated", "Entities");
            var contextOutput = Path.Combine(projectDir, "Generated", "Contexts");
            Directory.CreateDirectory(entityOutput);
            Directory.CreateDirectory(contextOutput);
            File.WriteAllText(Path.Combine(entityOutput, "Customer.cs"), "stale entity", Encoding.UTF8);
            File.WriteAllText(Path.Combine(contextOutput, "ConfiguredCtx.cs"), "stale context", Encoding.UTF8);

            var result = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} Microsoft.EntityFrameworkCore.Sqlite",
                workDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(entityOutput, "Customer.cs"));
            var contextCode = File.ReadAllText(Path.Combine(contextOutput, "ConfiguredCtx.cs"));
            Assert.DoesNotContain("stale entity", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("stale context", contextCode, StringComparison.Ordinal);
            Assert.Contains("namespace Configured.Entities;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Configured.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Configured.Entities;", contextCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"Customer\")]", entityCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<Customer> Customer", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(entityOutput, "Ignored.cs")));
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_filter_defaults_do_not_expand_explicit_cli_filters()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_filter_override_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "FilterOverrideApp.csproj");
        var dbFile = Path.Combine(tempRoot, "filter-override.db");

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
                  "outputDir": "Models",
                  "tables": [ "Ignored" ],
                  "force": true
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>FilterOverrideApp</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = """
                    CREATE TABLE Customer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                    CREATE TABLE Ignored (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                    """;
                cmd.ExecuteNonQuery();
            }

            var result = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} Microsoft.EntityFrameworkCore.Sqlite --table Customer",
                workDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityOutput = Path.Combine(projectDir, "Models");
            Assert.True(File.Exists(Path.Combine(entityOutput, "Customer.cs")));
            Assert.False(File.Exists(Path.Combine(entityOutput, "Ignored.cs")));
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_supplies_safety_defaults_with_cli_precedence()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_safety_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "SafetyDefaultsApp.csproj");
        var dbFile = Path.Combine(tempRoot, "safety-defaults.db");

        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            Directory.CreateDirectory(projectDir);
            File.WriteAllText(
                projectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>SafetyDefaultsApp</RootNamespace>
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

            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "project": "src/App",
                  "outputDir": "DryRunModels",
                  "tables": [ "Customer" ],
                  "dryRun": true,
                  "failOnWarnings": true,
                  "emitRoutineStubs": true,
                  "emitSequenceStubs": true,
                  "emitViewEntities": true,
                  "emitQueryArtifacts": true
                }
                """,
                Encoding.UTF8);

            var dryRun = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} Microsoft.EntityFrameworkCore.Sqlite --json",
                workDir);

            Assert.True(dryRun.ExitCode == 0,
                $"CLI dry run failed with exit code {dryRun.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{dryRun.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{dryRun.Stderr}");
            using (var document = JsonDocument.Parse(dryRun.Stdout))
            {
                var json = document.RootElement;
                Assert.Equal("succeeded", json.GetProperty("status").GetString());
                Assert.True(json.GetProperty("dryRun").GetBoolean());
                Assert.Equal(Path.GetFullPath(Path.Combine(projectDir, "DryRunModels")), json.GetProperty("outputDirectory").GetString());
            }
            Assert.False(Directory.Exists(Path.Combine(projectDir, "DryRunModels")));

            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "project": "src/App",
                  "outputDir": "ForceModels",
                  "context": "ForceCtx",
                  "tables": [ "Customer" ],
                  "noOverwrite": true
                }
                """,
                Encoding.UTF8);
            var forceOutput = Path.Combine(projectDir, "ForceModels");
            Directory.CreateDirectory(forceOutput);
            File.WriteAllText(Path.Combine(forceOutput, "Customer.cs"), "stale entity", Encoding.UTF8);
            File.WriteAllText(Path.Combine(forceOutput, "ForceCtx.cs"), "stale context", Encoding.UTF8);

            var force = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} Microsoft.EntityFrameworkCore.Sqlite --force",
                workDir);

            Assert.True(force.ExitCode == 0,
                $"CLI force override failed with exit code {force.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{force.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{force.Stderr}");
            Assert.DoesNotContain("stale entity", File.ReadAllText(Path.Combine(forceOutput, "Customer.cs")), StringComparison.Ordinal);
            Assert.DoesNotContain("stale context", File.ReadAllText(Path.Combine(forceOutput, "ForceCtx.cs")), StringComparison.Ordinal);

            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                """
                {
                  "project": "src/App",
                  "outputDir": "NoOverwriteModels",
                  "context": "NoOverwriteCtx",
                  "tables": [ "Customer" ],
                  "force": true
                }
                """,
                Encoding.UTF8);
            var noOverwriteOutput = Path.Combine(projectDir, "NoOverwriteModels");
            Directory.CreateDirectory(noOverwriteOutput);
            File.WriteAllText(Path.Combine(noOverwriteOutput, "Customer.cs"), "stale entity", Encoding.UTF8);
            File.WriteAllText(Path.Combine(noOverwriteOutput, "NoOverwriteCtx.cs"), "stale context", Encoding.UTF8);

            var noOverwrite = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} Microsoft.EntityFrameworkCore.Sqlite --no-overwrite",
                workDir);

            Assert.NotEqual(0, noOverwrite.ExitCode);
            Assert.Contains("already exists", noOverwrite.Stdout + noOverwrite.Stderr, StringComparison.OrdinalIgnoreCase);
            Assert.Contains("stale entity", File.ReadAllText(Path.Combine(noOverwriteOutput, "Customer.cs")), StringComparison.Ordinal);
            Assert.Contains("stale context", File.ReadAllText(Path.Combine(noOverwriteOutput, "NoOverwriteCtx.cs")), StringComparison.Ordinal);
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_blank_string_property_fails_as_config_error()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_blank_string_" + Guid.NewGuid().ToString("N"));
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
                  "outputDir": " "
                }
                """,
                Encoding.UTF8);

            var result = RunCli(
                $"scaffold {Quote("Data Source=:memory:")} Microsoft.EntityFrameworkCore.Sqlite --json",
                workDir);

            Assert.NotEqual(0, result.ExitCode);
            Assert.True(string.IsNullOrWhiteSpace(result.Stderr), result.Stderr);

            using var document = JsonDocument.Parse(result.Stdout);
            var json = document.RootElement;
            Assert.Equal("failed", json.GetProperty("status").GetString());
            Assert.Contains("EF tool configuration property 'outputDir' must not be blank.", json.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.False(json.GetProperty("warnings").GetProperty("hasDiagnostics").GetBoolean());
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_json_validation_failure_uses_effective_output_and_dry_run()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_json_failure_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var output = Path.Combine(tempRoot, "ConfiguredFailure").Replace('\\', '/');

        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                $$"""
                {
                  "outputDir": "{{output}}",
                  "namespace": "Bad-Name",
                  "dryRun": true
                }
                """,
                Encoding.UTF8);

            var result = RunCli(
                $"scaffold {Quote("Data Source=:memory:")} Microsoft.EntityFrameworkCore.Sqlite --json",
                workDir);

            Assert.NotEqual(0, result.ExitCode);
            Assert.True(string.IsNullOrWhiteSpace(result.Stderr), result.Stderr);

            using var document = JsonDocument.Parse(result.Stdout);
            var json = document.RootElement;
            Assert.Equal("failed", json.GetProperty("status").GetString());
            Assert.True(json.GetProperty("dryRun").GetBoolean());
            Assert.Equal(Path.GetFullPath(output), json.GetProperty("outputDirectory").GetString());
            Assert.Contains("Scaffold --namespace 'Bad-Name' is not a valid C# namespace", json.GetProperty("error").GetString(), StringComparison.Ordinal);
            Assert.False(Directory.Exists(output));
        }
        finally
        {
            TryDeleteDirectory(tempRoot);
        }
    }

    [Fact]
    public void Scaffold_dotnet_ef_config_emit_query_artifacts_emits_sqlite_virtual_table()
    {
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_scaffold_ef_config_query_artifact_" + Guid.NewGuid().ToString("N"));
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var output = Path.Combine(tempRoot, "Generated");
        var dbFile = Path.Combine(tempRoot, "query-artifacts.db");

        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);

            using (var cn = new Microsoft.Data.Sqlite.SqliteConnection($"Data Source={dbFile}"))
            {
                cn.Open();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = "CREATE VIRTUAL TABLE SearchDocs USING fts5(Body);";
                try
                {
                    cmd.ExecuteNonQuery();
                }
                catch (Microsoft.Data.Sqlite.SqliteException ex)
                {
                    if (Skip.If(true, $"SQLite FTS5 virtual tables are not available in this build: {ex.Message}")) return;
                }
            }

            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                $$"""
                {
                  "outputDir": {{JsonSerializer.Serialize(output)}},
                  "namespace": "CliScaffolded",
                  "context": "QueryArtifactConfigCtx",
                  "emitQueryArtifacts": true
                }
                """,
                Encoding.UTF8);

            var result = RunCli(
                $"scaffold {Quote($"Data Source={dbFile}")} Microsoft.EntityFrameworkCore.Sqlite",
                workDir);

            Assert.True(result.ExitCode == 0,
                $"CLI failed with exit code {result.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{result.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{result.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, "SearchDoc.cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "QueryArtifactConfigCtx.cs"));
            using var warningJson = JsonDocument.Parse(File.ReadAllText(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            Assert.Contains("[ReadOnlyEntity]", entityCode, StringComparison.Ordinal);
            Assert.Contains("[Table(\"SearchDocs\")]", entityCode, StringComparison.Ordinal);
            Assert.Contains("IQueryable<SearchDoc> SearchDocs", contextCode, StringComparison.Ordinal);
            Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                item.GetProperty("table").GetString() == "SearchDocs");
            Assert.Contains(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray(), item =>
                item.GetProperty("kind").GetString() == "VirtualTableShadow" &&
                item.GetProperty("metadata").GetProperty("shadowOf").GetString() == "SearchDocs");
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

}
