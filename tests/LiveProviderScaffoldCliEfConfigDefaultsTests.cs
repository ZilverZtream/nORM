#nullable enable

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_dotnet_ef_config_expanded_defaults_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var selectedTable = "CliLiveEfConfig" + suffix;
        var ignoredTable = "CliLiveEfConfigIgnored" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_ef_config_defaults_" + kind + "_" + suffix);
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "EfConfigDefaultsLiveProject.csproj");
        var contextName = "CliLiveEfConfigCtx";
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(configDir);
            Directory.CreateDirectory(workDir);
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(configDir, "dotnet-ef.json"),
                $$"""
                {
                  "project": "src/App",
                  "outputDir": "Generated/Entities",
                  "namespace": "Configured.Live.Entities",
                  "context": "{{contextName}}",
                  "contextDir": "Generated/Contexts",
                  "contextNamespace": "Configured.Live.Contexts",
                  "tables": [ {{JsonSerializer.Serialize(selectedTable)}} ],
                  "noPluralize": true,
                  "useDatabaseNames": true,
                  "force": true
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, selectedTable);
                SetupProjectAwareScaffold(connection, provider, kind, ignoredTable);
            }

            var entityOutput = Path.Combine(projectDir, "Generated", "Entities");
            var contextOutput = Path.Combine(projectDir, "Generated", "Contexts");
            Directory.CreateDirectory(entityOutput);
            Directory.CreateDirectory(contextOutput);
            File.WriteAllText(Path.Combine(entityOutput, selectedTable + ".cs"), "stale entity", Encoding.UTF8);
            File.WriteAllText(Path.Combine(contextOutput, contextName + ".cs"), "stale context", Encoding.UTF8);

            var scaffold = RunCli(
                $"scaffold {Quote(connectionString)} {EfProviderPackageName(kind)}",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(entityOutput, selectedTable + ".cs");
            var contextPath = Path.Combine(contextOutput, contextName + ".cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));
            Assert.False(File.Exists(Path.Combine(entityOutput, ignoredTable + ".cs")));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.DoesNotContain("stale entity", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("stale context", contextCode, StringComparison.Ordinal);
            Assert.Contains("namespace Configured.Live.Entities;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Configured.Live.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Configured.Live.Entities;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"[Table(\"{selectedTable}\"", entityCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{selectedTable}> {selectedTable}", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(entityOutput, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(entityOutput, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, ignoredTable);
                CleanupProjectAwareScaffold(cleanup, provider, kind, selectedTable);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_uses_dotnet_ef_config_startup_named_connection_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveEfConfig" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_ef_config_" + kind + "_" + suffix);
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var modelProjectDir = Path.Combine(tempRoot, "src", "Model");
        var startupProjectDir = Path.Combine(tempRoot, "src", "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupApp.csproj");
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
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
                  "context": "Configured.Live.Contexts.ConfiguredLiveCtx",
                  "framework": "net8.0",
                  "configuration": "Release",
                  "runtime": "win-x64",
                  "verbose": false,
                  "noColor": true,
                  "prefixOutput": false
                }
                """,
                Encoding.UTF8);
            WriteLiveScaffoldProject(root, modelProjectPath);
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
                    "LiveConfigDb": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:LiveConfigDb")} " +
                $"{EfProviderPackageName(kind)} " +
                "--output-dir Models " +
                $"--table {Quote(tableName)}",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "ConfiguredLiveCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Configured.Live.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class ConfiguredLiveCtx", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_cli_options_override_dotnet_ef_config_defaults_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveEfConfigOverride" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_ef_config_override_" + kind + "_" + suffix);
        var configDir = Path.Combine(tempRoot, ".config");
        var workDir = Path.Combine(tempRoot, "Work");
        var modelProjectDir = Path.Combine(tempRoot, "src", "Model");
        var startupProjectDir = Path.Combine(tempRoot, "src", "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupApp.csproj");
        var connectionName = "LiveConfigOverride" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
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
                  "project": "missing/ConfiguredModel",
                  "startupProject": "missing/ConfiguredStartup",
                  "context": "Configured.Should.Not.Win.ConfiguredCtx",
                  "framework": "net6.0",
                  "configuration": "Debug",
                  "runtime": "linux-x64",
                  "verbose": true,
                  "noColor": true,
                  "prefixOutput": true
                }
                """,
                Encoding.UTF8);
            WriteLiveScaffoldProject(root, modelProjectPath);
            File.WriteAllText(
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Explicit.Startup.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": {{JsonSerializer.Serialize(connectionString)}}
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=ConnectionStrings:" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"--startup-project {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context Explicit.Live.Contexts.CliLiveConfigOverrideCtx " +
                $"--table {Quote(tableName)}",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveConfigOverrideCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));
            Assert.False(File.Exists(Path.Combine(output, "ConfiguredCtx.cs")));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Explicit.Live.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliLiveConfigOverrideCtx", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Configured.Should.Not.Win", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=TargetProjectConnectionString", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", modelProjectDir);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupProjectAwareScaffold(cleanup, provider, kind, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(tempRoot);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }
}
