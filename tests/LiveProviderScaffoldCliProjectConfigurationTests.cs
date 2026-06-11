#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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
    public void Dotnet_norm_scaffold_respects_project_namespace_context_dir_and_nullable_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProject" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_project_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "LiveProject.csproj");
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveProjectCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveProjectCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);

            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains("public string Name { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string Notes { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("string? Notes", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("= default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("DbContextOptions options = null", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
    public void Dotnet_norm_scaffold_reads_directory_build_props_project_metadata_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProps" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_props_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "PropsLiveProject.csproj");
        var connectionName = "LiveProps" + suffix;
        var userSecretsId = "norm-live-props-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForLiveTest(userSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
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
                    <Nullable>enable</Nullable>
                    <UserSecretsId>{{userSecretsId}}</UserSecretsId>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            WriteLiveScaffoldProjectWithoutMetadata(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
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
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLivePropsCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLivePropsCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Inherited.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Inherited.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Inherited.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", contextCode, StringComparison.Ordinal);
            Assert.Contains("public string Name { get; set; } = default!;", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string? Notes { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=ProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
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
    public void Dotnet_norm_scaffold_project_metadata_overrides_directory_build_props_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProjectOverrides" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_project_overrides_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "src", "App");
        var projectPath = Path.Combine(projectDir, "ProjectOverridesLiveProject.csproj");
        var connectionName = "LiveProjectOverrides" + suffix;
        var inheritedUserSecretsId = "norm-live-inherited-" + Guid.NewGuid().ToString("N");
        var projectUserSecretsId = "norm-live-project-" + Guid.NewGuid().ToString("N");
        var inheritedUserSecretsFile = GetUserSecretsFilePathForLiveTest(inheritedUserSecretsId);
        var projectUserSecretsFile = GetUserSecretsFilePathForLiveTest(projectUserSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(inheritedUserSecretsFile)!);
            Directory.CreateDirectory(Path.GetDirectoryName(projectUserSecretsFile)!);
            File.WriteAllText(
                Path.Combine(tempRoot, "Directory.Build.props"),
                $$"""
                <Project>
                  <PropertyGroup>
                    <AssemblyName>Inherited.Assembly.Namespace</AssemblyName>
                    <Nullable>enable</Nullable>
                    <UserSecretsId>{{inheritedUserSecretsId}}</UserSecretsId>
                  </PropertyGroup>
                </Project>
                """,
                Encoding.UTF8);
            WriteLiveScaffoldProjectWithAssemblyMetadata(root, projectPath, projectUserSecretsId);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                inheritedUserSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=InheritedUserSecretConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                projectUserSecretsFile,
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
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveProjectOverridesCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveProjectOverridesCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Project.Assembly.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Project.Assembly.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Project.Assembly.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Inherited.Assembly.Namespace", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Inherited.Assembly.Namespace", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains("public string Name { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public string Notes { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("string? Notes", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("= default!;", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=ProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=InheritedUserSecretConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
            TryDeleteDirectory(Path.GetDirectoryName(inheritedUserSecretsFile)!);
            TryDeleteDirectory(Path.GetDirectoryName(projectUserSecretsFile)!);
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
    public void Dotnet_norm_scaffold_accepts_project_directory_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProjectDir" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_project_dir_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "DirectoryLiveProject.csproj");
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--project {Quote(projectDir)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveProjectDirectoryCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveProjectDirectoryCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);

            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
    public void Dotnet_norm_scaffold_accepts_project_and_force_short_aliases_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProjectForce" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_project_force_alias_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "ForceAliasLiveProject.csproj");
        var contextName = "CliLiveProjectForceAliasCtx";
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var output = Path.Combine(projectDir, "Models");
            Directory.CreateDirectory(output);
            File.WriteAllText(Path.Combine(output, tableName + ".cs"), "stale entity", Encoding.UTF8);
            File.WriteAllText(Path.Combine(output, contextName + ".cs"), "stale context", Encoding.UTF8);

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"-p {Quote(projectPath)} " +
                "-o Models " +
                "-f " +
                $"--context {contextName} " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, contextName + ".cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.DoesNotContain("stale entity", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("stale context", contextCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
    public void Dotnet_norm_scaffold_accepts_short_aliases_and_qualified_context_namespace_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveAliasCtx" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_alias_context_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "AliasContextLiveProject.csproj");
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--project {Quote(projectPath)} " +
                "-o Models " +
                "-n Live.Custom.Entities " +
                "-c Ignored.Context.Namespace.CliLiveAliasCtx " +
                "--context-namespace Live.Custom.Contexts " +
                $"-t {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveAliasCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Custom.Entities;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Custom.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Custom.Entities;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliLiveAliasCtx", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Ignored.Context.Namespace", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
    public void Dotnet_norm_scaffold_infers_current_directory_project_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentProject" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_project_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "CurrentLiveProject.csproj");
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentProjectCtx " +
                $"--table {Quote(tableName)}",
                projectDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveCurrentProjectCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
    public void Dotnet_norm_scaffold_inferred_current_directory_project_reads_named_connection_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentNamed" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_named_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "CurrentNamedLiveProject.csproj");
        var connectionName = "LiveCurrentNamed" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
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
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentNamedCtx " +
                $"--table {Quote(tableName)}",
                projectDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveCurrentNamedCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
    public void Dotnet_norm_scaffold_inferred_current_directory_project_user_secrets_override_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentSecrets" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_secrets_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "CurrentSecretsLiveProject.csproj");
        var connectionName = "LiveCurrentSecrets" + suffix;
        var userSecretsId = "norm-live-current-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForLiveTest(userSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            WriteLiveScaffoldProject(root, projectPath, userSecretsId);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=InferredProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
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
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentSecretsCtx " +
                $"--table {Quote(tableName)}",
                projectDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(projectDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(projectDir, "Data", "Contexts", "CliLiveCurrentSecretsCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable disable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=InferredProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(projectDir, "Models", "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
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
    public void Dotnet_norm_scaffold_no_project_reads_named_connection_current_directory_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentDirConfig" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_dir_config_" + kind + "_" + suffix);
        var workDir = Path.Combine(tempRoot, "Work");
        var connectionName = "LiveCurrentDirConfig" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(workDir);
            File.WriteAllText(
                Path.Combine(workDir, "appsettings.json"),
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
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentDirConfigCtx " +
                $"--table {Quote(tableName)}",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(workDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(workDir, "Data", "Contexts", "CliLiveCurrentDirConfigCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Scaffolded;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Scaffolded.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Scaffolded;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, workDir);
            RunDotNet("build -c Release --nologo", workDir);
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
    public void Dotnet_norm_scaffold_no_project_pass_through_environment_selects_current_directory_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveCurrentDirEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_current_dir_env_" + kind + "_" + suffix);
        var workDir = Path.Combine(tempRoot, "Work");
        var connectionName = "LiveCurrentDirEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(workDir);
            File.WriteAllText(
                Path.Combine(workDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=CurrentDirectoryBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(workDir, "appsettings.Production.json"),
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
                "--output-dir Models " +
                "--context-dir Data/Contexts " +
                "--context CliLiveCurrentDirEnvCtx " +
                $"--table {Quote(tableName)} " +
                "-- --environment Production",
                workDir);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(workDir, "Models", tableName + ".cs");
            var contextPath = Path.Combine(workDir, "Data", "Contexts", "CliLiveCurrentDirEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Scaffolded;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Scaffolded.Data.Contexts;", contextCode, StringComparison.Ordinal);
            Assert.Contains("using Scaffolded;", contextCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", entityCode, StringComparison.Ordinal);
            Assert.Contains("#nullable enable", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=CurrentDirectoryBaseConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(workDir, "Models", "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, workDir);
            RunDotNet("build -c Release --nologo", workDir);
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

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_startup_project_named_connection_overrides_target_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupProject" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_project_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupApp.csproj");
        var connectionName = "LiveStartupProject" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            WriteLiveScaffoldProject(root, modelProjectPath);
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
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Startup.Project.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
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
                $"-s {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context CliLiveStartupProjectCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupProjectCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliLiveStartupProjectCtx", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectConnectionString", contextCode, StringComparison.Ordinal);
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
    public void Dotnet_norm_scaffold_startup_project_directory_named_connection_overrides_target_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupDir" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_project_dir_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupDirectoryApp.csproj");
        var connectionName = "LiveStartupDirectory" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            WriteLiveScaffoldProject(root, modelProjectPath);
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
                startupProjectPath,
                """
                <Project Sdk="Microsoft.NET.Sdk">
                  <PropertyGroup>
                    <TargetFramework>net8.0</TargetFramework>
                    <RootNamespace>Startup.Directory.Namespace</RootNamespace>
                  </PropertyGroup>
                </Project>
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
                $"--startup-project {Quote(startupProjectDir)} " +
                "--output-dir Models " +
                "--context CliLiveStartupProjectDirectoryCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupProjectDirectoryCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliLiveStartupProjectDirectoryCtx", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectConnectionString", contextCode, StringComparison.Ordinal);
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
    public void Dotnet_norm_scaffold_startup_project_named_connection_shorthand_overrides_target_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupShort" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_short_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupShortApp.csproj");
        var connectionName = "LiveStartupShort" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            WriteLiveScaffoldProject(root, modelProjectPath);
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
            WriteLiveScaffoldProject(root, startupProjectPath);
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
                $"{Quote("Name=" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"--startup-project {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context CliLiveStartupShortCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupShortCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", entityCode, StringComparison.Ordinal);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectConnectionString", contextCode, StringComparison.Ordinal);
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
    public void Dotnet_norm_scaffold_startup_project_user_secrets_override_target_user_secrets_and_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupSecrets" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_secrets_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupSecretsApp.csproj");
        var connectionName = "LiveStartupSecrets" + suffix;
        var targetUserSecretsId = "norm-live-target-" + Guid.NewGuid().ToString("N");
        var startupUserSecretsId = "norm-live-startup-" + Guid.NewGuid().ToString("N");
        var targetUserSecretsFile = GetUserSecretsFilePathForLiveTest(targetUserSecretsId);
        var startupUserSecretsFile = GetUserSecretsFilePathForLiveTest(startupUserSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(targetUserSecretsFile)!);
            Directory.CreateDirectory(Path.GetDirectoryName(startupUserSecretsFile)!);
            WriteLiveScaffoldProject(root, modelProjectPath, targetUserSecretsId);
            WriteLiveScaffoldProject(root, startupProjectPath, startupUserSecretsId);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=StartupProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                targetUserSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectUserSecretConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupUserSecretsFile,
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
                "--context CliLiveStartupSecretsCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupSecretsCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class CliLiveStartupSecretsCtx", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=StartupProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=TargetProjectUserSecretConnectionString", contextCode, StringComparison.Ordinal);
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
            TryDeleteDirectory(Path.GetDirectoryName(targetUserSecretsFile)!);
            TryDeleteDirectory(Path.GetDirectoryName(startupUserSecretsFile)!);
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
    public void Dotnet_norm_scaffold_omitted_context_uses_named_connection_leaf_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveNamedContext" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_named_context_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedContextLiveProject.csproj");
        const string connectionName = "ReportingContext";
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
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
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "ReportingContext.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains("public partial class ReportingContext", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("ReportingContextContext", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
    public void Dotnet_norm_scaffold_named_connection_user_secrets_override_project_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveUserSecrets" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_user_secrets_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "UserSecretsLiveProject.csproj");
        var connectionName = "LiveUserSecrets" + suffix;
        var userSecretsId = "norm-live-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForLiveTest(userSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            WriteLiveScaffoldProject(root, projectPath, userSecretsId);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
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
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLiveUserSecretsCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveUserSecretsCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=ProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
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
    public void Dotnet_norm_scaffold_named_connection_shorthand_reads_project_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveNamedShort" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_named_short_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedShortLiveProject.csproj");
        var connectionName = "LiveNamedShort" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
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
                $"{Quote("name=" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLiveNamedShortCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveNamedShortCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
    public void Dotnet_norm_scaffold_named_connection_environment_overrides_project_appsettings_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveNamedEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_named_env_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "NamedEnvLiveProject.csproj");
        var connectionName = "LiveNamedEnv" + suffix;
        var environmentKey = "ConnectionStrings__" + connectionName;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ARealScaffoldConnectionString"
                  }
                }
                """,
                Encoding.UTF8);

            using (connection)
            {
                SetupProjectAwareScaffold(connection, provider, kind, tableName);
            }

            Environment.SetEnvironmentVariable(environmentKey, connectionString);
            var scaffold = RunCli(
                "scaffold " +
                $"{Quote("Name=" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLiveNamedEnvCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveNamedEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=ARealScaffoldConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
        }
        finally
        {
            Environment.SetEnvironmentVariable(environmentKey, null);
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
    public void Dotnet_norm_scaffold_named_connection_environment_overrides_project_user_secrets_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveEnvSecret" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_env_secret_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "EnvSecretLiveProject.csproj");
        var connectionName = "LiveEnvSecret" + suffix;
        var environmentKey = "ConnectionStrings__" + connectionName;
        var userSecretsId = "norm-live-env-secret-" + Guid.NewGuid().ToString("N");
        var userSecretsFile = GetUserSecretsFilePathForLiveTest(userSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(userSecretsFile)!);
            WriteLiveScaffoldProject(root, projectPath, userSecretsId);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                userSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProjectUserSecretConnectionString"
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
                $"{Quote("Name=" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLiveEnvSecretCtx " +
                $"--table {Quote(tableName)}",
                root,
                new Dictionary<string, string?>
                {
                    [environmentKey] = connectionString
                });

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveEnvSecretCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=ProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=ProjectUserSecretConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
            TryDeleteDirectory(Path.GetDirectoryName(userSecretsFile)!);
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
    public void Dotnet_norm_scaffold_named_connection_environment_overrides_startup_and_target_user_secrets_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupEnvSecret" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_env_secret_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupEnvSecretApp.csproj");
        var connectionName = "LiveStartupEnvSecret" + suffix;
        var environmentKey = "ConnectionStrings__" + connectionName;
        var targetUserSecretsId = "norm-live-target-env-secret-" + Guid.NewGuid().ToString("N");
        var startupUserSecretsId = "norm-live-startup-env-secret-" + Guid.NewGuid().ToString("N");
        var targetUserSecretsFile = GetUserSecretsFilePathForLiveTest(targetUserSecretsId);
        var startupUserSecretsFile = GetUserSecretsFilePathForLiveTest(startupUserSecretsId);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            Directory.CreateDirectory(Path.GetDirectoryName(targetUserSecretsFile)!);
            Directory.CreateDirectory(Path.GetDirectoryName(startupUserSecretsFile)!);
            WriteLiveScaffoldProject(root, modelProjectPath, targetUserSecretsId);
            WriteLiveScaffoldProject(root, startupProjectPath, startupUserSecretsId);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=StartupProjectAppsettingsConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                targetUserSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectUserSecretConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                startupUserSecretsFile,
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=StartupProjectUserSecretConnectionString"
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
                $"{Quote("Name=" + connectionName)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--project {Quote(modelProjectPath)} " +
                $"--startup-project {Quote(startupProjectPath)} " +
                "--output-dir Models " +
                "--context CliLiveStartupEnvSecretCtx " +
                $"--table {Quote(tableName)}",
                root,
                new Dictionary<string, string?>
                {
                    [environmentKey] = connectionString
                });

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupEnvSecretCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=StartupProjectAppsettingsConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=TargetProjectUserSecretConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=StartupProjectUserSecretConnectionString", contextCode, StringComparison.Ordinal);
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
            TryDeleteDirectory(Path.GetDirectoryName(targetUserSecretsFile)!);
            TryDeleteDirectory(Path.GetDirectoryName(startupUserSecretsFile)!);
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
    public void Dotnet_norm_scaffold_pass_through_environment_selects_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLivePassEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_pass_env_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "PassEnvironmentLiveProject.csproj");
        var connectionName = "LivePassEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ARealScaffoldConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.Production.json"),
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
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLivePassEnvCtx " +
                $"--table {Quote(tableName)} " +
                "-- --environment Production",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLivePassEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=ARealScaffoldConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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
    public void Dotnet_norm_scaffold_startup_project_pass_through_environment_selects_startup_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupPassEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_pass_env_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupPassEnvironmentApp.csproj");
        var connectionName = "LiveStartupPassEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            WriteLiveScaffoldProject(root, modelProjectPath);
            WriteLiveScaffoldProject(root, startupProjectPath);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.Production.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectProductionConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=StartupProjectBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.Production.json"),
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
                "--context CliLiveStartupPassEnvCtx " +
                $"--table {Quote(tableName)} " +
                "-- --environment Production",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupPassEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectBaseConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=TargetProjectProductionConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=StartupProjectBaseConnectionString", contextCode, StringComparison.Ordinal);
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
    public void Dotnet_norm_scaffold_startup_project_dotnet_environment_selects_startup_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveStartupDotnetEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_startup_dotnet_env_" + kind + "_" + suffix);
        var modelProjectDir = Path.Combine(tempRoot, "Model");
        var startupProjectDir = Path.Combine(tempRoot, "Startup");
        var modelProjectPath = Path.Combine(modelProjectDir, "ModelApp.csproj");
        var startupProjectPath = Path.Combine(startupProjectDir, "StartupDotnetEnvironmentApp.csproj");
        var connectionName = "LiveStartupDotnetEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(modelProjectDir);
            Directory.CreateDirectory(startupProjectDir);
            WriteLiveScaffoldProject(root, modelProjectPath);
            WriteLiveScaffoldProject(root, startupProjectPath);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(modelProjectDir, "appsettings.Development.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=TargetProjectDevelopmentConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=StartupProjectBaseConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(startupProjectDir, "appsettings.Development.json"),
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
                "--context CliLiveStartupDotnetEnvCtx " +
                $"--table {Quote(tableName)}",
                root,
                new Dictionary<string, string?>
                {
                    ["ASPNETCORE_ENVIRONMENT"] = null,
                    ["DOTNET_ENVIRONMENT"] = "Development"
                });

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(modelProjectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveStartupDotnetEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=TargetProjectBaseConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=TargetProjectDevelopmentConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=StartupProjectBaseConnectionString", contextCode, StringComparison.Ordinal);
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
    public void Dotnet_norm_scaffold_ambient_environment_selects_appsettings_environment_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveAmbientEnv" + suffix;
        var tempRoot = Path.Combine(Path.GetTempPath(), "norm_live_cli_ambient_env_" + kind + "_" + suffix);
        var projectDir = Path.Combine(tempRoot, "App");
        var projectPath = Path.Combine(projectDir, "AmbientEnvironmentLiveProject.csproj");
        var connectionName = "LiveAmbientEnv" + suffix;
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            Directory.CreateDirectory(projectDir);
            WriteLiveScaffoldProject(root, projectPath);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=BaseScaffoldConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.Production.json"),
                $$"""
                {
                  "ConnectionStrings": {
                    "{{connectionName}}": "Not=ProductionScaffoldConnectionString"
                  }
                }
                """,
                Encoding.UTF8);
            File.WriteAllText(
                Path.Combine(projectDir, "appsettings.Staging.json"),
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
                $"--project {Quote(projectPath)} " +
                "--output-dir Models " +
                "--context CliLiveAmbientEnvCtx " +
                $"--table {Quote(tableName)}",
                root,
                new Dictionary<string, string?>
                {
                    ["ASPNETCORE_ENVIRONMENT"] = "Staging",
                    ["DOTNET_ENVIRONMENT"] = null
                });

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var output = Path.Combine(projectDir, "Models");
            var entityPath = Path.Combine(output, tableName + ".cs");
            var contextPath = Path.Combine(output, "CliLiveAmbientEnvCtx.cs");
            Assert.True(File.Exists(entityPath));
            Assert.True(File.Exists(contextPath));

            var contextCode = File.ReadAllText(contextPath);
            Assert.Contains("namespace Live.Project.Namespace.Models;", contextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{tableName}> {tableName}s", contextCode, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain("Not=BaseScaffoldConnectionString", contextCode, StringComparison.Ordinal);
            Assert.DoesNotContain("Not=ProductionScaffoldConnectionString", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            RunDotNet("build -c Release --nologo", projectDir);
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