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
    // Live CLI project directory and alias scaffold tests.

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

}
