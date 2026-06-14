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
}
