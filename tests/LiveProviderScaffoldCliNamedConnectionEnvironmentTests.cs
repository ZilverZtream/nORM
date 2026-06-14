#nullable enable

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    // Live CLI named-connection environment override scaffold tests.

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

}
