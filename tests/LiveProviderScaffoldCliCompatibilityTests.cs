#nullable enable

using System;
using System.IO;
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
    public void Dotnet_norm_scaffold_provider_option_accepts_ef_provider_package_names_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var table = "CliLiveProviderOption" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_provider_option_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            using (connection)
            {
                SetupEfStyleAliasTable(connection, provider, kind, table);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--connection {Quote(connectionString)} " +
                $"--provider {EfProviderPackageName(kind)} " +
                $"--output-dir {Quote(output)} " +
                "-n CliLiveScaffolded " +
                "-c CliLiveProviderOptionCtx " +
                $"--table {Quote(table)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(output, table + ".cs");
            Assert.True(File.Exists(entityPath));
            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveProviderOptionCtx.cs"));
            Assert.Contains($"public partial class {table}", entityCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{table}>", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupEfStyleAliasTable(cleanup, provider, table);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
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
    public void Dotnet_norm_scaffold_honors_connection_provider_option_precedence_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var table = "CliLiveOptionPrecedence" + suffix;
        var explicitOutput = Path.Combine(Path.GetTempPath(), "norm_live_cli_option_precedence_explicit_" + kind + "_" + suffix);
        var providerPositionOutput = Path.Combine(Path.GetTempPath(), "norm_live_cli_option_precedence_provider_position_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupEfStyleAliasTable(connection, provider, kind, table);
            }

            var explicitScaffold = RunCli(
                "scaffold " +
                $"{Quote("Not=PositionalConnectionString")} notaprovider " +
                $"--connection {Quote(connectionString)} " +
                $"--provider {cliProvider} " +
                $"--output-dir {Quote(explicitOutput)} " +
                "-n CliLiveScaffolded " +
                "-c CliLiveExplicitOptionPrecedenceCtx " +
                $"--table {Quote(table)}",
                root);

            Assert.True(explicitScaffold.ExitCode == 0,
                $"CLI failed with exit code {explicitScaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{explicitScaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{explicitScaffold.Stderr}");

            var providerPositionScaffold = RunCli(
                "scaffold " +
                $"--connection {Quote(connectionString)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--output-dir {Quote(providerPositionOutput)} " +
                "-n CliLiveScaffolded " +
                "-c CliLiveProviderPositionCtx " +
                $"--table {Quote(table)}",
                root);

            Assert.True(providerPositionScaffold.ExitCode == 0,
                $"CLI failed with exit code {providerPositionScaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{providerPositionScaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{providerPositionScaffold.Stderr}");

            var explicitEntityPath = Path.Combine(explicitOutput, table + ".cs");
            var providerPositionEntityPath = Path.Combine(providerPositionOutput, table + ".cs");
            Assert.True(File.Exists(explicitEntityPath));
            Assert.True(File.Exists(providerPositionEntityPath));

            var explicitContextCode = File.ReadAllText(Path.Combine(explicitOutput, "CliLiveExplicitOptionPrecedenceCtx.cs"));
            var providerPositionContextCode = File.ReadAllText(Path.Combine(providerPositionOutput, "CliLiveProviderPositionCtx.cs"));
            Assert.Contains($"IQueryable<{table}>", explicitContextCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{table}>", providerPositionContextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(explicitOutput, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(explicitOutput, "nORM.ScaffoldWarnings.json")));
            Assert.False(File.Exists(Path.Combine(providerPositionOutput, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(providerPositionOutput, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, explicitOutput);
            RunDotNet("build -c Release --nologo", explicitOutput);
            WriteConsumerProject(root, providerPositionOutput);
            RunDotNet("build -c Release --nologo", providerPositionOutput);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupEfStyleAliasTable(cleanup, provider, table);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(explicitOutput);
            TryDeleteDirectory(providerPositionOutput);
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
    public void Dotnet_norm_dbcontext_scaffold_accepts_ef_provider_package_names_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var table = "CliLiveEfStyle" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_ef_style_" + kind + "_" + suffix);
        var msbuildProjectExtensionsPath = Path.Combine(Path.GetTempPath(), "norm_live_cli_msbuild_ext_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, _) = live.Value;
        try
        {
            using (connection)
            {
                SetupEfStyleAliasTable(connection, provider, kind, table);
            }

            var scaffold = RunCli(
                "dbcontext scaffold " +
                $"{Quote(connectionString)} " +
                $"{EfProviderPackageName(kind)} " +
                $"--output-dir {Quote(output)} " +
                "-n CliLiveScaffolded " +
                "-c CliLiveEfStyleCtx " +
                "--no-build " +
                "--framework net8.0 " +
                "--configuration Release " +
                "--runtime win-x64 " +
                $"--msbuildprojectextensionspath {Quote(msbuildProjectExtensionsPath)} " +
                "--data-annotations " +
                $"--table {Quote(table)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityPath = Path.Combine(output, table + ".cs");
            Assert.True(File.Exists(entityPath));
            var entityCode = File.ReadAllText(entityPath);
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveEfStyleCtx.cs"));
            Assert.Contains($"public partial class {table}", entityCode, StringComparison.Ordinal);
            Assert.Contains($"IQueryable<{table}>", contextCode, StringComparison.Ordinal);
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupEfStyleAliasTable(cleanup, provider, table);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
            if (sqliteFile is not null)
            {
                try { File.Delete(sqliteFile); } catch { }
            }
        }
    }

}
