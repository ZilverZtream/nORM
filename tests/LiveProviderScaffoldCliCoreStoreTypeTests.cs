#nullable enable

using System;
using System.IO;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    // Live CLI core store-type scaffold shape tests.

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_maps_temporal_store_types_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliTemporalStoreTypes" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_temporal_store_types_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupTemporalStoreTypes(connection, provider, kind, tableName);
            }

            var tableFilter = kind switch
            {
                ProviderKind.SqlServer => "dbo." + tableName,
                ProviderKind.Postgres => "public." + tableName,
                _ => tableName
            };

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveTemporalStoreTypeCtx " +
                $"--table {Quote(tableFilter)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            Assert.Contains("public DateOnly BusinessDate { get; set; }", entityCode, StringComparison.Ordinal);
            Assert.Contains("public DateTime CreatedAt { get; set; }", entityCode, StringComparison.Ordinal);
            if (kind == ProviderKind.MySql)
            {
                Assert.Contains("public TimeSpan? StartsAt { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("public TimeOnly? StartsAt { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.DoesNotContain("OffsetAt", entityCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.Contains("public TimeOnly? StartsAt { get; set; }", entityCode, StringComparison.Ordinal);
                Assert.Contains("public DateTimeOffset? OffsetAt { get; set; }", entityCode, StringComparison.Ordinal);
            }

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
                CleanupTemporalStoreTypes(cleanup, provider, kind, tableName);
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
