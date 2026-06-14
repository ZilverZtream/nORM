#nullable enable

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    // Live CLI index scaffold diagnostics tests.

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_preserves_index_metadata_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveIndexedOrder" + suffix;
        var nameIndex = "IX_CliLiveIndexedOrder_Name_" + suffix;
        var uniqueIndex = "UX_CliLiveIndexedOrder_Tenant_OrderNo_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_indexes_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupIndexMetadata(connection, provider, kind, tableName, nameIndex, uniqueIndex);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveIndexCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));

            Assert.Contains($"[Index(\"{nameIndex}\")]", entityCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{uniqueIndex}\", IsUnique = true, Order = 0)]", entityCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{uniqueIndex}\", IsUnique = true, Order = 1)]", entityCode, StringComparison.Ordinal);
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
                CleanupIndexMetadata(cleanup, provider, tableName);
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
    public void Dotnet_norm_scaffold_preserves_provider_index_metadata_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliLiveProviderIndex" + suffix;
        var partialIndex = "IX_CliLiveProviderIndex_Partial_" + suffix;
        var includedIndex = "IX_CliLiveProviderIndex_Included_" + suffix;
        var descendingIndex = "IX_CliLiveProviderIndex_Descending_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_provider_indexes_" + kind + "_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupProviderIndexMetadata(connection, provider, kind, tableName, partialIndex, includedIndex, descendingIndex);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveProviderIndexCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            Assert.Contains($"[Index(\"{descendingIndex}\", IsDescending = true)]", entityCode, StringComparison.Ordinal);

            if (kind != ProviderKind.MySql)
                Assert.Contains($"[Index(\"{partialIndex}\", FilterSql = ", entityCode, StringComparison.Ordinal);
            else
                Assert.DoesNotContain(partialIndex, entityCode, StringComparison.Ordinal);

            if (kind is ProviderKind.SqlServer or ProviderKind.Postgres)
            {
                Assert.Contains($"[Index(\"{includedIndex}\")]", entityCode, StringComparison.Ordinal);
                Assert.Contains($"[Index(\"{includedIndex}\", IsIncluded = true)]", entityCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.DoesNotContain(includedIndex, entityCode, StringComparison.Ordinal);
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
                CleanupProviderIndexMetadata(cleanup, provider, tableName);
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
