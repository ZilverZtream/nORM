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
    [Fact]
    public void Dotnet_norm_scaffold_reports_mysql_prefix_index_without_emitting_normal_index()
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliPrefixIndex" + suffix;
        var prefixIndex = "IX_CliPrefixIndex_Prefix_" + suffix;
        var fullPrefixIndex = "IX_CliPrefixIndex_Full_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_mysql_prefix_index_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(ProviderKind.MySql, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                SetupMySqlPrefixIndexMetadata(connection, provider, tableName, prefixIndex, fullPrefixIndex);
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLivePrefixIndexCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.DoesNotContain(prefixIndex, entityCode, StringComparison.Ordinal);
            Assert.Contains($"[Index(\"{fullPrefixIndex}\")]", entityCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath));

            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();
            var prefix = Assert.Single(providerOwned, item =>
                item.GetProperty("kind").GetString() == "PrefixIndex" &&
                item.GetProperty("name").GetString() == prefixIndex);
            Assert.Equal("SCF117", prefix.GetProperty("code").GetString());
            Assert.Equal("index", prefix.GetProperty("category").GetString());

            var prefixColumn = Assert.Single(prefix.GetProperty("metadata").GetProperty("prefixColumns").EnumerateArray());
            Assert.Equal("Name", prefixColumn.GetProperty("name").GetString());
            Assert.Equal(8, prefixColumn.GetProperty("prefixLength").GetInt32());
            Assert.DoesNotContain(providerOwned, item =>
                item.GetProperty("kind").GetString() == "PrefixIndex" &&
                item.GetProperty("name").GetString() == fullPrefixIndex);

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(ProviderKind.MySql, connectionString);
                CleanupMySqlPrefixIndexMetadata(cleanup, provider, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupMySqlPrefixIndexMetadata(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName,
        string prefixIndex,
        string fullPrefixIndex)
    {
        CleanupMySqlPrefixIndexMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        Execute(connection,
            $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} varchar(80) NOT NULL)",
            $"CREATE INDEX {provider.Escape(prefixIndex)} ON {table} ({name}(8))",
            $"CREATE INDEX {provider.Escape(fullPrefixIndex)} ON {table} ({name}(80))");
    }

    private static void CleanupMySqlPrefixIndexMetadata(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }
}
