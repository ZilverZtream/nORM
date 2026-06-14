#nullable enable

using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Text.Json;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Fact]
    public void Dotnet_norm_scaffold_reports_postgres_expression_btree_key_options_as_provider_owned()
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliPostgresExprBtreeOptions" + suffix;
        var indexName = "IX_CliPostgresExprBtreeOptions_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_postgres_expression_btree_options_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(ProviderKind.Postgres, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                try
                {
                    SetupPostgresExpressionBtreeKeyOptionsIndex(connection, provider, tableName, indexName);
                }
                catch (DbException ex)
                {
                    CleanupPostgresExpressionBtreeKeyOptionsIndex(connection, provider, tableName);
                    if (Skip.If(true, $"PostgreSQL expression B-tree key options are not available on this server: {ex.Message}")) return;
                }
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLivePostgresExprBtreeOptionsCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLivePostgresExprBtreeOptionsCtx.cs"));
            var warningJsonPath = Path.Combine(output, "nORM.ScaffoldWarnings.json");

            Assert.DoesNotContain(indexName, entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain($"HasExpressionIndex(\"{indexName}\"", contextCode, StringComparison.Ordinal);
            Assert.True(File.Exists(warningJsonPath));

            using var warningJson = JsonDocument.Parse(File.ReadAllText(warningJsonPath));
            var providerOwned = warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray().ToArray();

            var expression = Assert.Single(providerOwned, item =>
                item.GetProperty("kind").GetString() == "ExpressionIndex" &&
                item.GetProperty("name").GetString() == indexName);
            Assert.Equal("SCF112", expression.GetProperty("code").GetString());
            Assert.Equal("index", expression.GetProperty("category").GetString());

            var providerSpecific = Assert.Single(providerOwned, item =>
                item.GetProperty("kind").GetString() == "ProviderSpecificIndex" &&
                item.GetProperty("name").GetString() == indexName);
            Assert.Equal("SCF119", providerSpecific.GetProperty("code").GetString());
            Assert.Equal("index", providerSpecific.GetProperty("category").GetString());
            Assert.Contains("provider-specific key options", providerSpecific.GetProperty("detail").GetString(), StringComparison.OrdinalIgnoreCase);

            var metadata = providerSpecific.GetProperty("metadata");
            Assert.Equal("PostgreSQL", metadata.GetProperty("provider").GetString());
            Assert.Equal("btree", metadata.GetProperty("accessMethod").GetString());
            Assert.True(metadata.GetProperty("hasNonDefaultOperatorClass").GetBoolean());
            Assert.False(metadata.GetProperty("hasNullsNotDistinct").GetBoolean());

            WriteConsumerProject(root, output);
            RunDotNet("build -c Release --nologo", output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(ProviderKind.Postgres, connectionString);
                CleanupPostgresExpressionBtreeKeyOptionsIndex(cleanup, provider, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupPostgresExpressionBtreeKeyOptionsIndex(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName,
        string indexName)
    {
        CleanupPostgresExpressionBtreeKeyOptionsIndex(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        Execute(connection, $"CREATE TABLE {table} ({id} int NOT NULL PRIMARY KEY, {name} varchar(160) NULL)");
        Execute(connection, $"CREATE INDEX {provider.Escape(indexName)} ON {table} (lower({name}) text_pattern_ops)");
    }

    private static void CleanupPostgresExpressionBtreeKeyOptionsIndex(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }
}
