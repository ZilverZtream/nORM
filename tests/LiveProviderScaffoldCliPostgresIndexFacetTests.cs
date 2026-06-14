#nullable enable

using System;
using System.Data.Common;
using System.IO;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Fact]
    public void Dotnet_norm_scaffold_emits_postgres_null_sort_order_index_metadata()
    {
        var suffix = IdentifierSuffix();
        var tableName = "CliPostgresNullSort" + suffix;
        var indexName = "IX_CliPostgresNullSort_" + suffix;

        RunPostgresIndexFacetCliTest(
            ProviderKind.Postgres,
            tableName,
            indexName,
            "CliLivePostgresNullSortCtx",
            (connection, provider) => SetupPostgresNullSortOrderIndex(connection, provider, tableName, indexName),
            entityCode =>
            {
                Assert.Contains($"[Index(\"{indexName}\", NullSortOrder = IndexNullSortOrder.First)]", entityCode, StringComparison.Ordinal);
            });
    }

    [Fact]
    public void Dotnet_norm_scaffold_emits_postgres_nulls_not_distinct_unique_index_metadata()
    {
        var suffix = IdentifierSuffix();
        var tableName = "CliPostgresNullsDistinct" + suffix;
        var indexName = "IX_CliPostgresNullsDistinct_" + suffix;

        RunPostgresIndexFacetCliTest(
            ProviderKind.Postgres,
            tableName,
            indexName,
            "CliLivePostgresNullsDistinctCtx",
            (connection, provider) => SetupPostgresNullsNotDistinctIndex(connection, provider, tableName, indexName),
            entityCode =>
            {
                Assert.Contains($"[Index(\"{indexName}\", IsUnique = true, NullsNotDistinct = true)]", entityCode, StringComparison.Ordinal);
            });
    }

    private static void RunPostgresIndexFacetCliTest(
        ProviderKind kind,
        string tableName,
        string indexName,
        string contextName,
        Action<System.Data.Common.DbConnection, DatabaseProvider> setup,
        Action<string> assertEntityCode)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_postgres_index_facet_" + suffix);
        string? sqliteFile = null;

        var live = OpenLive(kind, ref sqliteFile);
        if (live is null)
            return;

        var (connection, provider, connectionString, cliProvider) = live.Value;
        try
        {
            using (connection)
            {
                try
                {
                    setup(connection, provider);
                }
                catch (DbException ex)
                {
                    CleanupPostgresIndexFacet(connection, provider, tableName);
                    if (Skip.If(true, $"PostgreSQL index facet is not available on this server: {ex.Message}")) return;
                }
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                $"--context {contextName} " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            assertEntityCode(entityCode);
            Assert.DoesNotContain("ProviderSpecificIndex", entityCode, StringComparison.Ordinal);
            Assert.DoesNotContain("ExpressionIndex", entityCode, StringComparison.Ordinal);
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
                CleanupPostgresIndexFacet(cleanup, provider, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupPostgresNullSortOrderIndex(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName,
        string indexName)
    {
        SetupPostgresNullableNameTable(connection, provider, tableName);
        Execute(connection, $"CREATE INDEX {provider.Escape(indexName)} ON {provider.Escape(tableName)} ({provider.Escape("Name")} ASC NULLS FIRST)");
    }

    private static void SetupPostgresNullsNotDistinctIndex(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName,
        string indexName)
    {
        SetupPostgresNullableNameTable(connection, provider, tableName);
        Execute(connection, $"CREATE UNIQUE INDEX {provider.Escape(indexName)} ON {provider.Escape(tableName)} ({provider.Escape("Name")}) NULLS NOT DISTINCT");
    }

    private static void SetupPostgresNullableNameTable(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        CleanupPostgresIndexFacet(connection, provider, tableName);

        var table = provider.Escape(tableName);
        Execute(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} int NOT NULL PRIMARY KEY, {provider.Escape("Name")} varchar(160) NULL)");
    }

    private static void CleanupPostgresIndexFacet(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }
}
