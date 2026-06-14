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
    public void Dotnet_norm_scaffold_emits_postgres_expression_index_include_metadata()
    {
        var suffix = IdentifierSuffix();
        var tableName = "CliPostgresExprInclude" + suffix;
        var indexName = "IX_CliPostgresExprInclude_" + suffix;

        RunPostgresExpressionIndexFacetCliTest(
            ProviderKind.Postgres,
            tableName,
            indexName,
            "CliLivePostgresExprIncludeCtx",
            (connection, provider) => SetupPostgresExpressionIncludeIndex(connection, provider, tableName, indexName),
            contextCode =>
            {
                Assert.Contains($"HasExpressionIndex(\"{indexName}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains("includedColumnNames:", contextCode, StringComparison.Ordinal);
                Assert.Contains("\"Score\"", contextCode, StringComparison.Ordinal);
            });
    }

    [Fact]
    public void Dotnet_norm_scaffold_emits_postgres_expression_index_null_semantics()
    {
        var suffix = IdentifierSuffix();
        var tableName = "CliPostgresExprNulls" + suffix;
        var indexName = "UX_CliPostgresExprNulls_" + suffix;

        RunPostgresExpressionIndexFacetCliTest(
            ProviderKind.Postgres,
            tableName,
            indexName,
            "CliLivePostgresExprNullsCtx",
            (connection, provider) => SetupPostgresExpressionNullSemanticsIndex(connection, provider, tableName, indexName),
            contextCode =>
            {
                Assert.Contains($"HasExpressionIndex(\"{indexName}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains("isUnique: true", contextCode, StringComparison.Ordinal);
                Assert.Contains("nullsNotDistinct: true", contextCode, StringComparison.Ordinal);
                Assert.Contains("nullSortOrder: IndexNullSortOrder.First", contextCode, StringComparison.Ordinal);
            });
    }

    private static void RunPostgresExpressionIndexFacetCliTest(
        ProviderKind kind,
        string tableName,
        string indexName,
        string contextName,
        Action<System.Data.Common.DbConnection, DatabaseProvider> setup,
        Action<string> assertContextCode)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_postgres_expression_index_facet_" + suffix);
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
                    CleanupPostgresExpressionIndexFacet(connection, provider, tableName);
                    if (Skip.If(true, $"PostgreSQL expression-index facet is not available on this server: {ex.Message}")) return;
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
            var contextCode = File.ReadAllText(Path.Combine(output, contextName + ".cs"));

            Assert.DoesNotContain(indexName, entityCode, StringComparison.Ordinal);
            assertContextCode(contextCode);
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
                CleanupPostgresExpressionIndexFacet(cleanup, provider, tableName);
            }
            catch
            {
                // Best-effort cleanup; failed cleanup should not hide the original assertion.
            }

            TryDeleteDirectory(output);
        }
    }

    private static void SetupPostgresExpressionIncludeIndex(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName,
        string indexName)
    {
        SetupPostgresExpressionIndexTable(connection, provider, tableName);
        Execute(connection,
            $"CREATE INDEX {provider.Escape(indexName)} ON {provider.Escape(tableName)} (lower({provider.Escape("Name")})) INCLUDE ({provider.Escape("Score")})");
    }

    private static void SetupPostgresExpressionNullSemanticsIndex(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName,
        string indexName)
    {
        SetupPostgresExpressionIndexTable(connection, provider, tableName);
        Execute(connection,
            $"CREATE UNIQUE INDEX {provider.Escape(indexName)} ON {provider.Escape(tableName)} (lower({provider.Escape("Name")}) NULLS FIRST) NULLS NOT DISTINCT");
    }

    private static void SetupPostgresExpressionIndexTable(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        CleanupPostgresExpressionIndexFacet(connection, provider, tableName);

        var table = provider.Escape(tableName);
        Execute(connection,
            $"CREATE TABLE {table} ({provider.Escape("Id")} int NOT NULL PRIMARY KEY, {provider.Escape("Name")} varchar(160) NULL, {provider.Escape("Score")} int NOT NULL)");
    }

    private static void CleanupPostgresExpressionIndexFacet(
        System.Data.Common.DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }
}
