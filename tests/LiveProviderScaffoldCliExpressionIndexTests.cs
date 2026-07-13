#nullable enable

using System;
using System.Data.Common;
using System.IO;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldCliParityTests
{
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Dotnet_norm_scaffold_emits_expression_index_metadata_on_live_provider(ProviderKind kind)
    {
        var root = FindRepositoryRoot();
        var suffix = IdentifierSuffix();
        var tableName = "CliExpressionIndex" + suffix;
        var expressionIndex = "IX_CliExpressionIndex_Expression_" + suffix;
        var partialExpressionIndex = "IX_CliExpressionIndex_Partial_" + suffix;
        var output = Path.Combine(Path.GetTempPath(), "norm_live_cli_expression_index_" + kind + "_" + suffix);
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
                    SetupExpressionIndexMetadata(connection, provider, kind, tableName, expressionIndex, partialExpressionIndex);
                }
                catch (DbException ex) when (kind == ProviderKind.MySql)
                {
                    CleanupExpressionIndexMetadata(connection, provider, tableName);
                    if (Skip.If(true, $"MySQL expression indexes are not available on this server: {ex.Message}")) return;
                }
            }

            var scaffold = RunCli(
                "scaffold " +
                $"--provider {cliProvider} " +
                $"--connection {Quote(connectionString)} " +
                $"--output {Quote(output)} " +
                "--namespace CliLiveScaffolded " +
                "--context CliLiveExpressionIndexCtx " +
                $"--table {Quote(tableName)}",
                root);

            Assert.True(scaffold.ExitCode == 0,
                $"CLI failed with exit code {scaffold.ExitCode}.{Environment.NewLine}STDOUT:{Environment.NewLine}{scaffold.Stdout}{Environment.NewLine}STDERR:{Environment.NewLine}{scaffold.Stderr}");

            var entityCode = File.ReadAllText(Path.Combine(output, tableName + ".cs"));
            var contextCode = File.ReadAllText(Path.Combine(output, "CliLiveExpressionIndexCtx.cs"));

            Assert.DoesNotContain(expressionIndex, entityCode, StringComparison.Ordinal);
            Assert.Contains($"HasExpressionIndex(\"{expressionIndex}\"", contextCode, StringComparison.Ordinal);
            Assert.Contains("LOWER", contextCode, StringComparison.OrdinalIgnoreCase);

            if (kind is ProviderKind.Sqlite or ProviderKind.Postgres)
            {
                Assert.DoesNotContain(partialExpressionIndex, entityCode, StringComparison.Ordinal);
                Assert.Contains($"HasExpressionIndex(\"{partialExpressionIndex}\"", contextCode, StringComparison.Ordinal);
                Assert.Contains("filterSql:", contextCode, StringComparison.Ordinal);
            }
            else
            {
                Assert.DoesNotContain(partialExpressionIndex, contextCode, StringComparison.Ordinal);
                Assert.Contains(provider.Escape("Score"), contextCode, StringComparison.Ordinal);
            }

            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.md")));
            Assert.False(File.Exists(Path.Combine(output, "nORM.ScaffoldWarnings.json")));

            WriteConsumerProject(root, output);
            ScaffoldCompileVerification.AssertCompiles(output);
        }
        finally
        {
            try
            {
                using var cleanup = Reopen(kind, connectionString);
                CleanupExpressionIndexMetadata(cleanup, provider, tableName);
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

    private static void SetupExpressionIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string tableName,
        string expressionIndex,
        string partialExpressionIndex)
    {
        CleanupExpressionIndexMetadata(connection, provider, tableName);

        var table = provider.Escape(tableName);
        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        var active = provider.Escape("Active");
        var score = provider.Escape("Score");
        var text = kind == ProviderKind.MySql ? "varchar(80)" : "text";
        var idType = kind == ProviderKind.Sqlite ? "INTEGER" : "int";
        var activeType = kind == ProviderKind.Postgres ? "boolean" : "int";
        var activePredicate = kind == ProviderKind.Postgres ? $"{active} = TRUE" : $"{active} = 1";

        Execute(connection,
            $"CREATE TABLE {table} ({id} {idType} NOT NULL PRIMARY KEY, {name} {text} NOT NULL, {active} {activeType} NOT NULL, {score} int NOT NULL)");

        if (kind == ProviderKind.MySql)
        {
            Execute(connection, $"CREATE INDEX {provider.Escape(expressionIndex)} ON {table} ((LOWER({name})), {score})");
            return;
        }

        Execute(connection,
            $"CREATE INDEX {provider.Escape(expressionIndex)} ON {table} (lower({name}))",
            $"CREATE INDEX {provider.Escape(partialExpressionIndex)} ON {table} (lower({name})) WHERE {activePredicate}");
    }

    private static void CleanupExpressionIndexMetadata(
        DbConnection connection,
        DatabaseProvider provider,
        string tableName)
    {
        Execute(connection, $"DROP TABLE IF EXISTS {provider.Escape(tableName)}");
    }
}
