#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_unions_schema_and_table_filters_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        var scratchDatabase = kind == ProviderKind.MySql
            ? "norm_runtime_filter_union_" + Guid.NewGuid().ToString("N")[..8].ToLowerInvariant()
            : null;
        var originalDatabase = connection.Database;
        await using (connection)
        {
            await SetupSchemaAndTableFilterUnionAsync(connection, provider, kind, scratchDatabase);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_filter_union_" + kind + "_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldFilterUnionContext",
                    new ScaffoldOptions
                    {
                        Schemas = new[] { SchemaFilterRequest(kind, scratchDatabase) },
                        Tables = new[] { SchemaFilterExplicitTable },
                        OverwriteFiles = false
                    });

                Assert.True(File.Exists(DefaultScaffoldEntityPath(dir, SchemaFilterSchemaTable)));
                Assert.True(File.Exists(DefaultScaffoldEntityPath(dir, SchemaFilterExplicitTable)));
                if (kind != ProviderKind.MySql)
                    Assert.False(File.Exists(DefaultScaffoldEntityPath(dir, SchemaFilterSkippedTable)));

                var schemaEntityCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, SchemaFilterSchemaTable));
                var explicitEntityCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, SchemaFilterExplicitTable));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldFilterUnionContext.cs"));

                Assert.Contains($"IQueryable<{SchemaFilterSchemaTable}>", contextCode, StringComparison.OrdinalIgnoreCase);
                Assert.Contains($"IQueryable<{SchemaFilterExplicitTable}>", contextCode, StringComparison.OrdinalIgnoreCase);
                if (kind != ProviderKind.MySql)
                    Assert.DoesNotContain(SchemaFilterSkippedTable, contextCode, StringComparison.Ordinal);

                if (kind == ProviderKind.MySql)
                {
                    Assert.DoesNotContain("Schema =", schemaEntityCode, StringComparison.Ordinal);
                    Assert.DoesNotContain("Schema =", explicitEntityCode, StringComparison.Ordinal);
                }
                else
                {
                    Assert.Contains($"[Table(\"{SchemaFilterSchemaTable}\", Schema = \"{SchemaFilterSchemaName}\")]", schemaEntityCode, StringComparison.Ordinal);
                    AssertExpectedExplicitTableSchema(kind, explicitEntityCode);
                }

                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownSchemaAndTableFilterUnionAsync(connection, provider, kind, scratchDatabase, originalDatabase);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_reports_missing_schema_filter_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupMissingSchemaFilterTableAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_missing_schema_" + kind + "_" + Guid.NewGuid().ToString("N"));
            var missingSchema = "missing_runtime_schema_" + Guid.NewGuid().ToString("N")[..8];
            try
            {
                var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
                    DatabaseScaffolder.ScaffoldAsync(
                        connection,
                        provider,
                        dir,
                        "LiveScaffold",
                        "LiveScaffoldMissingSchemaContext",
                        new ScaffoldOptions
                        {
                            Schemas = new[] { missingSchema },
                            OverwriteFiles = false
                        }));

                Assert.Contains("schema filter did not match", ex.Message, StringComparison.OrdinalIgnoreCase);
                Assert.Contains(missingSchema, ex.Message, StringComparison.Ordinal);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownMissingSchemaFilterTableAsync(connection, provider, kind);
            }
        }
    }

    private static void AssertExpectedExplicitTableSchema(ProviderKind kind, string explicitEntityCode)
    {
        switch (kind)
        {
            case ProviderKind.SqlServer:
                Assert.Contains($"[Table(\"{SchemaFilterExplicitTable}\", Schema = \"dbo\")]", explicitEntityCode, StringComparison.Ordinal);
                break;
            case ProviderKind.Postgres:
                Assert.Contains($"[Table(\"{SchemaFilterExplicitTable}\", Schema = \"public\")]", explicitEntityCode, StringComparison.Ordinal);
                break;
            default:
                Assert.DoesNotContain("Schema =", explicitEntityCode, StringComparison.Ordinal);
                break;
        }
    }
}
