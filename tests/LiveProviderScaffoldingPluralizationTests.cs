#nullable enable

using System;
using System.IO;
using System.Threading.Tasks;
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
    public async Task ScaffoldAsync_singularizes_plural_table_names_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPluralizerTablesAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_pluralizer_" + kind + "_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldPluralizerContext",
                    new ScaffoldOptions
                    {
                        Tables = new[]
                        {
                            PluralizerBlogTable,
                            PluralizerCategoryTable,
                            PluralizerClassTable,
                            PluralizerStatusTable
                        },
                        OverwriteFiles = false
                    });

                AssertPluralizedEntity(dir, PluralizerBlogEntity, PluralizerBlogTable);
                AssertPluralizedEntity(dir, PluralizerCategoryEntity, PluralizerCategoryTable);
                AssertPluralizedEntity(dir, PluralizerClassEntity, PluralizerClassTable);
                AssertPluralizedEntity(dir, PluralizerStatusEntity, PluralizerStatusTable);

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldPluralizerContext.cs"));
                Assert.Contains($"public IQueryable<{PluralizerBlogEntity}> {PluralizerBlogTable}", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public IQueryable<{PluralizerCategoryEntity}> {PluralizerCategoryTable}", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public IQueryable<{PluralizerClassEntity}> {PluralizerClassTable}", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public IQueryable<{PluralizerStatusEntity}> {PluralizerStatusTable}", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("Blogses", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("Categorieses", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, PluralizerBlogTable + ".cs")));
                Assert.False(File.Exists(Path.Combine(dir, PluralizerCategoryTable + ".cs")));
                Assert.False(File.Exists(Path.Combine(dir, PluralizerClassTable + ".cs")));
                Assert.False(File.Exists(Path.Combine(dir, PluralizerStatusTable + ".cs")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownPluralizerTablesAsync(connection, provider, kind);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public async Task ScaffoldAsync_preserves_plural_table_names_when_pluralizer_is_disabled_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupPluralizerTablesAsync(connection, provider, kind);
            var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_no_pluralizer_" + kind + "_" + Guid.NewGuid().ToString("N"));
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldNoPluralizerContext",
                    new ScaffoldOptions
                    {
                        Tables = new[]
                        {
                            PluralizerBlogTable,
                            PluralizerCategoryTable
                        },
                        UsePluralizer = false,
                        OverwriteFiles = false
                    });

                AssertPluralizedEntity(dir, PluralizerBlogTable, PluralizerBlogTable);
                AssertPluralizedEntity(dir, PluralizerCategoryTable, PluralizerCategoryTable);

                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldNoPluralizerContext.cs"));
                Assert.Contains($"public IQueryable<{PluralizerBlogTable}> {PluralizerBlogTable}", contextCode, StringComparison.Ordinal);
                Assert.Contains($"public IQueryable<{PluralizerCategoryTable}> {PluralizerCategoryTable}", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"IQueryable<{PluralizerBlogEntity}>", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain($"IQueryable<{PluralizerCategoryEntity}>", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, PluralizerBlogEntity + ".cs")));
                Assert.False(File.Exists(Path.Combine(dir, PluralizerCategoryEntity + ".cs")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownPluralizerTablesAsync(connection, provider, kind);
            }
        }
    }

    private static void AssertPluralizedEntity(string outputDirectory, string entityName, string tableName)
    {
        var entityCode = File.ReadAllText(Path.Combine(outputDirectory, entityName + ".cs"));
        Assert.Contains("public partial class " + entityName, entityCode, StringComparison.Ordinal);
        Assert.Contains($"[Table(\"{tableName}\"", entityCode, StringComparison.Ordinal);
    }
}
