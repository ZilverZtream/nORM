#nullable enable

using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Providers;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    private const string PluralizerBlogTable = "ScaffoldLivePluralBlogs";
    private const string PluralizerCategoryTable = "ScaffoldLivePluralCategories";
    private const string PluralizerClassTable = "ScaffoldLivePluralClasses";
    private const string PluralizerStatusTable = "ScaffoldLivePluralStatuses";
    private const string PluralizerBlogEntity = "ScaffoldLivePluralBlog";
    private const string PluralizerCategoryEntity = "ScaffoldLivePluralCategory";
    private const string PluralizerClassEntity = "ScaffoldLivePluralClass";
    private const string PluralizerStatusEntity = "ScaffoldLivePluralStatus";

    private static async Task SetupPluralizerTablesAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        await TeardownPluralizerTablesAsync(connection, provider, kind);

        var id = provider.Escape("Id");
        var name = provider.Escape("Name");
        foreach (var tableName in new[]
                 {
                     PluralizerBlogTable,
                     PluralizerCategoryTable,
                     PluralizerClassTable,
                     PluralizerStatusTable
                 })
        {
            await ExecuteAsync(connection,
                $"CREATE TABLE {provider.Escape(tableName)} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        }
    }

    private static async Task TeardownPluralizerTablesAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, PluralizerStatusTable, provider.Escape(PluralizerStatusTable)));
            await ExecuteAsync(connection, DropTable(kind, PluralizerClassTable, provider.Escape(PluralizerClassTable)));
            await ExecuteAsync(connection, DropTable(kind, PluralizerCategoryTable, provider.Escape(PluralizerCategoryTable)));
            await ExecuteAsync(connection, DropTable(kind, PluralizerBlogTable, provider.Escape(PluralizerBlogTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }
}
