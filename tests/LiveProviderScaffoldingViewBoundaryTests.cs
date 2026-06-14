#nullable enable

using System;
using System.Data.Common;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using nORM.Providers;
using nORM.Scaffolding;
using Xunit;

namespace nORM.Tests;

public sealed partial class LiveProviderScaffoldingParityTests
{
    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_key_looking_view_columns_stay_read_only_query_artifact_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var suffix = Guid.NewGuid().ToString("N")[..8];
        var baseTable = "ScaffoldLiveViewBoundaryBase" + suffix;
        var viewName = "ScaffoldLiveViewBoundaryReport" + suffix;
        var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_view_boundary_" + kind + "_" + suffix);
        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupKeyLookingViewBoundaryAsync(connection, provider, kind, baseTable, viewName);
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldViewBoundaryContext",
                    new ScaffoldOptions { Tables = new[] { DefaultSchemaTableFilter(kind, viewName) }, OverwriteFiles = false });

                var entityName = DefaultScaffoldEntityName(viewName);
                var viewCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, viewName));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldViewBoundaryContext.cs"));
                using var warningJson = JsonDocument.Parse(await File.ReadAllTextAsync(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));

                AssertKeyLookingViewStayedReadOnly(viewCode, contextCode, entityName);
                Assert.Empty(warningJson.RootElement.GetProperty("skippedDatabaseObjects").EnumerateArray());
                Assert.Contains(warningJson.RootElement.GetProperty("providerOwnedSchemaFeatures").EnumerateArray(), item =>
                    item.GetProperty("kind").GetString() == "MissingPrimaryKey" &&
                    LastTableNameEquals(item.GetProperty("table").GetString(), viewName));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await TeardownKeyLookingViewBoundaryAsync(connection, provider, kind, baseTable, viewName);
            }
        }
    }

    private static async Task SetupKeyLookingViewBoundaryAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseTable,
        string viewName)
    {
        await TeardownKeyLookingViewBoundaryAsync(connection, provider, kind, baseTable, viewName);

        var table = provider.Escape(baseTable);
        var view = provider.Escape(viewName);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");
        var displayName = provider.Escape("DisplayName");

        await ExecuteAsync(connection,
            $"CREATE TABLE {table} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE VIEW {view} AS SELECT {id}, {id} AS {parentId}, {name} AS {displayName} FROM {table}");
    }

    private static async Task TeardownKeyLookingViewBoundaryAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string baseTable,
        string viewName)
    {
        try
        {
            await ExecuteAsync(connection, DropView(kind, viewName, provider.Escape(viewName)));
            await ExecuteAsync(connection, DropTable(kind, baseTable, provider.Escape(baseTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    private static void AssertKeyLookingViewStayedReadOnly(
        string viewCode,
        string contextCode,
        string entityName)
    {
        Assert.Contains("[ReadOnlyEntity]", viewCode, StringComparison.Ordinal);
        Assert.Contains($"public partial class {entityName}", viewCode, StringComparison.Ordinal);
        Assert.Matches(@"public (?:int|long)\?? Id \{ get; set; \}", viewCode);
        Assert.Matches(@"public (?:int|long)\?? ParentId \{ get; set; \}", viewCode);
        Assert.Matches(@"public string\?? DisplayName \{ get; set; \}(?: = default!;)?", viewCode);
        Assert.Contains($"IQueryable<{entityName}>", contextCode, StringComparison.Ordinal);
        Assert.DoesNotContain("[Key]", viewCode, StringComparison.Ordinal);
        Assert.DoesNotContain("[ForeignKey(", viewCode, StringComparison.Ordinal);
        Assert.DoesNotContain("HasKey", contextCode, StringComparison.Ordinal);
        Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
    }
}
