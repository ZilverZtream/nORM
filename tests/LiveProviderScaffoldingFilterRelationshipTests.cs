#nullable enable

using System;
using System.Data.Common;
using System.IO;
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
    public async Task ScaffoldAsync_table_filter_suppresses_unselected_principal_relationship_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var suffix = Guid.NewGuid().ToString("N")[..8];
        var principalTable = "ScaffoldLiveFilterParent" + suffix;
        var dependentTable = "ScaffoldLiveFilterChild" + suffix;
        var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_filter_relationship_" + kind + "_" + suffix);
        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupFilteredRelationshipAsync(connection, provider, kind, principalTable, dependentTable);
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldFilteredRelationshipContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { DefaultSchemaTableFilter(kind, dependentTable) },
                        OverwriteFiles = false
                    });

                var principalEntity = DefaultScaffoldEntityName(principalTable);
                var dependentEntity = DefaultScaffoldEntityName(dependentTable);
                var dependentCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, dependentTable));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldFilteredRelationshipContext.cs"));

                Assert.False(File.Exists(DefaultScaffoldEntityPath(dir, principalTable)));
                Assert.Matches(@"public (?:int|long) ParentId \{ get; set; \}", dependentCode);
                Assert.DoesNotContain("[ForeignKey(", dependentCode, StringComparison.Ordinal);
                Assert.DoesNotContain(principalEntity, dependentCode, StringComparison.Ordinal);
                Assert.Contains($"IQueryable<{dependentEntity}>", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain(principalEntity, contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await CleanupFilteredRelationshipAsync(connection, provider, kind, principalTable, dependentTable);
            }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ScaffoldAsync_table_filter_suppresses_unselected_dependent_relationship_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var suffix = Guid.NewGuid().ToString("N")[..8];
        var principalTable = "ScaffoldLiveFilterParent" + suffix;
        var dependentTable = "ScaffoldLiveFilterChild" + suffix;
        var dir = Path.Combine(Path.GetTempPath(), "live_scaffold_filter_principal_" + kind + "_" + suffix);
        var (connection, provider) = live!.Value;
        await using (connection)
        {
            await SetupFilteredRelationshipAsync(connection, provider, kind, principalTable, dependentTable);
            try
            {
                await DatabaseScaffolder.ScaffoldAsync(
                    connection,
                    provider,
                    dir,
                    "LiveScaffold",
                    "LiveScaffoldFilteredPrincipalContext",
                    new ScaffoldOptions
                    {
                        Tables = new[] { DefaultSchemaTableFilter(kind, principalTable) },
                        OverwriteFiles = false
                    });

                var principalEntity = DefaultScaffoldEntityName(principalTable);
                var dependentEntity = DefaultScaffoldEntityName(dependentTable);
                var principalCode = await File.ReadAllTextAsync(DefaultScaffoldEntityPath(dir, principalTable));
                var contextCode = await File.ReadAllTextAsync(Path.Combine(dir, "LiveScaffoldFilteredPrincipalContext.cs"));

                Assert.False(File.Exists(DefaultScaffoldEntityPath(dir, dependentTable)));
                Assert.Contains($"IQueryable<{principalEntity}>", contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain(dependentEntity, principalCode, StringComparison.Ordinal);
                Assert.DoesNotContain(dependentEntity, contextCode, StringComparison.Ordinal);
                Assert.DoesNotContain("HasForeignKey", contextCode, StringComparison.Ordinal);
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.md")));
                Assert.False(File.Exists(Path.Combine(dir, "nORM.ScaffoldWarnings.json")));
                AssertScaffoldOutputBuilds(dir);
            }
            finally
            {
                if (Directory.Exists(dir))
                    Directory.Delete(dir, recursive: true);
                await CleanupFilteredRelationshipAsync(connection, provider, kind, principalTable, dependentTable);
            }
        }
    }

    private static async Task SetupFilteredRelationshipAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string principalTable,
        string dependentTable)
    {
        await CleanupFilteredRelationshipAsync(connection, provider, kind, principalTable, dependentTable);

        var principal = provider.Escape(principalTable);
        var dependent = provider.Escape(dependentTable);
        var id = provider.Escape("Id");
        var parentId = provider.Escape("ParentId");
        var name = provider.Escape("Name");

        await ExecuteAsync(connection,
            $"CREATE TABLE {principal} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {name} {TextType(kind, 80)} NOT NULL)");
        await ExecuteAsync(connection,
            $"CREATE TABLE {dependent} ({id} {IntType(kind)} NOT NULL PRIMARY KEY, {parentId} {IntType(kind)} NOT NULL, {name} {TextType(kind, 80)} NOT NULL, " +
            $"FOREIGN KEY ({parentId}) REFERENCES {principal} ({id}))");
    }

    private static async Task CleanupFilteredRelationshipAsync(
        DbConnection connection,
        DatabaseProvider provider,
        ProviderKind kind,
        string principalTable,
        string dependentTable)
    {
        try
        {
            await ExecuteAsync(connection, DropTable(kind, dependentTable, provider.Escape(dependentTable)));
            await ExecuteAsync(connection, DropTable(kind, principalTable, provider.Escape(principalTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }
}
