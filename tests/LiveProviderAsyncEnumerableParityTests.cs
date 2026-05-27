using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for AsAsyncEnumerable streaming. Covers ordinary query
/// streaming, GroupJoin streaming, and the deterministic Include rejection.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderAsyncEnumerableParityTests
{
    private const string ParentTable = "AsyncEnumLiveParent";
    private const string ChildTable = "AsyncEnumLiveChild";

    [Table(ParentTable)]
    public sealed class AsyncEnumLiveParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<AsyncEnumLiveChild> Children { get; set; } = new();
    }

    [Table(ChildTable)]
    public sealed class AsyncEnumLiveChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = "";
    }

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string VarCol(ProviderKind kind, int len) => kind == ProviderKind.SqlServer
        ? $"NVARCHAR({len})"
        : $"VARCHAR({len})";

    private static string DropTable(ProviderKind kind, string rawName, string escapedName) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{rawName}', N'U') IS NOT NULL DROP TABLE {escapedName}"
        : $"DROP TABLE IF EXISTS {escapedName}";

    private static DbContextOptions Options() => new()
    {
        OnModelCreating = mb =>
        {
            mb.Entity<AsyncEnumLiveParent>().HasKey(p => p.Id);
            mb.Entity<AsyncEnumLiveChild>().HasKey(c => c.Id);
            mb.Entity<AsyncEnumLiveParent>()
                .HasMany(p => p.Children)
                .WithOne()
                .HasForeignKey(c => c.ParentId, p => p.Id);
        }
    };

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var parent = ctx.Provider.Escape(ParentTable);
        var child = ctx.Provider.Escape(ChildTable);
        var id = ctx.Provider.Escape("Id");
        var name = ctx.Provider.Escape("Name");
        var parentId = ctx.Provider.Escape("ParentId");
        var tag = ctx.Provider.Escape("Tag");

        await ExecuteAsync(ctx, DropTable(kind, ChildTable, child));
        await ExecuteAsync(ctx, DropTable(kind, ParentTable, parent));
        await ExecuteAsync(ctx, $"CREATE TABLE {parent} ({id} {IntCol(kind)} PRIMARY KEY, {name} {VarCol(kind, 40)} NOT NULL)");
        await ExecuteAsync(ctx,
            $"CREATE TABLE {child} ({id} {IntCol(kind)} PRIMARY KEY, {parentId} {IntCol(kind)} NOT NULL, " +
            $"{tag} {VarCol(kind, 40)} NOT NULL)");
        await ExecuteAsync(ctx, $"INSERT INTO {parent} ({id},{name}) VALUES (1,'Alice'),(2,'Bob'),(3,'Carol')");
        await ExecuteAsync(ctx,
            $"INSERT INTO {child} ({id},{parentId},{tag}) VALUES " +
            "(1,1,'a1')," +
            "(2,1,'a2')," +
            "(3,2,'b1')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropTable(kind, ChildTable, ctx.Provider.Escape(ChildTable)));
            await ExecuteAsync(ctx, DropTable(kind, ParentTable, ctx.Provider.Escape(ParentTable)));
        }
        catch
        {
            // Best-effort cleanup; test body reports operational failures.
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task AsAsyncEnumerable_streaming_contract_matches_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider, Options()))
        {
            await SetupAsync(ctx, kind);
            try
            {
                var streamedNames = new List<string>();
                await foreach (var row in ctx.Query<AsyncEnumLiveParent>()
                    .Where(p => p.Id >= 2)
                    .OrderBy(p => p.Id)
                    .AsAsyncEnumerable())
                {
                    streamedNames.Add(row.Name);
                }

                Assert.Equal(new[] { "Bob", "Carol" }, streamedNames);

                var groupCounts = new Dictionary<string, int>();
                var query = ctx.Query<AsyncEnumLiveParent>()
                    .GroupJoin(
                        ctx.Query<AsyncEnumLiveChild>(),
                        p => p.Id,
                        c => c.ParentId,
                        (p, cs) => new { p.Name, ChildCount = cs.Count() });

                await foreach (var row in query.AsAsyncEnumerable())
                    groupCounts[row.Name] = row.ChildCount;

                Assert.Equal(2, groupCounts["Alice"]);
                Assert.Equal(1, groupCounts["Bob"]);
                Assert.Equal(0, groupCounts["Carol"]);

                var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(async () =>
                {
                    await foreach (var _ in ((INormQueryable<AsyncEnumLiveParent>)ctx.Query<AsyncEnumLiveParent>())
                        .Include(p => p.Children)
                        .AsAsyncEnumerable())
                    {
                    }
                });
                Assert.Contains("AsAsyncEnumerable does not support Include", ex.Message, StringComparison.Ordinal);
            }
            finally
            {
                await TeardownAsync(ctx, kind);
            }
        }
    }
}
