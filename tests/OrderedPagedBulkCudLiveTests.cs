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
/// Live-provider parity for ExecuteUpdate/Delete over ordered/paged queries.
/// The window resolves through a keyed subquery with the ORDER BY + paging
/// clause kept inside a derived table — this pins the provider-specific SQL:
/// the derived-table wrap on MySQL (update-target and LIMIT-in-IN restrictions),
/// OFFSET/FETCH inside a derived table on SQL Server, row-tuple IN for composite
/// keys on SQLite/Postgres/MySQL, and JOIN-based target CUD on SQL Server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class OrderedPagedBulkCudLiveTests
{
    private const string ItemTable = "OpcItem";
    private const string PairTable = "OpcPair";

    [Table(ItemTable)]
    private sealed class OpcItem
    {
        [Key] public int Id { get; set; }
        public int Val { get; set; }
    }

    [Table(PairTable)]
    private sealed class OpcPair
    {
        public int GroupId { get; set; }
        public int ItemId { get; set; }
        public int Score { get; set; }
    }

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";

    private static string DropDdl(ProviderKind kind, string table, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static DbContextOptions Options() => new()
    {
        OnModelCreating = mb => mb.Entity<OpcPair>().HasKey(p => new { p.GroupId, p.ItemId })
    };

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var ei = ctx.Provider.Escape(ItemTable);
        var ep = ctx.Provider.Escape(PairTable);
        await ExecuteAsync(ctx, DropDdl(kind, ItemTable, ei));
        await ExecuteAsync(ctx, DropDdl(kind, PairTable, ep));

        var intT = IntCol(kind);
        await ExecuteAsync(ctx,
            $"CREATE TABLE {ei} ({ctx.Provider.Escape("Id")} {intT} PRIMARY KEY, " +
            $"{ctx.Provider.Escape("Val")} {intT} NOT NULL)");
        await ExecuteAsync(ctx,
            $"CREATE TABLE {ep} ({ctx.Provider.Escape("GroupId")} {intT} NOT NULL, " +
            $"{ctx.Provider.Escape("ItemId")} {intT} NOT NULL, " +
            $"{ctx.Provider.Escape("Score")} {intT} NOT NULL, " +
            $"PRIMARY KEY ({ctx.Provider.Escape("GroupId")}, {ctx.Provider.Escape("ItemId")}))");

        // Val = (Id * 3 % 8) * 10 — distinct and deliberately not aligned with Id.
        await ExecuteAsync(ctx,
            $"INSERT INTO {ei} ({ctx.Provider.Escape("Id")},{ctx.Provider.Escape("Val")}) VALUES " +
            "(1,30),(2,60),(3,10),(4,40),(5,70),(6,20),(7,50),(8,0)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {ep} ({ctx.Provider.Escape("GroupId")},{ctx.Provider.Escape("ItemId")},{ctx.Provider.Escape("Score")}) VALUES " +
            "(1,1,5),(1,2,15),(2,1,25),(2,2,35),(3,1,45)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            await ExecuteAsync(ctx, DropDdl(kind, ItemTable, ctx.Provider.Escape(ItemTable)));
            await ExecuteAsync(ctx, DropDdl(kind, PairTable, ctx.Provider.Escape(PairTable)));
        }
        catch { /* best-effort */ }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ExecuteDelete_after_ordered_Take_deletes_only_the_window_on_live_provider(ProviderKind kind)
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
                var affected = await ctx.Query<OpcItem>().OrderBy(i => i.Val).Take(3).ExecuteDeleteAsync();
                Assert.Equal(3, affected);

                // Smallest three Vals belong to Ids 8 (0), 3 (10), 6 (20).
                var survivors = (await ctx.Query<OpcItem>().ToListAsync()).Select(i => i.Id).OrderBy(i => i).ToList();
                Assert.True(new[] { 1, 2, 4, 5, 7 }.SequenceEqual(survivors),
                    $"got [{string.Join(",", survivors)}]");
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ExecuteUpdate_after_filtered_ordered_Skip_updates_only_the_remainder_on_live_provider(ProviderKind kind)
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
                // Candidates Id 1..6 by Val descending: 5(70), 2(60), 4(40), 1(30), 6(20), 3(10);
                // Skip(4) leaves Ids 6 and 3.
                var affected = await ctx.Query<OpcItem>()
                    .Where(i => i.Id <= 6)
                    .OrderByDescending(i => i.Val)
                    .Skip(4)
                    .ExecuteUpdateAsync(s => s.SetProperty(i => i.Val, -1));
                Assert.Equal(2, affected);

                var items = await ctx.Query<OpcItem>().ToListAsync();
                Assert.All(items.Where(i => i.Id is 3 or 6), i => Assert.Equal(-1, i.Val));
                Assert.All(items.Where(i => i.Id is not (3 or 6)), i => Assert.True(i.Val >= 0));
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ExecuteDelete_composite_pk_after_ordered_Take_deletes_only_the_window_on_live_provider(ProviderKind kind)
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
                var affected = await ctx.Query<OpcPair>().OrderByDescending(p => p.Score).Take(2).ExecuteDeleteAsync();
                Assert.Equal(2, affected);

                // Highest two Scores are (3,1)=45 and (2,2)=35.
                var remaining = (await ctx.Query<OpcPair>().ToListAsync())
                    .Select(p => (p.GroupId, p.ItemId)).OrderBy(k => k).ToList();
                Assert.True(new[] { (1, 1), (1, 2), (2, 1) }.SequenceEqual(remaining),
                    $"got [{string.Join(",", remaining)}]");
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ExecuteUpdate_composite_pk_after_ordered_Take_updates_only_the_window_on_live_provider(ProviderKind kind)
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
                // Lowest two Scores are (1,1)=5 and (1,2)=15.
                var affected = await ctx.Query<OpcPair>().OrderBy(p => p.Score).Take(2)
                    .ExecuteUpdateAsync(s => s.SetProperty(p => p.Score, p => p.Score + 100));
                Assert.Equal(2, affected);

                var pairs = await ctx.Query<OpcPair>().ToListAsync();
                Assert.Equal(105, pairs.Single(p => p.GroupId == 1 && p.ItemId == 1).Score);
                Assert.Equal(115, pairs.Single(p => p.GroupId == 1 && p.ItemId == 2).Score);
                Assert.Equal(25, pairs.Single(p => p.GroupId == 2 && p.ItemId == 1).Score);
                Assert.Equal(35, pairs.Single(p => p.GroupId == 2 && p.ItemId == 2).Score);
                Assert.Equal(45, pairs.Single(p => p.GroupId == 3 && p.ItemId == 1).Score);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
