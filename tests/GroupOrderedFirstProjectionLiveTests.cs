using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for greatest-per-group projection members: the correlated
/// single-row subquery (TOP 1 on SQL Server, LIMIT 1 elsewhere; OFFSET/FETCH for
/// ElementAt) must re-apply the query's own Where filters, honor ThenBy chains,
/// flip every key for Last, and offset into the ordered group.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class GroupOrderedFirstProjectionLiveTests
{
    private const string Table = "GofItem";

    [Table(Table)]
    private sealed class GofItem
    {
        [Key] public int Id { get; set; }
        public int Grp { get; set; }
        public int Val { get; set; }
        public int Amount { get; set; }
        public int Live { get; set; }
    }

    private static string DropDdl(ProviderKind kind, string table, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{table}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var et = ctx.Provider.Escape(Table);
        await ExecuteAsync(ctx, DropDdl(kind, Table, et));
        var intT = kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
        await ExecuteAsync(ctx,
            $"CREATE TABLE {et} ({ctx.Provider.Escape("Id")} {intT} PRIMARY KEY, " +
            $"{ctx.Provider.Escape("Grp")} {intT} NOT NULL, {ctx.Provider.Escape("Val")} {intT} NOT NULL, " +
            $"{ctx.Provider.Escape("Amount")} {intT} NOT NULL, {ctx.Provider.Escape("Live")} {intT} NOT NULL)");
        // Group 1: highest Val (80) is on a Live=0 row — a subquery that drops the
        // Where would pick it. Vals stay distinct WITHIN each filtered set: a tie on
        // the First() ordering is legitimately nondeterministic in SQL.
        await ExecuteAsync(ctx,
            $"INSERT INTO {et} ({ctx.Provider.Escape("Id")},{ctx.Provider.Escape("Grp")},{ctx.Provider.Escape("Val")},{ctx.Provider.Escape("Amount")},{ctx.Provider.Escape("Live")}) VALUES " +
            "(1,1,50,10,1),(2,1,80,20,0),(3,1,60,5,1)," +
            "(4,2,30,40,1),(5,2,90,15,0),(6,2,20,25,1)");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, Table, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Filtered_greatest_per_group_matches_linq_on_live_provider(ProviderKind kind)
    {
        var live = LiveProviderFactory.OpenLive(kind);
        if (Skip.If(live is null, $"Live provider {kind} not configured")) return;

        var (connection, provider) = live!.Value;
        await using (connection)
        using (var ctx = new DbContext(connection, provider))
        {
            await SetupAsync(ctx, kind);
            try
            {
                // Source Where re-applies inside the subquery: group 1's top Live row
                // is Id 3 (Val 60), not the Live=0 Id 2 (Val 80).
                var top = (await ctx.Query<GofItem>().Where(x => x.Live == 1).GroupBy(x => x.Grp)
                    .Select(g => new { g.Key, Amt = g.OrderByDescending(x => x.Val).First().Amount })
                    .ToListAsync()).OrderBy(r => r.Key).ToList();
                Assert.Equal(2, top.Count);
                Assert.Equal(5, top[0].Amt);
                Assert.Equal(40, top[1].Amt);

                // ThenBy tiebreak + Last flips every key: ascending Val then descending
                // Amount, Last = highest Val (ties broken by LOWEST Amount).
                var last = (await ctx.Query<GofItem>().GroupBy(x => x.Grp)
                    .Select(g => new { g.Key, Pick = g.OrderBy(x => x.Val).ThenByDescending(x => x.Amount).Last().Id })
                    .ToListAsync()).OrderBy(r => r.Key).ToList();
                Assert.Equal(2, last[0].Pick);
                Assert.Equal(5, last[1].Pick);

                // ElementAt offsets into the ordered group.
                var second = (await ctx.Query<GofItem>().GroupBy(x => x.Grp)
                    .Select(g => new { g.Key, Mid = g.OrderBy(x => x.Amount).ThenBy(x => x.Id).ElementAt(1).Id })
                    .ToListAsync()).OrderBy(r => r.Key).ToList();
                Assert.Equal(1, second[0].Mid);
                Assert.Equal(6, second[1].Mid);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
