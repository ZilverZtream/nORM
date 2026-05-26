using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for MinBy and MaxBy terminal operators across all 4 providers.
///
/// Silent-wrongness risks:
///   * MinBy returning same row as MaxBy → ordering direction ignored.
///   * MinBy returning middle value → LIMIT applied before ORDER BY.
///   * Empty-sequence MinBy returning null instead of throwing → semantics wrong.
///
/// Schema: MbLiveRow (Id, IntVal, Name)
///   (1,30,'c'),(2,10,'a'),(3,50,'e'),(4,20,'b'),(5,40,'d')
/// MinBy(IntVal) → Id=2 (val=10); MaxBy(IntVal) → Id=3 (val=50)
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderMinByMaxByParityTests
{
    private const string Table = "MbLiveRow";

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
    private static string VarCol(ProviderKind kind, int len) => kind switch
    {
        ProviderKind.SqlServer => $"NVARCHAR({len})",
        _ => $"VARCHAR({len})"
    };

    private static string DropDdl(ProviderKind kind, string esc) => kind == ProviderKind.SqlServer
        ? $"IF OBJECT_ID(N'{Table}', N'U') IS NOT NULL DROP TABLE {esc};"
        : $"DROP TABLE IF EXISTS {esc};";

    private static async Task ExecuteAsync(DbContext ctx, string sql)
    {
        await using var cmd = ctx.Connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task SetupAsync(DbContext ctx, ProviderKind kind)
    {
        var esc  = ctx.Provider.Escape(Table);
        var eId  = ctx.Provider.Escape("Id");
        var eVal = ctx.Provider.Escape("IntVal");
        var eName = ctx.Provider.Escape("Name");
        var intT = IntCol(kind);
        var varT = VarCol(kind, 5);

        await ExecuteAsync(ctx, DropDdl(kind, esc));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {esc} ({eId} {intT} PRIMARY KEY, {eVal} {intT} NOT NULL, {eName} {varT} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {esc} ({eId},{eVal},{eName}) VALUES " +
            "(1,30,'c'),(2,10,'a'),(3,50,'e'),(4,20,'b'),(5,40,'d')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class MbLiveRow
    {
        [Key] public int    Id     { get; set; }
        public int    IntVal { get; set; }
        public string Name   { get; set; } = "";
    }

    // ── 1: MinBy integer column returns entity with smallest value ────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task MinBy_integer_column_returns_entity_with_smallest_value_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<MbLiveRow>().MinByAsync(r => r.IntVal);
                Assert.Equal(2, row.Id);
                Assert.Equal(10, row.IntVal);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 2: MaxBy integer column returns entity with largest value ─────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task MaxBy_integer_column_returns_entity_with_largest_value_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<MbLiveRow>().MaxByAsync(r => r.IntVal);
                Assert.Equal(3, row.Id);
                Assert.Equal(50, row.IntVal);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 3: MinBy and MaxBy return distinct rows on live provider ──────────────
    // Proves ordering direction is set: if MinBy ≡ MaxBy the direction is wrong.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task MinBy_and_MaxBy_return_distinct_rows_on_live_provider(ProviderKind kind)
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
                var min = await ctx.Query<MbLiveRow>().MinByAsync(r => r.IntVal);
                var max = await ctx.Query<MbLiveRow>().MaxByAsync(r => r.IntVal);
                Assert.NotEqual(min.Id, max.Id);
                Assert.True(min.IntVal < max.IntVal);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 4: MinBy after Where filters first ───────────────────────────────────
    // Among rows where IntVal > 20 → {(1,30,'c'),(3,50,'e'),(5,40,'d')}
    // MinBy(IntVal) → Id=1, IntVal=30

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task MinBy_after_Where_returns_minimum_from_filtered_set_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<MbLiveRow>().Where(r => r.IntVal > 20).MinByAsync(r => r.IntVal);
                Assert.Equal(1, row.Id);
                Assert.Equal(30, row.IntVal);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 5: MaxBy after Where filters first ───────────────────────────────────
    // Among rows where IntVal < 40 → {(1,30),(2,10),(4,20)}
    // MaxBy(IntVal) → Id=1, IntVal=30

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task MaxBy_after_Where_returns_maximum_from_filtered_set_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<MbLiveRow>().Where(r => r.IntVal < 40).MaxByAsync(r => r.IntVal);
                Assert.Equal(1, row.Id);
                Assert.Equal(30, row.IntVal);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 6: MinBy on empty sequence throws ────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task MinBy_on_empty_sequence_throws_InvalidOperationException_on_live_provider(ProviderKind kind)
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
                await Assert.ThrowsAsync<InvalidOperationException>(
                    () => ctx.Query<MbLiveRow>().Where(r => r.IntVal > 9999).MinByAsync(r => r.IntVal));
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 7: MaxBy on empty sequence throws ────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task MaxBy_on_empty_sequence_throws_InvalidOperationException_on_live_provider(ProviderKind kind)
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
                await Assert.ThrowsAsync<InvalidOperationException>(
                    () => ctx.Query<MbLiveRow>().Where(r => r.IntVal > 9999).MaxByAsync(r => r.IntVal));
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 8: MinBy by string column ─────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task MinBy_string_column_returns_lexicographically_first_row_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<MbLiveRow>().MinByAsync(r => r.Name);
                Assert.Equal("a", row.Name);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 9: MaxBy by string column ─────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task MaxBy_string_column_returns_lexicographically_last_row_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<MbLiveRow>().MaxByAsync(r => r.Name);
                Assert.Equal("e", row.Name);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
