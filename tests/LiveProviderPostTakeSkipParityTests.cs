using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for the Post-Take/Skip derived-table-wrap family.
/// Each op group (Where, OrderBy, Count, First/Single/Last/ElementAt, Distinct,
/// Reverse, Any/All, scalar aggregate, GroupBy, Skip) is verified to return the
/// correct windowed result on every configured provider.
///
/// Silent-wrongness proof: the test data is arranged so that a naive translation
/// (no derived-table wrap) would return different rows from the correct result.
/// Passing on SQL Server / Postgres / MySQL proves the derived-table SQL is
/// accepted and semantically correct on real engines.
///
/// Tracked in docs/live-provider-linq-parity.md — flips Post-Take/Skip 🚧 rows
/// to ✅ for SqlServer / Postgres / MySQL.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderPostTakeSkipParityTests
{
    // ── data ──────────────────────────────────────────────────────────────────
    // Table: PtsRow (Id, Val, Cat)
    // Rows : (1,10,'a'), (2,20,'b'), (3,30,'a'), (4,40,'b'), (5,50,'a')
    //
    // Inner window:  OrderBy(Id).Take(3) = rows {1,2,3}, vals {10,20,30}, cats {a,b,a}
    // Naive result:  operations applied to the full table — different rows would come back.

    private const string Table = "PtsRow";

    private static string IntCol(ProviderKind kind) => kind == ProviderKind.Sqlite ? "INTEGER" : "INT";
    private static string VarCol(ProviderKind kind) => kind switch
    {
        ProviderKind.SqlServer => "NVARCHAR(10)",
        _                      => "VARCHAR(10)"
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
        var esc = ctx.Provider.Escape(Table);
        var drop = DropDdl(kind, esc);
        var intT = IntCol(kind);
        var varT = VarCol(kind);
        var eId  = ctx.Provider.Escape("Id");
        var eVal = ctx.Provider.Escape("Val");
        var eCat = ctx.Provider.Escape("Cat");
        await ExecuteAsync(ctx,
            $"{drop} CREATE TABLE {esc} ({eId} {intT} PRIMARY KEY, {eVal} {intT} NOT NULL, {eCat} {varT} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {esc} ({eId},{eVal},{eCat}) VALUES (1,10,'a'),(2,20,'b'),(3,30,'a'),(4,40,'b'),(5,50,'a')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try
        {
            var esc = ctx.Provider.Escape(Table);
            await ExecuteAsync(ctx, DropDdl(kind, esc));
        }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class PtsRow
    {
        [Key] public int Id  { get; set; }
        public int    Val { get; set; }
        public string Cat { get; set; } = "";
    }

    // ── 1: Where after Take ───────────────────────────────────────────────────
    // Window {1,2,3}, Cat='b' inside that window → only row 2.
    // Naive WHERE Cat='b' LIMIT 3 would return rows {2,4} (first 3 b-rows
    // from the unwindowed table) — 2 rows, not 1.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Where_after_Take_filters_inside_the_window_on_live_provider(ProviderKind kind)
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
                var rows = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .Where(r => r.Cat == "b")
                    .OrderBy(r => r.Id)
                    .ToListAsync();

                Assert.Equal(new[] { 2 }, rows.Select(r => r.Id).ToArray());
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 2: OrderBy after Take ────────────────────────────────────────────────
    // Window {1,2,3} with Vals {10,20,30}. Re-sort by Val DESC → IDs {3,2,1}.
    // Naive ORDER BY Val DESC LIMIT 3 would return rows {5,4,3} — wrong rows.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task OrderBy_after_Take_resorts_the_windowed_rows_on_live_provider(ProviderKind kind)
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
                var rows = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .OrderByDescending(r => r.Val)
                    .ToListAsync();

                Assert.Equal(new[] { 3, 2, 1 }, rows.Select(r => r.Id).ToArray());
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 3: Count after Take ──────────────────────────────────────────────────
    // Count of window {1,2,3} where Val >= 20 → 2 rows (rows 2,3).
    // Naive COUNT(*) WHERE Val>=20 → 4 (rows 2,3,4,5) — wrong count.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Count_after_Take_counts_within_the_window_on_live_provider(ProviderKind kind)
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
                var count = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .Where(r => r.Val >= 20)
                    .CountAsync();

                Assert.Equal(2, count);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 4: First after Take ──────────────────────────────────────────────────
    // First from window {1,2,3} sorted by Val DESC → row 3 (Val=30).
    // Naive ORDER BY Val DESC LIMIT 1 → row 5 (Val=50) — wrong row.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task First_after_Take_picks_from_the_window_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .OrderByDescending(r => r.Val)
                    .FirstAsync();

                Assert.Equal(3, row.Id);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 5: Single after Take ─────────────────────────────────────────────────
    // Single row in window {1,2,3} where Cat='b' → row 2.
    // Naive WHERE Cat='b' on full table has 2 rows → Single would throw — wrong.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Single_after_Take_matches_exactly_one_windowed_row_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .SingleAsync(r => r.Cat == "b");

                Assert.Equal(2, row.Id);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 6: ElementAt after Take ──────────────────────────────────────────────
    // ElementAt(2) of window {1,2,3} sorted by Id → row 3.
    // Naive OFFSET 2 LIMIT 1 on full table → row 3 (same here, but verifies syntax).

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ElementAt_after_Take_returns_correct_windowed_position_on_live_provider(ProviderKind kind)
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
                // Window {1,2,3} by Id; ElementAt(1) = second element = row 2.
                var row = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .ElementAtAsync(1);

                Assert.Equal(2, row.Id);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 7: Distinct after Take ───────────────────────────────────────────────
    // Window {1,2,3} has Cat values {a,b,a} → Distinct → {a,b} → Count = 2.
    // Full table has {a,b,a,b,a} → same 2 distinct cats, but row 4 (Cat=b)
    // would be included in a naive scan — the wrap confirms only window rows counted.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Distinct_after_Take_operates_on_windowed_rows_on_live_provider(ProviderKind kind)
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
                var distinctCats = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .Select(r => r.Cat)
                    .Distinct()
                    .ToListAsync();

                Assert.Equal(2, distinctCats.Count);
                Assert.Contains("a", distinctCats);
                Assert.Contains("b", distinctCats);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 8: Reverse after Take ────────────────────────────────────────────────
    // Reverse of window {1,2,3} by Id → IDs {3,2,1}.
    // Naive ORDER BY Id DESC LIMIT 3 → rows {5,4,3} — wrong rows.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Reverse_after_Take_reverses_only_the_windowed_rows_on_live_provider(ProviderKind kind)
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
                var rows = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .Reverse()
                    .ToListAsync();

                Assert.Equal(new[] { 3, 2, 1 }, rows.Select(r => r.Id).ToArray());
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 9: Any after Take ────────────────────────────────────────────────────
    // Any(Val>25) in window {10,20,30} → true (row 3, Val=30).
    // Also: Any(Val>45) in that window → false (Val=50 is outside the window).

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Any_after_Take_tests_predicate_within_the_window_on_live_provider(ProviderKind kind)
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
                // Val=30 is in the window → true.
                var anyAbove25 = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id).Take(3)
                    .AnyAsync(r => r.Val > 25);
                Assert.True(anyAbove25);

                // Val=50 is row 5, outside the Take(3) window → false.
                var anyAbove45 = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id).Take(3)
                    .AnyAsync(r => r.Val > 45);
                Assert.False(anyAbove45);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 10: Sum aggregate after Take ─────────────────────────────────────────
    // Sum(Val) in window {10,20,30} → 60.
    // Naive SUM(Val) on full table → 150 — wrong sum.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Sum_after_Take_aggregates_only_the_windowed_rows_on_live_provider(ProviderKind kind)
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
                var sum = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .SumAsync(r => r.Val);

                Assert.Equal(60, sum);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 11: GroupBy after Take ───────────────────────────────────────────────
    // GroupBy(Cat) on window {a,b,a} → {a:2, b:1}.
    // Naive GroupBy on full table → {a:3, b:2} — wrong counts.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupBy_after_Take_groups_only_the_windowed_rows_on_live_provider(ProviderKind kind)
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
                var groups = (await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .GroupBy(r => r.Cat)
                    .Select(g => new { g.Key, Count = g.Count() })
                    .ToListAsync())
                    .OrderBy(g => g.Key)
                    .ToArray();

                Assert.Equal(2, groups.Length);
                Assert.Equal("a", groups[0].Key); Assert.Equal(2, groups[0].Count);
                Assert.Equal("b", groups[1].Key); Assert.Equal(1, groups[1].Count);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 12: Skip after Take ──────────────────────────────────────────────────
    // Skip(1) on window {1,2,3} → rows {2,3}.
    // Naive OFFSET 1 on full table → rows {2,3,4} — 3 rows, wrong.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Skip_after_Take_skips_within_the_window_on_live_provider(ProviderKind kind)
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
                var rows = await ctx.Query<PtsRow>()
                    .OrderBy(r => r.Id)
                    .Take(3)
                    .Skip(1)
                    .OrderBy(r => r.Id)
                    .ToListAsync();

                Assert.Equal(new[] { 2, 3 }, rows.Select(r => r.Id).ToArray());
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 13: Set op (Union) after Take ────────────────────────────────────────
    // Union window {1,2,3} with itself → still {1,2,3} (UNION deduplicates).
    // Verifies that set ops after Take produce derived-table SQL that real engines accept.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Union_after_Take_produces_derived_table_SQL_accepted_by_live_provider(ProviderKind kind)
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
                var window = ctx.Query<PtsRow>().OrderBy(r => r.Id).Take(3);
                var rows = await window
                    .Union(window)
                    .OrderBy(r => r.Id)
                    .ToListAsync();

                // UNION deduplicates — still exactly {1,2,3}.
                Assert.Equal(new[] { 1, 2, 3 }, rows.Select(r => r.Id).ToArray());
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
