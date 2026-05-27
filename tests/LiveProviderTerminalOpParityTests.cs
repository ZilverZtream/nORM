using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for terminal operators: First, FirstOrDefault, Single,
/// SingleOrDefault, Last, LastOrDefault, ElementAt, Any, All, Count, LongCount.
///
/// The parity matrix rows for these operators are marked ✅ for all providers
/// but cite only <c>TerminalOperatorParityTests</c> (SQLite in-memory). This
/// class closes that gap with real-database execution on all 4 providers.
///
/// Silent-wrongness risks:
///   * Last / LastOrDefault: require ORDER BY flip — a provider that returns
///     first row instead passes silently when the table has only one row.
///   * ElementAt: paged 1-row OFFSET — correct only when the offset semantics
///     match (SqlServer OFFSET … FETCH NEXT 1 ROWS ONLY).
///   * Any with predicate: must push predicate into EXISTS subquery, not
///     client-filter.
///   * All: must generate NOT EXISTS or ALL semantics — easy to flip.
///
/// Schema: TopRow (Id, Val, Cat)
///   (1, 10, 'a'), (2, 20, 'b'), (3, 30, 'a'), (4, 40, 'b'), (5, 50, 'c')
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderTerminalOpParityTests
{
    private const string Table = "TopRow";

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
        var eVal = ctx.Provider.Escape("Val");
        var eCat = ctx.Provider.Escape("Cat");
        var intT = IntCol(kind);
        var varT = VarCol(kind, 5);

        await ExecuteAsync(ctx, DropDdl(kind, esc));
        await ExecuteAsync(ctx,
            $"CREATE TABLE {esc} ({eId} {intT} PRIMARY KEY, {eVal} {intT} NOT NULL, {eCat} {varT} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {esc} ({eId},{eVal},{eCat}) VALUES " +
            "(1,10,'a'),(2,20,'b'),(3,30,'a'),(4,40,'b'),(5,50,'c')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class TopRow
    {
        [Key] public int Id  { get; set; }
        public int    Val { get; set; }
        public string Cat { get; set; } = "";
    }

    // ── 1: First returns first ordered row ───────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task First_ordered_returns_row_with_smallest_val_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<TopRow>().OrderBy(r => r.Val).FirstAsync();
                Assert.Equal(1, row.Id);
                Assert.Equal(10, row.Val);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 2: FirstOrDefault returns null on empty ───────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task FirstOrDefault_returns_null_for_no_match_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<TopRow>().Where(r => r.Val > 9999).FirstOrDefaultAsync();
                Assert.Null(row);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 3: Last returns last ordered row ──────────────────────────────────────
    //
    // Silent-wrongness: if Last uses unflipped ORDER BY it returns the same row as First.
    // Test data: OrderBy(Val) → last row is Id=5, Val=50.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Last_ordered_returns_row_with_largest_val_on_live_provider(ProviderKind kind)
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
                // Last must flip ORDER BY to DESC to pick the final row efficiently.
                var row = await ctx.Query<TopRow>().OrderBy(r => r.Val).LastAsync();
                // Distinct from First: if Last accidentally returns First, Id==1 not 5.
                Assert.Equal(5, row.Id);
                Assert.Equal(50, row.Val);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 4: LastOrDefault with no match ────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task LastOrDefault_returns_null_for_no_match_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<TopRow>().Where(r => r.Cat == "zzz").OrderBy(r => r.Val).LastOrDefaultAsync();
                Assert.Null(row);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 5: ElementAt returns correct offset row ───────────────────────────────
    //
    // OrderBy(Val) → [10,20,30,40,50]. ElementAt(2) = 0-based index 2 = Val=30.

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task ElementAt_returns_zero_based_offset_row_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<TopRow>().OrderBy(r => r.Val).ElementAtAsync(2);
                Assert.Equal(3, row.Id);
                Assert.Equal(30, row.Val);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 6: Count with predicate ───────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Count_with_predicate_returns_matching_row_count_on_live_provider(ProviderKind kind)
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
                // Cat='a' rows: Id=1 and Id=3 → 2 rows
                var count = await ctx.Query<TopRow>().Where(r => r.Cat == "a").CountAsync();
                Assert.Equal(2, count);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 7: LongCount ──────────────────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task LongCount_returns_long_total_row_count_on_live_provider(ProviderKind kind)
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
                var count = await ctx.Query<TopRow>().LongCountAsync();
                Assert.Equal(5L, count);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 8: Any with predicate ─────────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Any_with_predicate_returns_true_when_match_exists_on_live_provider(ProviderKind kind)
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
                // Val > 45 → only Id=5 (Val=50) matches → true
                var hasHigh = await ctx.Query<TopRow>().AnyAsync(r => r.Val > 45);
                Assert.True(hasHigh);

                // Val > 9999 → no match → false
                var hasMissing = await ctx.Query<TopRow>().AnyAsync(r => r.Val > 9999);
                Assert.False(hasMissing);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 9: All with predicate ─────────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task All_with_predicate_returns_true_only_when_every_row_matches_on_live_provider(ProviderKind kind)
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
                // All Val > 0 → true (all values are positive)
                var allPositive = await ctx.Query<TopRow>().AllAsync(r => r.Val > 0);
                Assert.True(allPositive);

                // All Val > 25 → false (Val=10 and Val=20 fail the predicate)
                var allHigh = await ctx.Query<TopRow>().AllAsync(r => r.Val > 25);
                Assert.False(allHigh);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 10: Single / SingleOrDefault ─────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Single_returns_the_one_matching_row_on_live_provider(ProviderKind kind)
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
                // Cat='c' has exactly one row (Id=5).
                var row = await ctx.Query<TopRow>().Where(r => r.Cat == "c").SingleAsync();
                Assert.Equal(5, row.Id);
                Assert.Equal(50, row.Val);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task SingleOrDefault_returns_null_when_no_row_matches_on_live_provider(ProviderKind kind)
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
                var row = await ctx.Query<TopRow>().Where(r => r.Cat == "zzz").SingleOrDefaultAsync();
                Assert.Null(row);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
