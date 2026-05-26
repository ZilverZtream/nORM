using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for <c>Union</c>, <c>Intersect</c>, <c>Except</c>, and
/// <c>Concat</c>, plus their composition with <c>Where</c>, <c>OrderBy</c>,
/// <c>Skip</c>, and <c>Take</c>.
///
/// The parity matrix row "Union / Intersect / Except / Concat" was marked ✅ for all
/// providers but cited only <c>QueryTranslatorCrossProviderTests</c> (shape-only) and
/// <c>LinqSetOpCompositionTests</c> (SQLite in-memory only) as evidence. This class
/// closes that gap with 4-provider live execution.
///
/// Schema: SopRow (Id INT PK, Val INT, Cat VARCHAR)
///
/// Left  set: Ids 1-3  → (1,10,'a'), (2,20,'b'), (3,30,'a')
/// Right set: Ids 3-5  → (3,30,'a'), (4,40,'b'), (5,50,'c')
///
/// Overlap: row 3 is in both sets.
///
/// Union     → dedup  → Ids {1,2,3,4,5}   (5 rows)
/// Concat    → all    → Ids {1,2,3,3,4,5} (6 rows with dup)
/// Intersect → common → Ids {3}            (1 row)
/// Except    → L−R    → Ids {1,2}          (2 rows)
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderSetOpParityTests
{
    private const string Table = "SopRow";

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
    private sealed class SopRow
    {
        [Key] public int Id  { get; set; }
        public int    Val { get; set; }
        public string Cat { get; set; } = "";
    }

    // ── 1: Union deduplicates rows ────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Union_deduplicates_overlapping_rows_on_live_provider(ProviderKind kind)
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
                // Left: Id<=3, Right: Id>=3. Row 3 appears in both → deduped by UNION.
                var left  = ctx.Query<SopRow>().Where(r => r.Id <= 3);
                var right = ctx.Query<SopRow>().Where(r => r.Id >= 3);
                var ids = (await left.Union(right).ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 1, 2, 3, 4, 5 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 2: Concat preserves duplicates ───────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Concat_preserves_duplicate_rows_on_live_provider(ProviderKind kind)
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
                // Left: Id<=3 (rows 1,2,3), Right: Id>=3 (rows 3,4,5).
                // UNION ALL yields 6 rows: 1,2,3,3,4,5.
                var left  = ctx.Query<SopRow>().Where(r => r.Id <= 3);
                var right = ctx.Query<SopRow>().Where(r => r.Id >= 3);
                var ids = (await left.Concat(right).ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 1, 2, 3, 3, 4, 5 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 3: Intersect returns only common rows ─────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Intersect_returns_only_rows_present_in_both_sides_on_live_provider(ProviderKind kind)
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
                // Left: Id<=3 (rows 1,2,3), Right: Id>=3 (rows 3,4,5).
                // INTERSECT → only row 3 (all columns must match).
                var left  = ctx.Query<SopRow>().Where(r => r.Id <= 3);
                var right = ctx.Query<SopRow>().Where(r => r.Id >= 3);
                var ids = (await left.Intersect(right).ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 3 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 4: Except returns left-only rows ──────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Except_returns_rows_in_left_not_present_in_right_on_live_provider(ProviderKind kind)
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
                // Left: Id<=3 (rows 1,2,3), Right: Id>=3 (rows 3,4,5).
                // EXCEPT → rows in left not in right → rows 1 and 2.
                var left  = ctx.Query<SopRow>().Where(r => r.Id <= 3);
                var right = ctx.Query<SopRow>().Where(r => r.Id >= 3);
                var ids = (await left.Except(right).ToListAsync())
                    .Select(r => r.Id).OrderBy(x => x).ToArray();

                Assert.Equal(new[] { 1, 2 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 5: Union + Where (derived-table wrapping) ─────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Where_after_Union_filters_the_combined_result_on_live_provider(ProviderKind kind)
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
                // Union of all rows (full table union full table) → 5 distinct rows.
                // Where(Cat == 'a') → rows 1 (Id=1,Cat=a) and 3 (Id=3,Cat=a).
                var q = ctx.Query<SopRow>().Union(ctx.Query<SopRow>())
                    .Where(r => r.Cat == "a")
                    .OrderBy(r => r.Id);
                var ids = (await q.ToListAsync()).Select(r => r.Id).ToArray();

                Assert.Equal(new[] { 1, 3 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 6: Union + OrderBy + Skip + Take ──────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Union_OrderBy_Skip_Take_pages_the_combined_result_on_live_provider(ProviderKind kind)
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
                // Left: Id<=3, Right: Id>=3 → Union → 5 distinct rows [1,2,3,4,5].
                // OrderBy(Id).Skip(1).Take(2) → [2,3].
                var left  = ctx.Query<SopRow>().Where(r => r.Id <= 3);
                var right = ctx.Query<SopRow>().Where(r => r.Id >= 3);
                var ids = (await left.Union(right).OrderBy(r => r.Id).Skip(1).Take(2).ToListAsync())
                    .Select(r => r.Id).ToArray();

                Assert.Equal(new[] { 2, 3 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 7: Concat + Where ─────────────────────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task Where_after_Concat_filters_all_rows_including_duplicates_on_live_provider(ProviderKind kind)
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
                // Left: Id<=3 (rows 1,2,3), Right: Id>=3 (rows 3,4,5).
                // Concat → 6 rows (3 appears twice). Where(Id != 3) → 4 rows: 1,2,4,5.
                var left  = ctx.Query<SopRow>().Where(r => r.Id <= 3);
                var right = ctx.Query<SopRow>().Where(r => r.Id >= 3);
                var ids = (await left.Concat(right).Where(r => r.Id != 3).OrderBy(r => r.Id).ToListAsync())
                    .Select(r => r.Id).ToArray();

                Assert.Equal(new[] { 1, 2, 4, 5 }, ids);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
