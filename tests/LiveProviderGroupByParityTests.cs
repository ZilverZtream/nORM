using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Live-provider parity for GroupBy, GroupBy+HAVING, and GroupBy with composite
/// anonymous keys. Every row in the parity matrix that names
/// <c>LiveProviderShapeParityTests</c> as live evidence for GroupBy actually has no
/// GroupBy test in that file. This class closes that gap.
///
/// Test data: GbpRow (Id, Cat, Val, Tag)
///   (1, 'a', 10, 'x')
///   (2, 'b', 20, 'y')
///   (3, 'a', 30, 'x')
///   (4, 'b', 40, 'z')
///   (5, 'a', 50, 'x')
///   (6, 'c', 60, 'y')
///
/// GroupBy(Cat) → groups: a={1,3,5}, b={2,4}, c={6}
/// GroupBy(Cat).Where(g => g.Count() >= 2) HAVING → a(3), b(2)
/// GroupBy(new{Cat,Tag}) → a/x={1,3,5}, b/y={2}, b/z={4}, c/y={6}
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderGroupByParityTests
{
    private const string Table = "GbpRow";

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
        var eCat = ctx.Provider.Escape("Cat");
        var eVal = ctx.Provider.Escape("Val");
        var eTag = ctx.Provider.Escape("Tag");
        var intT = IntCol(kind);
        var varT = VarCol(kind, 10);
        await ExecuteAsync(ctx,
            $"{DropDdl(kind, esc)} CREATE TABLE {esc} " +
            $"({eId} {intT} PRIMARY KEY, {eCat} {varT} NOT NULL, {eVal} {intT} NOT NULL, {eTag} {varT} NOT NULL)");
        await ExecuteAsync(ctx,
            $"INSERT INTO {esc} ({eId},{eCat},{eVal},{eTag}) VALUES " +
            $"(1,'a',10,'x'),(2,'b',20,'y'),(3,'a',30,'x'),(4,'b',40,'z'),(5,'a',50,'x'),(6,'c',60,'y')");
    }

    private static async Task TeardownAsync(DbContext ctx, ProviderKind kind)
    {
        try { await ExecuteAsync(ctx, DropDdl(kind, ctx.Provider.Escape(Table))); }
        catch { /* best-effort */ }
    }

    [Table(Table)]
    private sealed class GbpRow
    {
        [Key] public int Id { get; set; }
        public string Cat { get; set; } = "";
        public int Val { get; set; }
        public string Tag { get; set; } = "";
    }

    // ── 1: GroupBy single key with count ─────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupBy_single_key_count_returns_correct_group_sizes_on_live_provider(ProviderKind kind)
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
                var groups = (await ctx.Query<GbpRow>()
                    .GroupBy(r => r.Cat)
                    .Select(g => new { Key = g.Key, Count = g.Count() })
                    .ToListAsync())
                    .OrderBy(g => g.Key)
                    .ToArray();

                Assert.Equal(3, groups.Length);
                Assert.Equal("a", groups[0].Key); Assert.Equal(3, groups[0].Count);
                Assert.Equal("b", groups[1].Key); Assert.Equal(2, groups[1].Count);
                Assert.Equal("c", groups[2].Key); Assert.Equal(1, groups[2].Count);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 2: GroupBy single key with Sum ───────────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupBy_single_key_sum_returns_correct_totals_on_live_provider(ProviderKind kind)
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
                var groups = (await ctx.Query<GbpRow>()
                    .GroupBy(r => r.Cat)
                    .Select(g => new { Key = g.Key, Total = g.Sum(r => r.Val) })
                    .ToListAsync())
                    .OrderBy(g => g.Key)
                    .ToArray();

                // a: 10+30+50=90, b: 20+40=60, c: 60
                Assert.Equal(3, groups.Length);
                Assert.Equal("a", groups[0].Key); Assert.Equal(90, groups[0].Total);
                Assert.Equal("b", groups[1].Key); Assert.Equal(60, groups[1].Total);
                Assert.Equal("c", groups[2].Key); Assert.Equal(60, groups[2].Total);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 3: GroupBy HAVING (group count filter) ───────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupBy_having_count_filters_small_groups_on_live_provider(ProviderKind kind)
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
                // Keep only groups with count >= 2: a(3), b(2) — not c(1)
                var groups = (await ctx.Query<GbpRow>()
                    .GroupBy(r => r.Cat)
                    .Where(g => g.Count() >= 2)
                    .Select(g => new { Key = g.Key, Count = g.Count() })
                    .ToListAsync())
                    .OrderBy(g => g.Key)
                    .ToArray();

                Assert.Equal(2, groups.Length);
                Assert.Equal("a", groups[0].Key); Assert.Equal(3, groups[0].Count);
                Assert.Equal("b", groups[1].Key); Assert.Equal(2, groups[1].Count);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 4: GroupBy HAVING aggregate predicate (sum filter) ───────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupBy_having_sum_filters_groups_by_aggregate_value_on_live_provider(ProviderKind kind)
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
                // Keep groups where Sum(Val) > 65: a(90), b(60 - NO), c(60 - NO)
                // Wait: b=60, c=60, a=90. Only a > 65.
                var groups = (await ctx.Query<GbpRow>()
                    .GroupBy(r => r.Cat)
                    .Where(g => g.Sum(r => r.Val) > 65)
                    .Select(g => new { Key = g.Key, Total = g.Sum(r => r.Val) })
                    .ToListAsync())
                    .OrderBy(g => g.Key)
                    .ToArray();

                Assert.Single(groups);
                Assert.Equal("a", groups[0].Key);
                Assert.Equal(90, groups[0].Total);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 5: GroupBy composite anonymous key ───────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupBy_composite_anon_key_groups_by_two_columns_on_live_provider(ProviderKind kind)
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
                // Groups: (a,x)={1,3,5}→3, (b,y)={2}→1, (b,z)={4}→1, (c,y)={6}→1
                var groups = (await ctx.Query<GbpRow>()
                    .GroupBy(r => new { r.Cat, r.Tag })
                    .Select(g => new { g.Key.Cat, g.Key.Tag, Count = g.Count() })
                    .ToListAsync())
                    .OrderBy(g => g.Cat).ThenBy(g => g.Tag)
                    .ToArray();

                Assert.Equal(4, groups.Length);
                Assert.Equal(("a", "x", 3), (groups[0].Cat, groups[0].Tag, groups[0].Count));
                Assert.Equal(("b", "y", 1), (groups[1].Cat, groups[1].Tag, groups[1].Count));
                Assert.Equal(("b", "z", 1), (groups[2].Cat, groups[2].Tag, groups[2].Count));
                Assert.Equal(("c", "y", 1), (groups[3].Cat, groups[3].Tag, groups[3].Count));
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 6: GroupBy composite key with HAVING ─────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupBy_composite_key_having_count_filters_correctly_on_live_provider(ProviderKind kind)
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
                // Only (a,x) has count >= 2
                var groups = (await ctx.Query<GbpRow>()
                    .GroupBy(r => new { r.Cat, r.Tag })
                    .Where(g => g.Count() >= 2)
                    .Select(g => new { g.Key.Cat, g.Key.Tag, Count = g.Count() })
                    .ToListAsync())
                    .OrderBy(g => g.Cat).ThenBy(g => g.Tag)
                    .ToArray();

                Assert.Single(groups);
                Assert.Equal("a", groups[0].Cat);
                Assert.Equal("x", groups[0].Tag);
                Assert.Equal(3, groups[0].Count);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    // ── 7: GroupBy with Min/Max aggregates ───────────────────────────────────

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupBy_with_min_max_returns_correct_range_on_live_provider(ProviderKind kind)
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
                var groups = (await ctx.Query<GbpRow>()
                    .GroupBy(r => r.Cat)
                    .Select(g => new { Key = g.Key, Min = g.Min(r => r.Val), Max = g.Max(r => r.Val) })
                    .ToListAsync())
                    .OrderBy(g => g.Key)
                    .ToArray();

                // a: min=10, max=50; b: min=20, max=40; c: min=60, max=60
                Assert.Equal(3, groups.Length);
                Assert.Equal(10, groups[0].Min); Assert.Equal(50, groups[0].Max);
                Assert.Equal(20, groups[1].Min); Assert.Equal(40, groups[1].Max);
                Assert.Equal(60, groups[2].Min); Assert.Equal(60, groups[2].Max);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    [InlineData(ProviderKind.Sqlite)]
    public async Task GroupBy_raw_igrouping_groups_client_side_after_provider_fetch(ProviderKind kind)
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
                var groups = (await ctx.Query<GbpRow>()
                    .GroupBy(r => r.Cat)
                    .ToListAsync())
                    .OrderBy(g => g.Key)
                    .ToArray();

                Assert.Equal(3, groups.Length);
                Assert.Equal("a", groups[0].Key); Assert.Equal(3, groups[0].Count());
                Assert.Equal("b", groups[1].Key); Assert.Equal(2, groups[1].Count());
                Assert.Equal("c", groups[2].Key); Assert.Single(groups[2]);
            }
            finally { await TeardownAsync(ctx, kind); }
        }
    }
}
