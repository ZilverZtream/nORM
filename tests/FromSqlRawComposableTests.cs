using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Composable FromSqlRaw / FromSqlInterpolated: the raw SQL is wrapped as a derived table so ordinary LINQ
/// operators (Where, OrderBy, Skip/Take, projection, Count) and the entity's global/tenant filters compose on
/// top. Parameters bind positionally as @p0, @p1 without colliding with the composed operators' parameters.
/// Shapes that would build the FROM from a different source (joins, GroupBy, Include, set operators) fail loud
/// rather than silently querying the mapped table instead of the raw SQL.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FromSqlRawComposableTests
{
    [Table("FsrWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FsrWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL);
                INSERT INTO FsrWidget VALUES (1,'a',3),(2,'b',7),(3,'c',9),(4,'d',1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void basic_raw_then_tolist()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var ids = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget").ToList().Select(w => w.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1, 2, 3, 4 }, ids);
    }

    [Fact]
    public void raw_composed_with_where_and_orderby()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var ids = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget")
            .Where(w => w.Score > 5)
            .OrderByDescending(w => w.Score)
            .Select(w => w.Id)
            .ToList();
        Assert.Equal(new[] { 3, 2 }, ids);   // scores 9, 7
    }

    [Fact]
    public void raw_with_parameter_composed_with_paging()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var ids = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget WHERE Score >= @p0", 3)
            .OrderBy(w => w.Id)
            .Skip(1).Take(1)
            .Select(w => w.Id)
            .ToList();
        // Score >= 3 → ids 1,2,3 ordered; skip 1 take 1 → id 2.
        Assert.Equal(new[] { 2 }, ids);
    }

    [Fact]
    public void raw_count()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var n = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget").Count(w => w.Score > 5);
        Assert.Equal(2, n);
    }

    [Fact]
    public async Task raw_composed_tolist_async()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var rows = await ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget")
            .Where(w => w.Score >= 7).OrderBy(w => w.Id).ToListAsync();
        Assert.Equal(new[] { 2, 3 }, rows.Select(w => w.Id).ToArray());
    }

    [Fact]
    public void interpolated_composable()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var threshold = 5;
        var ids = ctx.FromSqlInterpolated<Widget>($"SELECT * FROM FsrWidget WHERE Score > {threshold}")
            .OrderBy(w => w.Id).Select(w => w.Id).ToList();
        Assert.Equal(new[] { 2, 3 }, ids);
    }

    [Fact]
    public void distinct_raw_sql_strings_do_not_share_a_cached_plan()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var a = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget WHERE Score > 5").OrderBy(w => w.Id).Select(w => w.Id).ToList();
        var b = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget WHERE Score < 5").OrderBy(w => w.Id).Select(w => w.Id).ToList();
        Assert.Equal(new[] { 2, 3 }, a);   // scores 7, 9
        Assert.Equal(new[] { 1, 4 }, b);   // scores 3, 1 — must NOT reuse the first plan's SQL
    }

    [Fact]
    public void same_sql_different_parameters_binds_fresh_values()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var a = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget WHERE Score >= @p0", 7).Select(w => w.Id).OrderBy(i => i).ToList();
        var b = ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget WHERE Score >= @p0", 3).Select(w => w.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2, 3 }, a);        // Score >= 7 → 7, 9
        Assert.Equal(new[] { 1, 2, 3 }, b);     // Score >= 3 → 3, 7, 9 — must bind 3, not replay the cached 7
    }

    [Fact]
    public void complex_shape_over_raw_fails_loud()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget").GroupBy(w => w.Score).Select(g => g.Count()).ToList());
    }

    [Fact]
    public void norm_operators_compose_over_raw_root()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        // FromSqlRaw returns an INormQueryable, so the nORM-specific operators (AsNoTracking, AsSplitQuery)
        // chain directly off it — a raw query is a first-class nORM query, not a foreign IQueryable.
        var raw = (INormQueryable<Widget>)ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget WHERE Score > 5");
        var ids = raw.AsNoTracking().AsSplitQuery().OrderBy(w => w.Id).Select(w => w.Id).ToList();
        Assert.Equal(new[] { 2, 3 }, ids);          // scores 7, 9 — the raw filter is honoured
        Assert.Empty(ctx.ChangeTracker.Entries);    // AsNoTracking left nothing tracked
    }

    // The mapped table (FsrWidget) has 4 rows; these raw SQLs filter to 2 (or 0). Each shape below hits a
    // dedicated fast path that builds SQL from the mapped table — the assertions fail if the fast path fires
    // and ignores the raw SQL. (Several earlier tests use an unfiltered raw SQL, which can't detect that.)
    [Fact]
    public void raw_filter_is_honored_through_fast_path_shapes()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        const string filtered = "SELECT * FROM FsrWidget WHERE Score > 5";  // ids 2,3 (scores 7,9)

        // Direct list — the list fast path selects * from the mapped table.
        var all = ctx.FromSqlRaw<Widget>(filtered).ToList().Select(w => w.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 2, 3 }, all);

        // First over an ordering — a mapped-table first row would be id 1.
        var first = ctx.FromSqlRaw<Widget>(filtered).OrderBy(w => w.Id).First();
        Assert.Equal(2, first.Id);

        // Sync Count with no predicate — a mapped-table count would be 4.
        Assert.Equal(2, ctx.FromSqlRaw<Widget>(filtered).Count());

        // Any with a raw filter that excludes every row — a mapped-table Any() would be true.
        Assert.False(ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget WHERE Score > 100").Any());
        Assert.True(ctx.FromSqlRaw<Widget>(filtered).Any());

        // Scalar aggregates — a mapped-table Sum/Min would be 20 / 1.
        Assert.Equal(16, ctx.FromSqlRaw<Widget>(filtered).Sum(w => w.Score));   // 7 + 9
        Assert.Equal(7, ctx.FromSqlRaw<Widget>(filtered).Min(w => w.Score));
        Assert.Equal(9, ctx.FromSqlRaw<Widget>(filtered).Max(w => w.Score));

        // All — over the raw rows (7,9) every Score > 3, but the mapped table (incl. 1,3) would be false.
        Assert.True(ctx.FromSqlRaw<Widget>(filtered).All(w => w.Score > 3));
    }

    [Fact]
    public async Task async_terminals_compose_directly_on_raw_root()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        // Async terminals are called straight on the raw queryable (no intervening standard operator to
        // turn it into a NormQueryableImpl first), exercising NormRawSqlQueryable's own INormQueryable surface.
        var raw = (INormQueryable<Widget>)ctx.FromSqlRaw<Widget>("SELECT * FROM FsrWidget WHERE Score >= @p0", 7);
        Assert.Equal(2, await raw.CountAsync());
        var list = await raw.ToListAsync();
        Assert.Equal(new[] { 2, 3 }, list.Select(w => w.Id).OrderBy(i => i).ToArray());
    }

    [Table("FsrOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public System.Collections.Generic.List<Line> Lines { get; set; } = new();
    }

    [Table("FsrLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
    }

    [Fact]
    public void include_over_raw_root_fails_loud()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FsrOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE FsrLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL);
                INSERT INTO FsrOrder VALUES (1),(2),(3);
                INSERT INTO FsrLine VALUES (1,1),(2,1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Include rebuilds the root from the mapped table and would silently drop the raw filter, so it must
        // fail loud rather than return the wrong rows.
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ((INormQueryable<Order>)ctx.FromSqlRaw<Order>("SELECT * FROM FsrOrder WHERE Id <= 2"))
                .Include(o => o.Lines)
                .ToList());
        Assert.Contains("Include", ex.Message);
    }

    [Table("FsrSoft")]
    public class Soft
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
        public bool IsDeleted { get; set; }
    }

    [Fact]
    public void raw_applies_global_soft_delete_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FsrSoft (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO FsrSoft VALUES (1,3,0),(2,7,1),(3,9,0);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Soft>(s => !s.IsDeleted);
        opts.OnModelCreating = mb => mb.Entity<Soft>().HasKey(s => s.Id);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Row 2 is soft-deleted; the global filter is applied to the outer query over the raw derived table.
        var ids = ctx.FromSqlRaw<Soft>("SELECT * FROM FsrSoft").OrderBy(s => s.Id).Select(s => s.Id).ToList();
        Assert.Equal(new[] { 1, 3 }, ids);
    }

    [Table("FsrTenantWidget")]
    public class TenantWidget
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public int Score { get; set; }
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    // Security contract: the tenant predicate is applied to the OUTER query over the raw derived table, so a
    // composable FromSqlRaw can never read (or aggregate) another tenant's rows — even when the raw SQL itself
    // explicitly names them. This is the security-critical companion to the soft-delete global-filter test.
    [Fact]
    public void raw_enforces_tenant_isolation_including_a_cross_tenant_attack()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FsrTenantWidget (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Score INTEGER NOT NULL);
                INSERT INTO FsrTenantWidget VALUES (1,1,10),(2,1,20),(3,2,300),(4,2,400);
                """;
            cmd.ExecuteNonQuery();
        }

        DbContext CtxFor(int tenant) => new(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenantProvider(tenant),
            OnModelCreating = mb => mb.Entity<TenantWidget>().HasKey(w => w.Id)
        });

        // Each tenant sees only its own rows through a raw SELECT *.
        var t1 = CtxFor(1);
        Assert.Equal(new[] { 1, 2 },
            t1.FromSqlRaw<TenantWidget>("SELECT * FROM FsrTenantWidget").OrderBy(w => w.Id).Select(w => w.Id).ToList());
        var t2 = CtxFor(2);
        Assert.Equal(new[] { 3, 4 },
            t2.FromSqlRaw<TenantWidget>("SELECT * FROM FsrTenantWidget").OrderBy(w => w.Id).Select(w => w.Id).ToList());

        // Cross-tenant attack: tenant 1's query whose raw SQL explicitly asks for tenant 2's rows returns
        // nothing — the outer tenant predicate filters the derived table regardless of what the SQL selected.
        Assert.Empty(
            t1.FromSqlRaw<TenantWidget>("SELECT * FROM FsrTenantWidget WHERE TenantId = 2").ToList());

        // Aggregates and Count are tenant-scoped too (would be 730 / 4 without the outer tenant filter).
        Assert.Equal(30, t1.FromSqlRaw<TenantWidget>("SELECT * FROM FsrTenantWidget").Sum(w => w.Score));
        Assert.Equal(2, t1.FromSqlRaw<TenantWidget>("SELECT * FROM FsrTenantWidget").Count());
    }
}
