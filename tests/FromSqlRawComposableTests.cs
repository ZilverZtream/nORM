using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
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
}
