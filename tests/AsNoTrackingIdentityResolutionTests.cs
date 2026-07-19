using System;
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
/// <c>AsNoTrackingWithIdentityResolution</c> (EF Core parity): the query is untracked, but a root entity whose
/// key appears more than once in one result set (e.g. a Concat/UNION ALL with overlapping arms) is materialized
/// to a SINGLE shared instance — unlike plain <c>AsNoTracking</c>, which returns distinct instances per row.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AsNoTrackingIdentityResolutionTests
{
    [Table("AnirItem")]
    public class Item
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext Boot(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AnirItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                              "INSERT INTO AnirItem (Id, Name) VALUES (1,'a'),(2,'b'),(3,'c');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Item>().HasKey(i => i.Id) };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    // Concat's two arms overlap on Id=2, so the UNION ALL result contains Id=2 twice.
    private static INormQueryable<Item> OverlappingConcat(DbContext ctx) =>
        (INormQueryable<Item>)ctx.Query<Item>().Where(i => i.Id <= 2).Concat(ctx.Query<Item>().Where(i => i.Id >= 2));

    [Fact]
    public void identity_resolution_collapses_a_duplicated_root_to_one_instance()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var rows = OverlappingConcat(ctx).AsNoTrackingWithIdentityResolution().ToList();
        var twos = rows.Where(i => i.Id == 2).ToList();

        Assert.Equal(2, twos.Count);        // Id=2 still appears twice in the sequence
        Assert.Same(twos[0], twos[1]);      // but both are the SAME instance
    }

    [Fact]
    public void plain_no_tracking_returns_distinct_instances_for_a_duplicated_root()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var rows = OverlappingConcat(ctx).AsNoTracking().ToList();
        var twos = rows.Where(i => i.Id == 2).ToList();

        Assert.Equal(2, twos.Count);
        Assert.NotSame(twos[0], twos[1]);   // no identity resolution → distinct instances
    }

    [Fact]
    public async Task identity_resolution_works_on_the_async_path()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var rows = await OverlappingConcat(ctx).AsNoTrackingWithIdentityResolution().ToListAsync();
        var twos = rows.Where(i => i.Id == 2).ToList();

        Assert.Equal(2, twos.Count);
        Assert.Same(twos[0], twos[1]);
    }

    [Fact]
    public void results_are_not_tracked()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        var item = OverlappingConcat(ctx).AsNoTrackingWithIdentityResolution().ToList().First();
        Assert.Throws<InvalidOperationException>(() => ctx.Entry(item));   // untracked
    }

    [Fact]
    public async Task identity_resolution_works_for_a_compiled_query()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        // Norm.CompileQuery generates its own inline materialize loops, separate from the executor's — they
        // must resolve identity too, else a compiled duplicate-root query returns distinct instances.
        var compiled = Norm.CompileQuery((DbContext c, int _) =>
            ((INormQueryable<Item>)c.Query<Item>().Where(i => i.Id <= 2).Concat(c.Query<Item>().Where(i => i.Id >= 2)))
                .AsNoTrackingWithIdentityResolution());

        var rows = await compiled(ctx, 0);
        var twos = rows.Where(i => i.Id == 2).ToList();

        Assert.Equal(2, twos.Count);
        Assert.Same(twos[0], twos[1]);
    }

    [Fact]
    public void simple_single_table_query_still_works_with_the_new_operator()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);

        // A plain single-table query can never produce a duplicate root; the operator must still be accepted
        // and return the rows (no duplicates to collapse).
        var rows = ((INormQueryable<Item>)ctx.Query<Item>()).AsNoTrackingWithIdentityResolution().ToList();
        Assert.Equal(new[] { 1, 2, 3 }, rows.Select(i => i.Id).OrderBy(x => x).ToArray());
    }
}
