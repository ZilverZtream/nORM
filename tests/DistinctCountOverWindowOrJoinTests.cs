using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// `.Distinct().Count()` / `.LongCount()` whose Distinct source already emitted a full statement (a Take/Skip
/// window renders `SELECT DISTINCT * FROM (… LIMIT n)`, a JOIN renders `SELECT DISTINCT col FROM … JOIN …`)
/// must count the DEDUPED rows. The Count rewrite used to strip everything before the first ` FROM `, dropping
/// the leading `DISTINCT` (and the join projection), so it returned the pre-dedup / cartesian row count with
/// no error — a silently-wrong scalar. The fix wraps a top-level DISTINCT statement in a counting subquery
/// (the same shape already used for set-op sources).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DistinctCountOverWindowOrJoinTests
{
    [Table("DcwItem")]
    public class Item
    {
        [Key] public int Id { get; set; }
        public string City { get; set; } = "";
        public int Grp { get; set; }
    }

    private static DbContext NewCtx(out Item[] seed)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE DcwItem (Id INTEGER PRIMARY KEY, City TEXT NOT NULL, Grp INTEGER NOT NULL);" +
                // cities across 8 rows: NY,LA,NY,SF,LA,NY,SF,LA -> first 5 distinct = {NY,LA,SF} = 3
                "INSERT INTO DcwItem VALUES (1,'NY',1),(2,'LA',1),(3,'NY',1),(4,'SF',1),(5,'LA',1),(6,'NY',2),(7,'SF',2),(8,'LA',2);";
            cmd.ExecuteNonQuery();
        }
        seed = new[]
        {
            new Item{Id=1,City="NY",Grp=1}, new Item{Id=2,City="LA",Grp=1}, new Item{Id=3,City="NY",Grp=1},
            new Item{Id=4,City="SF",Grp=1}, new Item{Id=5,City="LA",Grp=1}, new Item{Id=6,City="NY",Grp=2},
            new Item{Id=7,City="SF",Grp=2}, new Item{Id=8,City="LA",Grp=2},
        };
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public async Task Ordered_take_select_distinct_count_matches_linq()
    {
        using var ctx = NewCtx(out var seed);
        var expected = seed.OrderBy(i => i.Id).Take(5).Select(i => i.City).Distinct().Count();
        var actual = await ctx.Query<Item>().OrderBy(i => i.Id).Take(5).Select(i => i.City).Distinct().CountAsync();
        Assert.Equal(3, expected);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Ordered_skip_take_select_distinct_count_matches_linq()
    {
        using var ctx = NewCtx(out var seed);
        var expected = seed.OrderBy(i => i.Id).Skip(1).Take(5).Select(i => i.City).Distinct().Count();
        var actual = await ctx.Query<Item>().OrderBy(i => i.Id).Skip(1).Take(5).Select(i => i.City).Distinct().CountAsync();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Ordered_take_select_distinct_longcount_matches_linq()
    {
        using var ctx = NewCtx(out var seed);
        var expected = seed.OrderBy(i => i.Id).Take(5).Select(i => i.City).Distinct().LongCount();
        var actual = await ctx.Query<Item>().OrderBy(i => i.Id).Take(5).Select(i => i.City).Distinct().LongCountAsync();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Join_select_distinct_count_matches_linq()
    {
        using var ctx = NewCtx(out var seed);
        var expected = seed.Join(seed, a => a.Grp, b => b.Grp, (a, b) => a.City).Distinct().Count();
        var actual = await ctx.Query<Item>()
            .Join(ctx.Query<Item>(), a => a.Grp, b => b.Grp, (a, b) => a.City)
            .Distinct().CountAsync();
        Assert.Equal(3, expected);
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task Distinct_count_without_window_or_join_stays_correct()
    {
        // Control: the plain projected-distinct-count path (already handled) must remain correct.
        using var ctx = NewCtx(out var seed);
        var expected = seed.Select(i => i.City).Distinct().Count();
        var actual = await ctx.Query<Item>().Select(i => i.City).Distinct().CountAsync();
        Assert.Equal(expected, actual);
    }
}
