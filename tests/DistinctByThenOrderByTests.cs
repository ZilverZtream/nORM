using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// `DistinctBy(key)` followed by a top-level `OrderBy`/`Skip`/`Take` must order the deduped result. The
/// DistinctBy translator renders its own derived-table wrap aliased `__distinctbyN`; a subsequent OrderBy
/// on the outer sequence resolved its parameter to a freshly-minted `T{n}` alias the FROM never defines,
/// emitting `ORDER BY "T2"."Id"` -> `no such column: T2.Id`. The outer ordering must bind to the
/// derived-table alias instead.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DistinctByThenOrderByTests
{
    [Table("DboItem")]
    public class Item
    {
        [Key] public int Id { get; set; }
        public string City { get; set; } = "";
    }

    private static DbContext NewCtx(out Item[] seed)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE DboItem (Id INTEGER PRIMARY KEY, City TEXT NOT NULL);" +
                "INSERT INTO DboItem VALUES (1,'NY'),(2,'LA'),(3,'NY'),(4,'SF'),(5,'LA'),(6,'NY');";
            cmd.ExecuteNonQuery();
        }
        seed = new[]
        {
            new Item{Id=1,City="NY"}, new Item{Id=2,City="LA"}, new Item{Id=3,City="NY"},
            new Item{Id=4,City="SF"}, new Item{Id=5,City="LA"}, new Item{Id=6,City="NY"},
        };
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void DistinctBy_then_orderby_descending_matches_linq()
    {
        using var ctx = NewCtx(out var seed);
        // DistinctBy(City) keeps the first row per city (NY=1, LA=2, SF=4), then OrderBy(Id) desc -> 4,2,1.
        var expected = seed.DistinctBy(i => i.City).OrderByDescending(i => i.Id).Select(i => i.Id).ToArray();
        var actual = ctx.Query<Item>().DistinctBy(i => i.City).OrderByDescending(i => i.Id).Select(i => i.Id).ToList().ToArray();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void DistinctBy_then_orderby_skip_take_matches_linq()
    {
        using var ctx = NewCtx(out var seed);
        var expected = seed.DistinctBy(i => i.City).OrderBy(i => i.Id).Skip(1).Take(1).Select(i => i.Id).ToArray();
        var actual = ctx.Query<Item>().DistinctBy(i => i.City).OrderBy(i => i.Id).Skip(1).Take(1).Select(i => i.Id).ToList().ToArray();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void ExceptBy_then_orderby_descending_matches_linq()
    {
        using var ctx = NewCtx(out var seed);
        var keys = new[] { "LA" };
        // keep rows whose City is not LA -> {1,3,4,6}, then order by Id desc.
        var expected = seed.ExceptBy(keys, i => i.City).OrderByDescending(i => i.Id).Select(i => i.Id).ToArray();
        var actual = ctx.Query<Item>().ExceptBy(keys, i => i.City).OrderByDescending(i => i.Id).Select(i => i.Id).ToList().ToArray();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void IntersectBy_then_orderby_descending_matches_linq()
    {
        using var ctx = NewCtx(out var seed);
        var keys = new[] { "NY", "SF" };
        var expected = seed.IntersectBy(keys, i => i.City).OrderByDescending(i => i.Id).Select(i => i.Id).ToArray();
        var actual = ctx.Query<Item>().IntersectBy(keys, i => i.City).OrderByDescending(i => i.Id).Select(i => i.Id).ToList().ToArray();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void UnionBy_then_orderby_descending_matches_linq()
    {
        using var ctx = NewCtx(out var seed);
        var extra = new[] { new Item { Id = 99, City = "BOS" }, new Item { Id = 100, City = "NY" } };
        var expected = seed.UnionBy(extra, i => i.City).OrderByDescending(i => i.Id).Select(i => i.Id).ToArray();
        var actual = ctx.Query<Item>().UnionBy(extra, i => i.City).OrderByDescending(i => i.Id).Select(i => i.Id).ToList().ToArray();
        Assert.Equal(expected, actual);
    }
}
