using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A reshaping projection or a DISTINCT applied AFTER a set operation (Union/Concat/Intersect/Except) must
/// bind to the unified result of the compound, not be silently dropped. Where-after-set-op already wraps the
/// compound as a derived table; Select and Distinct must do the same. Without it, `setop(...).Select(x => x.Id
/// + 100)` returned the raw ids (projection lost, wrong values) and `Concat(...).Distinct()` kept the
/// duplicates (DISTINCT lost, duplicated rows). Oracle: LINQ-to-Objects over the same data.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetOperationProjectionDistinctTests
{
    [Table("SopRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    // (Id,A,B): A>5 -> {1,3,5,6}; B>5 -> {2,3,7}
    private static readonly (int Id, int A, int B)[] Data =
        { (1,8,2),(2,3,9),(3,7,7),(4,1,1),(5,10,4),(6,6,6),(7,2,8) };

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SopRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);" +
                string.Concat(Data.Select(d => $"INSERT INTO SopRow VALUES ({d.Id},{d.A},{d.B});"));
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false);
    }

    private static readonly List<Row> Oracle =
        Data.Select(d => new Row { Id = d.Id, A = d.A, B = d.B }).ToList();

    [Fact]
    public void Reshaping_projection_after_union_is_applied()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);
        var actual = ctx.Query<Row>().Where(x => x.A > 5)
            .Union(ctx.Query<Row>().Where(x => x.B > 5))
            .Select(x => x.Id + 100).ToList().OrderBy(v => v).ToArray();
        var expected = Oracle.Where(x => x.A > 5).Union(Oracle.Where(x => x.B > 5))
            .Select(x => x.Id + 100).OrderBy(v => v).ToArray();
        Assert.Equal(expected, actual);       // expected [101,102,103,105,106,107]
    }

    [Fact]
    public void Distinct_after_concat_removes_duplicates()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);
        var actual = ctx.Query<Row>().Where(x => x.A > 5).Select(x => x.Id)
            .Concat(ctx.Query<Row>().Where(x => x.B > 5).Select(x => x.Id))
            .Distinct().ToList().OrderBy(v => v).ToArray();
        var expected = Oracle.Where(x => x.A > 5).Select(x => x.Id)
            .Concat(Oracle.Where(x => x.B > 5).Select(x => x.Id))
            .Distinct().OrderBy(v => v).ToArray();
        Assert.Equal(expected, actual);       // expected [1,2,3,5,6,7]; bug returned 3 twice
    }

    [Fact]
    public void Count_of_distinct_after_concat_is_correct()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);
        var actual = ctx.Query<Row>().Where(x => x.A > 5).Select(x => x.Id)
            .Concat(ctx.Query<Row>().Where(x => x.B > 5).Select(x => x.Id))
            .Distinct().Count();
        var expected = Oracle.Where(x => x.A > 5).Select(x => x.Id)
            .Concat(Oracle.Where(x => x.B > 5).Select(x => x.Id))
            .Distinct().Count();
        Assert.Equal(expected, actual);       // expected 6
    }
}
