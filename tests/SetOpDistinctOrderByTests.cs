using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A Distinct over a set operation is wrapped as a derived table (SELECT DISTINCT * FROM (compound) AS alias).
/// A trailing OrderBy/paging must resolve its key columns against that wrap alias, not a phantom alias that is
/// not in the SQL. Before the fix, `setop(...).Distinct().OrderBy(col)` emitted `ORDER BY T1.col` and SQLite
/// rejected it with "no such column: T1.col". Found by the coverage-guided query-IR differential fuzzer.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetOpDistinctOrderByTests
{
    [Table("SdRow")]
    public class Row { [Key] public int Id { get; set; } public int A { get; set; } public int B { get; set; } }

    private static DbContext Ctx(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SdRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);" +
                "INSERT INTO SdRow VALUES (1,5,3),(2,2,4),(3,5,5);";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false);
    }

    [Fact]
    public void Union_distinct_orderby_column_orders_the_deduplicated_result()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Ctx(cn);
        var ids = ctx.Query<Row>().Where(r => r.B < 6)
            .Union(ctx.Query<Row>().Where(r => r.A >= 5))
            .Distinct().OrderBy(r => r.A).ThenBy(r => r.Id)
            .ToList().Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 1, 3 }, ids);   // A=2(Id2), A=5(Id1), A=5(Id3)
    }

    [Fact]
    public void Union_distinct_paging_over_the_wrap_returns_the_window()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Ctx(cn);
        var ids = ctx.Query<Row>().Where(r => r.B < 6)
            .Union(ctx.Query<Row>().Where(r => r.A >= 5))
            .Distinct().OrderBy(r => r.Id).Skip(1).Take(1)
            .ToList().Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2 }, ids);          // ordered [1,2,3], skip 1 -> [2,3], take 1 -> [2]
    }
}
