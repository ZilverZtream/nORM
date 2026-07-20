using System;
using System.Collections.Generic;
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
/// Chained Skip/Take must compose with LINQ semantics rather than the later call overwriting the earlier:
/// Skip(a).Skip(b) skips a+b, Take(a).Take(b) takes min(a,b), and Skip(a).Take(n).Skip(b) skips a+b. The
/// translator kept only the last paging value, silently returning the wrong rows. The chained-Skip case was
/// found and auto-minimized by the coverage-guided query-IR differential fuzzer. Paging after Distinct is
/// also covered.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PagingCompositionTests
{
    [Table("PcRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
    }

    private static DbContext Make(SqliteConnection cn, string data)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE PcRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL);" + data;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions(), ownsConnection: false);
    }

    private static int[] Ids(DbContext ctx, Func<IQueryable<Row>, IQueryable<Row>> shape)
        => shape(ctx.Query<Row>()).ToList().Select(r => r.Id).ToArray();

    private const string SixRows = "INSERT INTO PcRow VALUES (1,1),(2,2),(3,3),(4,4),(5,5),(6,6);";

    [Fact]
    public void Chained_skip_composes_additively()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Make(cn, SixRows);
        Assert.Equal(new[] { 4, 5, 6 }, Ids(ctx, q => q.OrderBy(r => r.Id).Skip(2).Skip(1)));   // skip 3
    }

    [Fact]
    public void Chained_take_composes_as_minimum()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Make(cn, SixRows);
        Assert.Equal(new[] { 1, 2 }, Ids(ctx, q => q.OrderBy(r => r.Id).Take(4).Take(2)));         // min(4,2)
    }

    [Fact]
    public void Skip_take_skip_preserves_the_earlier_offset()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Make(cn, SixRows);
        // Skip(1) -> [2..6]; Take(3) -> [2,3,4]; Skip(1) -> [3,4].
        Assert.Equal(new[] { 3, 4 }, Ids(ctx, q => q.OrderBy(r => r.Id).Skip(1).Take(3).Skip(1)));
    }

    [Fact]
    public void Skip_past_the_distinct_count_returns_empty()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Make(cn, "INSERT INTO PcRow VALUES (1,1),(2,5),(3,5),(4,2),(5,1);");
        Assert.Empty(ctx.Query<Row>().Where(r => r.A >= 5).Distinct().OrderBy(r => r.Id).Skip(5).ToList());
    }

    [Fact]
    public void Take_after_distinct_bounds_the_result()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Make(cn, "INSERT INTO PcRow VALUES (1,1),(2,5),(3,5),(4,2),(5,1);");
        var rows = ctx.Query<Row>().Where(r => r.A >= 5).Distinct().OrderBy(r => r.Id).Take(1).ToList();
        Assert.Single(rows);
        Assert.Equal(2, rows[0].Id);
    }
}
