using System;
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
/// Correctness sweep over aggregate query shapes (Sum/Min/Max/Average/Count, with and without
/// predicates, over empty sets and nullable columns, and grouped) — the same silent-wrong-result
/// risk area as the fast-path operator-drop bugs.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class AggregateCorrectnessSweepTests
{
    [Table("AgRow")]
    private class AgRow
    {
        [Key] public int Id { get; set; }
        public int Value { get; set; }
        public int? Nullable { get; set; }
        public int Bucket { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Value: 10,20,30,40; Nullable: 5,NULL,15,NULL; Bucket: 0,1,0,1
            cmd.CommandText = "CREATE TABLE AgRow (Id INTEGER PRIMARY KEY, Value INTEGER NOT NULL, Nullable INTEGER, Bucket INTEGER NOT NULL);" +
                              "INSERT INTO AgRow VALUES (1,10,5,0),(2,20,NULL,1),(3,30,15,0),(4,40,NULL,1);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void Scalar_aggregates_are_correct()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Equal(100, ctx.Query<AgRow>().Sum(r => r.Value));
        Assert.Equal(10, ctx.Query<AgRow>().Min(r => r.Value));
        Assert.Equal(40, ctx.Query<AgRow>().Max(r => r.Value));
        Assert.Equal(25.0, ctx.Query<AgRow>().Average(r => r.Value));
        Assert.Equal(4, ctx.Query<AgRow>().Count());

        // Nullable column: SQL SUM/AVG ignore NULLs (5 + 15 = 20; avg over 2 non-null = 10).
        Assert.Equal(20, ctx.Query<AgRow>().Sum(r => r.Nullable));
        Assert.Equal(15, ctx.Query<AgRow>().Max(r => r.Nullable));
    }

    [Fact]
    public void Filtered_aggregates_are_correct()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        // Bucket 0 -> rows 1,3 (Value 10,30)
        Assert.Equal(40, ctx.Query<AgRow>().Where(r => r.Bucket == 0).Sum(r => r.Value));
        Assert.Equal(2, ctx.Query<AgRow>().Count(r => r.Bucket == 1));
        Assert.Equal(30, ctx.Query<AgRow>().Where(r => r.Bucket == 0).Max(r => r.Value));
    }

    [Fact]
    public void Sum_over_empty_set_is_zero()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        Assert.Equal(0, ctx.Query<AgRow>().Where(r => r.Id == 999).Sum(r => r.Value));
        Assert.Equal(0, ctx.Query<AgRow>().Count(r => r.Id == 999));
    }

    [Fact]
    public void GroupBy_aggregate_is_correct()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        // Bucket 0: 10+30=40 ; Bucket 1: 20+40=60
        var byBucket = ctx.Query<AgRow>()
            .GroupBy(r => r.Bucket)
            .Select(g => new { Bucket = g.Key, Sum = g.Sum(x => x.Value) })
            .OrderBy(x => x.Bucket)
            .ToList();

        Assert.Equal(2, byBucket.Count);
        Assert.Equal(40, byBucket[0].Sum);
        Assert.Equal(60, byBucket[1].Sum);
    }
}
