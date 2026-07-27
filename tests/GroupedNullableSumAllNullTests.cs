using System;
using System.Collections.Generic;
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
/// Regression: a grouped Sum over a nullable column must follow Enumerable.Sum semantics — for a group
/// whose values are all NULL, Sum(int?) skips the nulls over an empty effective sequence and returns 0,
/// not null. The scalar terminal Sum already coerces DBNull→0; the grouped projection path emitted a bare
/// SUM(col) with no COALESCE and materialized SQL NULL back as the nullable default (null) for an all-null
/// group — silently diverging from LINQ-to-Objects.
/// </summary>
[Trait("Category", "Fast")]
public class GroupedNullableSumAllNullTests
{
    [Table("GrpSumRow")]
    private sealed class Row
    {
        [Key] public int Id { get; set; }
        public int Category { get; set; }
        public int? Amount { get; set; }
    }

    private static readonly Row[] Seed =
    {
        new Row { Id = 1, Category = 1, Amount = null },   // category 1: ALL null
        new Row { Id = 2, Category = 1, Amount = null },
        new Row { Id = 3, Category = 2, Amount = 5 },      // category 2: has a value
        new Row { Id = 4, Category = 2, Amount = null },   // mixed null + value
    };

    [Fact]
    public async Task Grouped_sum_of_an_all_null_group_is_zero_not_null()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE GrpSumRow (Id INTEGER PRIMARY KEY, Category INTEGER NOT NULL, Amount INTEGER NULL);";
            c.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in Seed) await ctx.InsertAsync(r);

        var norm = ctx.Query<Row>()
            .GroupBy(x => x.Category)
            .Select(g => new { g.Key, Total = g.Sum(x => x.Amount) })
            .ToList()
            .OrderBy(x => x.Key)
            .ToList();

        var oracle = Seed
            .GroupBy(x => x.Category)
            .Select(g => new { g.Key, Total = g.Sum(x => x.Amount) })
            .OrderBy(x => x.Key)
            .ToList();

        Assert.Equal(oracle.Select(x => x.Key), norm.Select(x => x.Key));
        // The crux: category 1 (all-null) must be 0, matching Enumerable.Sum(int?), not null.
        Assert.Equal(oracle.Select(x => x.Total), norm.Select(x => x.Total));
        Assert.Equal(0, norm.Single(x => x.Key == 1).Total);
    }
}
