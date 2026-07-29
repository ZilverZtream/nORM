using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A grouped SUM/AVG/MIN/MAX over a value-converter column (GroupBy(...).Select(g => g.Max(x => x.ConvCol)))
/// runs over the STORED provider values and the projection materializer reads the result back UNCONVERTED, so
/// the grouped result was silently wrong (MAX over a +1000 offset converter returned 1009 not 9; SUM returned
/// the stored total). Correct translation is impossible for a non-linear/non-monotonic converter, so nORM now
/// rejects these forms fail-loud — consistent with the top-level scalar Sum/Avg guard and never silently wrong.
/// Aggregating a plain (converter-free) column is unaffected. The scalar Min/Max contract over a non-monotonic
/// converter (converts the returned value but orders in provider space) is documented here for completeness.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupedAggregateOverConverterTests
{
    [Table("Hunt54Order")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int Bucket { get; set; }   // plain group key, no converter
        public int Score { get; set; }    // value-converter column
        public int Qty { get; set; }      // plain int, no converter
    }

    // Order-preserving offset: model N stored as N + 1000. Monotonic, but SUM of stored != model sum.
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => Convert.ToInt32(value) - 1000;
    }

    // Negating: model N stored as -N. NON-monotonic (order-reversing).
    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => -value;
        public override object? ConvertFromProvider(int value) => -Convert.ToInt32(value);
    }

    private static DbContext Bootstrap(SqliteConnection cn, ValueConverter<int, int> conv, string storedRows)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Hunt54Order (Id INTEGER PRIMARY KEY, Bucket INTEGER NOT NULL, Score INTEGER NOT NULL, Qty INTEGER NOT NULL);" +
                "INSERT INTO Hunt54Order VALUES " + storedRows + ";";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().Property<int>(o => o.Score).HasConversion(conv);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // Bucket 1 -> model scores {5, 9}; Bucket 2 -> model score {7}.  (offset: stored = model + 1000)
    private const string OffsetRows = "(1,1,1005,100),(2,1,1009,200),(3,2,1007,300)";

    [Fact]
    public void Grouped_Max_over_converter_column_is_rejected()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn, new OffsetConverter(), OffsetRows);

        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>().GroupBy(o => o.Bucket)
                .Select(g => new { g.Key, Mx = g.Max(x => x.Score) })
                .ToList());
    }

    [Fact]
    public void Grouped_Min_over_converter_column_is_rejected()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn, new OffsetConverter(), OffsetRows);

        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>().GroupBy(o => o.Bucket)
                .Select(g => new { g.Key, Mn = g.Min(x => x.Score) })
                .ToList());
    }

    [Fact]
    public void Grouped_Sum_over_converter_column_is_rejected()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn, new OffsetConverter(), OffsetRows);

        // The top-level scalar Sum over a converter column also fails loud; the grouped path is now consistent
        // (it previously returned the stored sum 2014 silently).
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>().GroupBy(o => o.Bucket)
                .Select(g => new { g.Key, Sm = g.Sum(x => x.Score) })
                .ToList());
    }

    [Fact]
    public void Grouped_Max_over_negating_converter_is_rejected()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn, new NegatingConverter(), "(1,1,-10,1),(2,1,-30,1),(3,2,-20,1)");

        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>().GroupBy(o => o.Bucket)
                .Select(g => new { g.Key, Mx = g.Max(x => x.Score) })
                .ToList());
    }

    // CONTROL: aggregating a PLAIN (converter-free) column is unaffected by the guard.
    [Fact]
    public void Grouped_aggregate_over_plain_column_still_works()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn, new OffsetConverter(), OffsetRows);

        var byBucket = ctx.Query<Order>()
            .GroupBy(o => o.Bucket)
            .Select(g => new { g.Key, MaxQty = g.Max(x => x.Qty), SumQty = g.Sum(x => x.Qty) })
            .ToList().ToDictionary(x => x.Key, x => x);

        Assert.Equal(200, byBucket[1].MaxQty); // max(100,200)
        Assert.Equal(300, byBucket[1].SumQty); // 100+200
        Assert.Equal(300, byBucket[2].MaxQty);
    }

    // Documents the ACCEPTED scalar Min/Max contract over a NON-monotonic converter: the returned value is
    // converted (ConvertFromProvider) but the extremum is chosen in provider space, so a negating converter
    // inverts which row wins. This is the numeric generalization of the pending string-enum ordering decision;
    // the scalar path deliberately keeps it (correct translation is impossible), unlike the grouped path which
    // now fails loud. Pinned here so a change to that contract is caught.
    [Fact]
    public void Scalar_MinMax_over_non_monotonic_converter_converts_value_but_orders_in_provider_space()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        // model {10,20,30} stored {-10,-20,-30}
        using var ctx = Bootstrap(cn, new NegatingConverter(), "(1,1,-10,1),(2,1,-20,1),(3,1,-30,1)");

        // Server MAX(stored) = -10 -> ConvertFromProvider(-10) = 10 (the model MINIMUM).
        Assert.Equal(10, ctx.Query<Order>().Max(o => o.Score));
        // Server MIN(stored) = -30 -> ConvertFromProvider(-30) = 30 (the model MAXIMUM).
        Assert.Equal(30, ctx.Query<Order>().Min(o => o.Score));
    }
}
