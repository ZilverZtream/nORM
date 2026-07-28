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
/// Sum/Average over a value-converter column combine the STORED provider values; ConvertFromProvider is a
/// per-value map that does not distribute over the aggregate for a non-linear converter, so there is no
/// correct scalar result — returning the raw stored aggregate is silently wrong. The navigation-collection
/// path already fails loud (GuardAggregateOverConverterColumn); the top-level ctx.Query path must be
/// consistent. Min/Max return one stored value and ARE converted back to the model value.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AggregateOverConverterColumnTests
{
    [Table("AocOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }

    // Order-preserving: model N stored as N + 1000. Sum of stored (3021) != model Sum (21).
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => Convert.ToInt32(value) - 1000;
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE AocOrder (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);" +
                "INSERT INTO AocOrder VALUES (1,1005),(2,1007),(3,1009);";   // model 5,7,9
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().Property<int>(o => o.Score).HasConversion(new OffsetConverter());
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void Sum_over_converter_column_fails_loud()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);
        Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Query<Order>().Sum(o => o.Score));
    }

    [Fact]
    public void Average_over_converter_column_fails_loud()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);
        Assert.Throws<NormUnsupportedFeatureException>(() => ctx.Query<Order>().Average(o => o.Score));
    }

    [Fact]
    public void Max_over_converter_column_still_returns_model_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);
        Assert.Equal(9, ctx.Query<Order>().Max(o => o.Score));   // stored 1009 -> model 9
    }

    // ---- many-to-many collection aggregate over a converter column ----
    [Table("AocPost")] public class Post { [Key] public int Id { get; set; } public List<AocTag> Tags { get; set; } = new(); }
    [Table("AocTag")] public class AocTag { [Key] public int Id { get; set; } public int Weight { get; set; } }

    private static DbContext BootstrapM2m(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE AocPost (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE AocTag (Id INTEGER PRIMARY KEY, Weight INTEGER NOT NULL);" +
                "CREATE TABLE AocPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);" +
                "INSERT INTO AocPost VALUES (1);" +
                "INSERT INTO AocTag VALUES (1,1005),(2,1007);" +   // model 5,7
                "INSERT INTO AocPostTag VALUES (1,1),(1,2);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Post>().HasMany(p => p.Tags).WithMany().UsingTable("AocPostTag", "PostId", "TagId");
                mb.Entity<AocTag>().Property<int>(t => t.Weight).HasConversion(new OffsetConverter());
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void M2m_collection_Sum_over_converter_column_fails_loud()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = BootstrapM2m(cn);
        // Sum of stored (1005+1007=2012) is silently wrong for model Sum (12) -> must fail loud, like the
        // relation nav path, rather than return the stored aggregate.
        Assert.Throws<NormUnsupportedFeatureException>(
            () => ctx.Query<Post>().Select(p => p.Tags.Sum(t => t.Weight)).ToList());
    }
}
