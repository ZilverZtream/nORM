using System;
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
/// Pins that a simple root-query predicate on a value-converter column applies the converter. The
/// single-comparison "simple query" fast path bound the RAW model value (`Score = 5` against a stored
/// 1005), silently matching nothing; it now defers converter columns to the full translator. Non-converter
/// simple queries still take the fast path unchanged.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SimpleQueryConverterColumnContractTests
{
    [Table("SqcWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
    }

    // Order-preserving: model N stored as N + 1000.
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SqcWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL);
                INSERT INTO SqcWidget VALUES (1,'a',1005),(2,'b',1007),(3,'c',1009);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Widget>().HasKey(w => w.Id);
            mb.Entity<Widget>().Property<int>(w => w.Score).HasConversion(new OffsetConverter());
        }};
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public void Equality_on_a_converter_column_matches_the_stored_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { 1 }, ctx.Query<Widget>().Where(w => w.Score == 5).ToList().Select(w => w.Id).ToArray());
    }

    [Fact]
    public void Closure_equality_on_a_converter_column_matches()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        var target = 7;
        Assert.Equal(new[] { 2 }, ctx.Query<Widget>().Where(w => w.Score == target).ToList().Select(w => w.Id).ToArray());
    }

    [Fact]
    public void Ordering_on_a_converter_column_matches()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { 2, 3 }, ctx.Query<Widget>().Where(w => w.Score > 6).ToList().OrderBy(w => w.Id).Select(w => w.Id).ToArray());
    }

    [Fact]
    public void Materialized_value_round_trips_through_the_converter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        var w = ctx.Query<Widget>().Where(w => w.Score == 5).ToList().Single();
        Assert.Equal(5, w.Score);   // read back as the model value, not the stored 1005
    }

    [Fact]
    public void Non_converter_simple_query_still_works()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        // Name has no converter → still the fast path, unchanged.
        Assert.Equal(new[] { 2 }, ctx.Query<Widget>().Where(w => w.Name == "b").ToList().Select(w => w.Id).ToArray());
    }
}
