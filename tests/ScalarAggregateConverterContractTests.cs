using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Pins that a scalar Min/Max over a value-converter column returns the MODEL value. Min/Max read an actual
/// stored column value, so the result must be run back through the converter (EF applies ConvertFromProvider);
/// otherwise Max(o =&gt; o.Score) returns the stored provider value. Uses an ORDER-PRESERVING converter so the
/// server-side extremum picks the right row and only the value conversion is under test. Sum is left
/// unconverted (it aggregates across rows and does not correspond to a single stored value).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ScalarAggregateConverterContractTests
{
    [Table("SacOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
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
                CREATE TABLE SacOrder (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);
                INSERT INTO SacOrder VALUES (1,1005),(2,1007),(3,1009);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Order>().Property<int>(o => o.Score).HasConversion(new OffsetConverter());
        }};
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Max_over_converter_column_returns_model_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(9, await ctx.Query<Order>().MaxAsync(o => o.Score));   // stored 1009 → model 9
    }

    [Fact]
    public async Task Min_over_converter_column_returns_model_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(5, await ctx.Query<Order>().MinAsync(o => o.Score));   // stored 1005 → model 5
    }

    [Fact]
    public void Sync_max_over_converter_column_returns_model_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(9, ctx.Query<Order>().Max(o => o.Score));
    }
}
