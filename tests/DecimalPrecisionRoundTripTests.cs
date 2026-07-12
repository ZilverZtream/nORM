using System;
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
/// A decimal written and read back must be bit-exact. SQLite has no native DECIMAL type, so a naive
/// mapping to REAL (IEEE-754 double) silently loses precision on high-scale or large-magnitude
/// values — data corruption. This verifies the stored representation round-trips exactly.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DecimalPrecisionRoundTripTests
{
    [Table("DecItem")]
    private class DecItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public decimal Value { get; set; }
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DecItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Theory]
    [InlineData("0.1")]
    [InlineData("0.3")]
    [InlineData("123456789012345.678901234")]     // 24 significant digits — exceeds double's 15-17
    [InlineData("79228162514264337593543950335")]  // decimal.MaxValue
    [InlineData("-0.0000000001")]
    [InlineData("0.10000000000000001")]            // distinct from 0.1 only beyond double precision
    public async Task Decimal_round_trips_exactly(string literal)
    {
        var value = decimal.Parse(literal, System.Globalization.CultureInfo.InvariantCulture);

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DecItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var item = new DecItem { Value = value };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        var loaded = await ctx.Query<DecItem>().Where(d => d.Id == item.Id).FirstAsync();
        Assert.Equal(value, loaded.Value);
    }

    [Fact]
    public async Task Decimal_sum_of_tenths_is_exact()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DecItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Value TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        // 0.1 + 0.1 + 0.1 = 0.3 exactly in decimal (0.30000000000000004 in double).
        for (int i = 0; i < 3; i++) ctx.Add(new DecItem { Value = 0.1m });
        await ctx.SaveChangesAsync();

        var rows = await ctx.Query<DecItem>().ToListAsync();
        Assert.Equal(0.3m, rows.Sum(r => r.Value));
    }
}
