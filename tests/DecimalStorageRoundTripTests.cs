using System;
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
/// Money-safety pin: decimals must round-trip write→read EXACTLY. On SQLite the
/// storage path is invariant-culture TEXT in both directions — the approximate
/// REAL coercion documented for SQL-side comparisons and aggregates must never
/// touch stored values. Boundary values cover cents, 28-significant-digit
/// precision, maximum scale, and negatives.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class DecimalStorageRoundTripTests
{
    [Table("DecRtItem")]
    private class DecRtItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public decimal Amount { get; set; }
    }

    public static TheoryData<decimal> BoundaryValues => new()
    {
        0m,
        0.01m,
        -0.01m,
        1234567890123456789012345678m,        // 28 significant digits
        -1234567890123456789012345678m,
        0.0000000000000000000000000001m,      // maximum scale (28)
        79228162514264337593543950335m,       // decimal.MaxValue
        -79228162514264337593543950335m,      // decimal.MinValue
        19.99m,
        123456.789012m,
        0.1m + 0.2m,                          // 0.3 exactly in decimal, not in double
    };

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE DecRtItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Amount TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<DecRtItem>() };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Theory]
    [MemberData(nameof(BoundaryValues))]
    public async Task Decimal_round_trips_exactly_through_insert_and_query(decimal value)
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        ctx.Add(new DecRtItem { Amount = value });
        await ctx.SaveChangesAsync();

        // Fresh context so the value comes from storage, not the identity map.
        await using var fresh = new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { OnModelCreating = mb => mb.Entity<DecRtItem>() });
        var read = await fresh.Query<DecRtItem>().FirstAsync();

        Assert.Equal(value, read.Amount);
    }

    [Fact]
    public async Task Decimal_round_trips_exactly_through_update()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var item = new DecRtItem { Amount = 1.00m };
        ctx.Add(item);
        await ctx.SaveChangesAsync();

        item.Amount = 0.0000000000000000000000000001m;
        await ctx.SaveChangesAsync();

        await using var fresh = new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { OnModelCreating = mb => mb.Entity<DecRtItem>() });
        var read = await fresh.Query<DecRtItem>().FirstAsync();
        Assert.Equal(0.0000000000000000000000000001m, read.Amount);
    }

    [Fact]
    public async Task Decimal_parameterized_where_matches_exact_stored_value()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        ctx.Add(new DecRtItem { Amount = 19.99m });
        ctx.Add(new DecRtItem { Amount = 19.990001m });
        await ctx.SaveChangesAsync();

        var price = 19.99m;
        var matches = await ctx.Query<DecRtItem>().Where(x => x.Amount == price).ToListAsync();

        var match = Assert.Single(matches);
        Assert.Equal(19.99m, match.Amount);
    }
}
