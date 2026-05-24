using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins high-precision <see cref="decimal"/> round-trip. SQLite stores decimals
/// as TEXT to preserve full precision; the materializer must read them back
/// without losing scale digits, and equality predicates against a high-precision
/// constant must round-trip cleanly. A REAL-coerced storage would lose
/// significant digits in the 20+ digit range — this test would catch that
/// regression.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDecimalPrecisionColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DecRow (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL);
            INSERT INTO DecRow VALUES
                (1, '1234567890.123456789012345'),
                (2, '0.0000000000000001'),
                (3, '79228162514264337593543950335'),  -- decimal.MaxValue
                (4, '-79228162514264337593543950335'); -- decimal.MinValue
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task High_precision_decimal_round_trips_without_losing_digits()
    {
        var row = (await _ctx.Query<DecRow>().Where(r => r.Id == 1).ToListAsync())[0];
        // Mid-magnitude value with 15 fractional digits — sits comfortably inside decimal's
        // 28–29 significant-digit precision so a clean round-trip is reasonable.
        Assert.Equal(1234567890.123456789012345m, row.Amount);
    }

    [Fact]
    public async Task Tiny_decimal_round_trips_without_underflow_to_zero()
    {
        var row = (await _ctx.Query<DecRow>().Where(r => r.Id == 2).ToListAsync())[0];
        // 16 leading zeros after the decimal point — would underflow if the storage path
        // coerced through double (only ~15 significant digits of precision).
        Assert.Equal(0.0000000000000001m, row.Amount);
    }

    [Fact]
    public async Task Decimal_boundary_values_round_trip_through_materializer()
    {
        var rows = (await _ctx.Query<DecRow>()
            .Where(r => r.Id == 3 || r.Id == 4)
            .OrderBy(r => r.Id)
            .ToListAsync())
            .ToArray();
        Assert.Equal(decimal.MaxValue, rows[0].Amount);
        Assert.Equal(decimal.MinValue, rows[1].Amount);
    }

    [Table("DecRow")]
    public sealed class DecRow
    {
        [Key] public int Id { get; set; }
        public decimal Amount { get; set; }
    }
}
