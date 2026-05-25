using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Probe pin for <c>Math.Round(decimalCol)</c> and
/// <c>Math.Round(decimalCol, digits)</c> in projection. Decimal column
/// stores as TEXT; the banker's-rounding CASE emit (928460f) uses ABS
/// / CAST AS INTEGER on the raw column, so values like '10.5' (lex >
/// '2') need to round numerically not lexicographically.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathRoundDecimalTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmrdItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO PmrdItem VALUES
                (1, '10.5'),   -- banker's round to even -> 10
                (2, '2.5'),    -- banker's round to even -> 2
                (3, '100.7');  -- round up -> 101
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmrdItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Round_decimal_column_no_digits_uses_banker_rounding_per_row()
    {
        var r = await _ctx.Query<PmrdItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Round(p.V) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(10m, r[0].R);   // 10.5 -> 10 (banker's)
        Assert.Equal(2m, r[1].R);    // 2.5 -> 2 (banker's)
        Assert.Equal(101m, r[2].R);  // 100.7 -> 101
    }

    [Table("PmrdItem")]
    public sealed class PmrdItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
