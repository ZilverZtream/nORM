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
/// Probe pin for `Math.Round(decimalCol, digits)` in projection. Verifies
/// the multi-digit Round emit preserves CAST AS REAL semantics on a TEXT-
/// stored decimal column -- without the coercion, lex ordering of mixed-
/// magnitude values produces incorrect partial rounds. Sister to the
/// digits-0 probe in LinqProjectionMathRoundDecimalTests.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqMathRoundDecimalDigitsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        // Values chosen to avoid IEEE-754 binary-representation boundaries
        // (e.g. 100.745 stores as 100.7449999..., which banker's-rounds to
        // 100.74 instead of 100.75 -- the documented REAL-precision tradeoff).
        cmd.CommandText = """
            CREATE TABLE MrddItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO MrddItem VALUES
                (1, '10.234'),   -- round down to 10.23
                (2, '2.567'),    -- round up to 2.57
                (3, '100.999');  -- round up to 101.00
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<MrddItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Round_decimal_column_two_digits_rounds_numerically_per_row()
    {
        var r = await _ctx.Query<MrddItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Round(p.V, 2) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(10.23m, r[0].R, precision: 9);
        Assert.Equal(2.57m,  r[1].R, precision: 9);
        Assert.Equal(101.00m, r[2].R, precision: 9);
    }

    [Table("MrddItem")]
    public sealed class MrddItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
