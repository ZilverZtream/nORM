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
/// Strict pins for <c>decimal.Round</c> overloads. Direct sister to the
/// Math.Round fix: .NET defaults to ToEven (banker's) for all decimal
/// overloads, but SQLite's native ROUND is round-half-away-from-zero, so
/// a naive emit silently disagrees on every exact-half tie. The
/// 2-arg overload (decimal, MidpointRounding) also needs overload-aware
/// dispatch since it shares arity with (decimal, int) -- the same trap
/// Math.Round hit.
///
/// Note: Microsoft.Data.Sqlite stores decimal as TEXT and binds via
/// CultureInfo.InvariantCulture; SQLite's REAL functions operate on the
/// numeric value once the text is interpreted. Round-trip precision is
/// sufficient for the small fractions exercised here.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDecimalRoundModeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdrmItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO PdrmItem VALUES
                (1, '0.5'),
                (2, '1.5'),
                (3, '2.5'),
                (4, '3.5'),
                (5, '-2.5'),
                (6, '1.2');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdrmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_decimal_Round_default_uses_banker_rounding_to_even()
    {
        var r = await _ctx.Query<PdrmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = decimal.Round(p.V) }).ToListAsync();
        Assert.Equal(6, r.Count);
        Assert.Equal(0m, r[0].R);   // 0.5 -> 0 (banker's)
        Assert.Equal(2m, r[1].R);   // 1.5 -> 2
        Assert.Equal(2m, r[2].R);   // 2.5 -> 2 (banker's)
        Assert.Equal(4m, r[3].R);   // 3.5 -> 4
        Assert.Equal(-2m, r[4].R);  // -2.5 -> -2 (banker's)
        Assert.Equal(1m, r[5].R);   // 1.2 -> 1
    }

    [Fact]
    public async Task Select_decimal_Round_with_MidpointRounding_AwayFromZero_matches_dotnet()
    {
        var r = await _ctx.Query<PdrmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = decimal.Round(p.V, MidpointRounding.AwayFromZero) }).ToListAsync();
        Assert.Equal(6, r.Count);
        Assert.Equal(1m, r[0].R);    // 0.5 -> 1
        Assert.Equal(2m, r[1].R);    // 1.5 -> 2
        Assert.Equal(3m, r[2].R);    // 2.5 -> 3
        Assert.Equal(4m, r[3].R);    // 3.5 -> 4
        Assert.Equal(-3m, r[4].R);   // -2.5 -> -3
        Assert.Equal(1m, r[5].R);    // 1.2 -> 1
    }

    [Table("PdrmItem")]
    public sealed class PdrmItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
