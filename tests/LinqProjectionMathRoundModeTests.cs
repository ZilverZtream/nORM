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
/// Strict pins for <c>Math.Round(double, MidpointRounding)</c> and
/// <c>Math.Round(double, int, MidpointRounding)</c>. .NET defaults to
/// banker's rounding (ToEven); SQLite's native ROUND() rounds half-
/// away-from-zero. The plain 1- and 2-arg Math.Round overloads use
/// the .NET default (ToEven), so a naive ROUND emit disagrees at
/// every exact-half tie -- that's a silent-wrongness bug.
///
/// Strict pin covers both ToEven (banker's, the .NET default) and
/// AwayFromZero (matches SQLite's native semantics).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathRoundModeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmrmItem (Id INTEGER PRIMARY KEY, V REAL NOT NULL);
            INSERT INTO PmrmItem VALUES
                (1, 0.5),    -- ToEven->0; AwayFromZero->1
                (2, 1.5),    -- ToEven->2; AwayFromZero->2
                (3, 2.5),    -- ToEven->2; AwayFromZero->3
                (4, 3.5),    -- ToEven->4; AwayFromZero->4
                (5, -2.5),   -- ToEven->-2; AwayFromZero->-3
                (6, 1.2);    -- both round to 1
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmrmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Round_default_uses_banker_rounding_to_even()
    {
        // Math.Round(v) -- 1-arg overload defaults to ToEven.
        var r = await _ctx.Query<PmrmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Round(p.V) }).ToListAsync();
        Assert.Equal(6, r.Count);
        Assert.Equal(Math.Round(0.5), r[0].R, 6);   // 0
        Assert.Equal(Math.Round(1.5), r[1].R, 6);   // 2
        Assert.Equal(Math.Round(2.5), r[2].R, 6);   // 2 (banker's)
        Assert.Equal(Math.Round(3.5), r[3].R, 6);   // 4
        Assert.Equal(Math.Round(-2.5), r[4].R, 6);  // -2 (banker's)
        Assert.Equal(Math.Round(1.2), r[5].R, 6);   // 1
    }

    [Fact]
    public async Task Select_Math_Round_with_MidpointRounding_AwayFromZero_matches_dotnet()
    {
        var r = await _ctx.Query<PmrmItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, R = Math.Round(p.V, MidpointRounding.AwayFromZero) }).ToListAsync();
        Assert.Equal(6, r.Count);
        Assert.Equal(1.0, r[0].R, 6);   // 0.5 -> 1
        Assert.Equal(2.0, r[1].R, 6);   // 1.5 -> 2
        Assert.Equal(3.0, r[2].R, 6);   // 2.5 -> 3
        Assert.Equal(4.0, r[3].R, 6);   // 3.5 -> 4
        Assert.Equal(-3.0, r[4].R, 6);  // -2.5 -> -3
        Assert.Equal(1.0, r[5].R, 6);   // 1.2 -> 1
    }

    [Table("PmrmItem")]
    public sealed class PmrmItem
    {
        [Key] public int Id { get; set; }
        public double V { get; set; }
    }
}
