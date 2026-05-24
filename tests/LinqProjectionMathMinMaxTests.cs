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
/// Strict pin + implement-first for <c>Math.Min(a, b)</c> and
/// <c>Math.Max(a, b)</c> in projection. SQLite has scalar min(X, Y, ...)
/// and max(X, Y, ...) builtins (the same names work as both aggregate and
/// scalar -- 2+ args -> scalar). Without a provider entry, projection
/// routes through SCV's fallback and emits raw MIN(a, b) which SQLite
/// also parses as a scalar -- so it might actually work even without the
/// provider entry. Probe to find out.
///
/// Silent-wrongness shapes:
///   * Wrong arg order -> Min and Max swap; one returns the larger and the
///     other the smaller.
///   * Single-arg overload silently bound -> Math.Min(int) doesn't exist
///     in .NET, no concern; static-method dispatch on Math is straightforward.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathMinMaxTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmmItem (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL);
            INSERT INTO PmmItem VALUES
                (1, 5, 10),
                (2, 7, 3),
                (3, 0, 0),
                (4, -2, -5);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Min_int_returns_smaller_of_two_columns()
    {
        var result = await _ctx.Query<PmmItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Lo = Math.Min(p.A, p.B) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.Equal(5, result[0].Lo);
        Assert.Equal(3, result[1].Lo);
        Assert.Equal(0, result[2].Lo);
        Assert.Equal(-5, result[3].Lo);
    }

    [Fact]
    public async Task Select_Math_Max_int_returns_larger_of_two_columns()
    {
        var result = await _ctx.Query<PmmItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Hi = Math.Max(p.A, p.B) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.Equal(10, result[0].Hi);
        Assert.Equal(7, result[1].Hi);
        Assert.Equal(0, result[2].Hi);
        Assert.Equal(-2, result[3].Hi);
    }

    [Table("PmmItem")]
    public sealed class PmmItem
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }
}
