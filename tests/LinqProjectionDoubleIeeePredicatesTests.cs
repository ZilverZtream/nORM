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
/// Strict pins for IEEE 754 predicates in projection:
/// <c>double.IsNaN / double.IsInfinity / double.IsFinite</c>. SQLite stores
/// REAL as IEEE 754 doubles and supports the literal <c>1e999</c> as +Inf
/// (out-of-range double literals parse to +/-Infinity).
///
/// SQL idioms:
///   IsNaN(x)      : (x != x)                            -- NaN is the only IEEE value that isn't equal to itself.
///   IsInfinity(x) : (ABS(x) = 1e999)                    -- ABS strips sign; compare against the +Inf literal.
///   IsFinite(x)   : (x = x AND ABS(x) != 1e999)         -- not NaN AND not +/-Inf.
///
/// Microsoft.Data.Sqlite explicitly refuses to bind double.NaN at the
/// parameter layer ("Cannot store 'NaN' values"), and the same guard
/// catches NaN returned from UDFs -- so a NaN row cannot be physically
/// inserted via this driver. The IsNaN pin therefore only verifies the
/// false case against finite + +/-Inf rows, which is sufficient to
/// exercise the emit shape (x != x evaluating to false for ordinary
/// values, including infinities which are equal to themselves).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDoubleIeeePredicatesTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdipItem (Id INTEGER PRIMARY KEY, X REAL NOT NULL);
            INSERT INTO PdipItem VALUES
                (1, 1.5),
                (2, 1e999),
                (3, -1e999);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdipItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_double_IsNaN_returns_false_for_finite_and_infinity_rows()
    {
        var r = await _ctx.Query<PdipItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, F = double.IsNaN(p.X) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.False(r[0].F);  // 1.5
        Assert.False(r[1].F);  // +Inf (Inf == Inf in IEEE)
        Assert.False(r[2].F);  // -Inf
    }

    [Fact]
    public async Task Select_double_IsInfinity_flags_both_infinities()
    {
        var r = await _ctx.Query<PdipItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, F = double.IsInfinity(p.X) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.False(r[0].F);  // 1.5
        Assert.True(r[1].F);   // +Inf
        Assert.True(r[2].F);   // -Inf
    }

    [Fact]
    public async Task Select_double_IsFinite_flags_only_finite_row()
    {
        var r = await _ctx.Query<PdipItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, F = double.IsFinite(p.X) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.True(r[0].F);   // 1.5
        Assert.False(r[1].F);  // +Inf
        Assert.False(r[2].F);  // -Inf
    }

    [Table("PdipItem")]
    public sealed class PdipItem
    {
        [Key] public int Id { get; set; }
        public double X { get; set; }
    }
}
