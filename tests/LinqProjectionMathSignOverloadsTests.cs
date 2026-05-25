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
/// Strict pins for <c>Math.Sign</c> across overloads. The double overload
/// has been mapped for a while via the typeof(Math) switch, but the int /
/// long / decimal sister overloads share the method name and same arity
/// so they all funnel into the same provider entry -- this pin guards
/// against future regressions from per-overload analyzer-list churn or
/// arity gate tightening, and ensures the int/long/decimal admission
/// works end-to-end (analyzer name-based + ETSV declType-based +
/// provider arity-only).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathSignOverloadsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmsiItem (
                Id INTEGER PRIMARY KEY,
                Iv INTEGER NOT NULL,
                Lv INTEGER NOT NULL,
                Dv REAL NOT NULL,
                Mv TEXT NOT NULL
            );
            INSERT INTO PmsiItem VALUES
                (1,  5,   1000000000000,  3.5,  '2.5'),
                (2,  0,   0,              0.0,  '0'),
                (3, -7,  -2000000000000, -1.25, '-9.99');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmsiItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Sign_int_column_projects_sign_per_row()
    {
        var r = await _ctx.Query<PmsiItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = Math.Sign(p.Iv) }).ToListAsync();
        Assert.Equal(new[] { 1, 0, -1 }, r.Select(x => x.S).ToArray());
    }

    [Fact]
    public async Task Select_Math_Sign_long_column_projects_sign_per_row()
    {
        var r = await _ctx.Query<PmsiItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = Math.Sign(p.Lv) }).ToListAsync();
        Assert.Equal(new[] { 1, 0, -1 }, r.Select(x => x.S).ToArray());
    }

    [Fact]
    public async Task Select_Math_Sign_double_column_projects_sign_per_row()
    {
        var r = await _ctx.Query<PmsiItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = Math.Sign(p.Dv) }).ToListAsync();
        Assert.Equal(new[] { 1, 0, -1 }, r.Select(x => x.S).ToArray());
    }

    [Fact]
    public async Task Select_Math_Sign_decimal_column_projects_sign_per_row()
    {
        var r = await _ctx.Query<PmsiItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = Math.Sign(p.Mv) }).ToListAsync();
        Assert.Equal(new[] { 1, 0, -1 }, r.Select(x => x.S).ToArray());
    }

    [Table("PmsiItem")]
    public sealed class PmsiItem
    {
        [Key] public int Id { get; set; }
        public int Iv { get; set; }
        public long Lv { get; set; }
        public double Dv { get; set; }
        public decimal Mv { get; set; }
    }
}
