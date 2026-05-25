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
/// Verify pin for <c>Math.PI / Math.Tau / Math.E</c> constants in
/// projection. These are static fields that closure-fold to a double
/// literal at translation time and emit via SCV.FormatLiteral.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathPiTauETests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmpteItem (Id INTEGER PRIMARY KEY, V REAL NOT NULL);
            INSERT INTO PmpteItem VALUES (1, 2.0), (2, 1.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmpteItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_PI_times_column_returns_correct_per_row()
    {
        var r = await _ctx.Query<PmpteItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = p.V * Math.PI }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(2.0 * Math.PI, r[0].V, 6);
        Assert.Equal(1.0 * Math.PI, r[1].V, 6);
    }

    [Fact]
    public async Task Select_Math_Tau_times_column_returns_correct_per_row()
    {
        var r = await _ctx.Query<PmpteItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = p.V * Math.Tau }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(2.0 * Math.Tau, r[0].V, 6);
        Assert.Equal(1.0 * Math.Tau, r[1].V, 6);
    }

    [Fact]
    public async Task Select_Math_E_times_column_returns_correct_per_row()
    {
        var r = await _ctx.Query<PmpteItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = p.V * Math.E }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(2.0 * Math.E, r[0].V, 6);
        Assert.Equal(1.0 * Math.E, r[1].V, 6);
    }

    [Table("PmpteItem")]
    public sealed class PmpteItem
    {
        [Key] public int Id { get; set; }
        public double V { get; set; }
    }
}
