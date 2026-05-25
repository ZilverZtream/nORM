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
/// Pin for <c>Math.Min/Math.Max</c> on decimal columns in projection.
/// Decimal columns store as TEXT; SQLite MIN/MAX scalar functions
/// compare using the values' storage classes. Probe whether the
/// per-row pairing returns correct results.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathMinMaxDecimalTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmmmdItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO PmmmdItem VALUES
                (1, '3.5', '7.5'),
                (2, '10.0', '5.5'),
                (3, '-1.25', '0.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PmmmdItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Math_Min_decimal_columns_returns_lower_per_row()
    {
        var r = await _ctx.Query<PmmmdItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Min(p.A, p.B) }).ToListAsync();
        Assert.Equal(new[] { 3.5m, 5.5m, -1.25m }, r.Select(x => x.V).ToArray());
    }

    [Fact]
    public async Task Select_Math_Max_decimal_columns_returns_higher_per_row()
    {
        var r = await _ctx.Query<PmmmdItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = Math.Max(p.A, p.B) }).ToListAsync();
        Assert.Equal(new[] { 7.5m, 10.0m, 0.0m }, r.Select(x => x.V).ToArray());
    }

    [Table("PmmmdItem")]
    public sealed class PmmmdItem
    {
        [Key] public int Id { get; set; }
        public decimal A { get; set; }
        public decimal B { get; set; }
    }
}
