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
/// Probe pin for decimal column compared to a decimal literal IN A
/// PROJECTION (boolean result column). The ETSV WHERE-side already
/// CASTs both sides AS REAL (8d795f4); this verifies whether SCV does
/// the same when the comparison appears as a projection field.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDecimalCompareBooleanTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdcbItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO PdcbItem VALUES
                (1, '10.5'),
                (2, '2.0'),
                (3, '1.5'),
                (4, '100.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdcbItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_decimal_column_greater_than_literal_returns_correct_boolean_per_row()
    {
        var r = await _ctx.Query<PdcbItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, Big = p.V > 2m }).ToListAsync();
        Assert.Equal(4, r.Count);
        Assert.True(r[0].Big);     // 10.5 > 2
        Assert.False(r[1].Big);    // 2.0 > 2 is false
        Assert.False(r[2].Big);    // 1.5 > 2
        Assert.True(r[3].Big);     // 100.0 > 2
    }

    [Table("PdcbItem")]
    public sealed class PdcbItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
