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
/// Probe pin for DISTINCT over a decimal column. SQLite stores decimal as
/// TEXT, so '10.5' and '10.50' are distinct strings even though they
/// represent the same decimal value. DISTINCT on the raw column dedups
/// lexically -- after CAST AS REAL it dedups numerically.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctDecimalColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DdcItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO DdcItem VALUES
                (1, '10.5'),
                (2, '10.50'),
                (3, '10.500'),
                (4, '2.0'),
                (5, '2.00'),
                (6, '100.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<DdcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_V_then_Distinct_decimal_dedups_numerically_not_lexically()
    {
        var rows = await _ctx.Query<DdcItem>()
            .Select(p => new { V = p.V })
            .Distinct()
            .OrderBy(r => r.V)
            .ToListAsync();
        var distinctValues = rows.Select(r => r.V).ToList();
        // Numerically distinct: 10.5, 2.0, 100.0 -> 3 values.
        // Lex distinct (bug): 10.5, 10.50, 10.500, 2.0, 2.00, 100.0 -> 6 values.
        Assert.Equal(3, distinctValues.Count);
        Assert.Contains(10.5m, distinctValues);
        Assert.Contains(2.0m, distinctValues);
        Assert.Contains(100.0m, distinctValues);
    }

    [Table("DdcItem")]
    public sealed class DdcItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
