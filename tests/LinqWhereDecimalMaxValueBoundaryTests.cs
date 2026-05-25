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
/// Audit probe: the decimal-cluster bilateral CAST AS REAL wrap in ETSV
/// (8d795f4) wraps BOTH operands of a decimal comparison. That worked for
/// decimal because CAST(text AS REAL) on extreme values loses precision
/// but doesn't return NULL/empty (unlike SQLite's datetime() which
/// returns empty for DateTime.MaxValue.9999999). This probe verifies the
/// bilateral wrap on decimal.MaxValue does NOT regress the comparison.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDecimalMaxValueBoundaryTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdmvbItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO WdmvbItem VALUES (1, '1.0'), (2, '100.0'), (3, '1000000.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdmvbItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_decimal_column_less_than_decimal_MaxValue_returns_all_rows()
    {
        // Bilateral CAST AS REAL on decimal.MaxValue (7.9e28) loses precision
        // to double range but still produces a finite double > any column value.
        var ids = await _ctx.Query<WdmvbItem>()
            .Where(p => p.V < decimal.MaxValue)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, ids.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Where_decimal_column_greater_than_decimal_MinValue_returns_all_rows()
    {
        var ids = await _ctx.Query<WdmvbItem>()
            .Where(p => p.V > decimal.MinValue)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WdmvbItem")]
    public sealed class WdmvbItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
