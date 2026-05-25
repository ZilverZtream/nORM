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
/// Strict pin for <c>decimal.Compare(a, b)</c> in WHERE -- sister to the
/// projection fix in e975313 and the DateTime/string/TimeSpan WHERE
/// mirrors. Real-world filter shape:
///   Where(p =&gt; decimal.Compare(p.A, p.B) &gt; 0)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDecimalCompareTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdcItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO WdcItem VALUES
                (1, '5.0', '3.0'),
                (2, '4.0', '4.0'),
                (3, '-1.0', '7.5');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_decimal_Compare_greater_than_zero_returns_only_a_greater_rows()
    {
        var ids = await _ctx.Query<WdcItem>()
            .Where(p => decimal.Compare(p.A, p.B) > 0)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1 }, ids.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Where_decimal_Compare_equals_zero_returns_only_equal_rows()
    {
        var ids = await _ctx.Query<WdcItem>()
            .Where(p => decimal.Compare(p.A, p.B) == 0)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 2 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WdcItem")]
    public sealed class WdcItem
    {
        [Key] public int Id { get; set; }
        public decimal A { get; set; }
        public decimal B { get; set; }
    }
}
