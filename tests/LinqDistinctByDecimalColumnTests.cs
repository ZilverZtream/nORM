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
/// Probe pin for DistinctBy on a decimal column. Sister of the DISTINCT
/// fix (e59e44f) -- DistinctBy has a separate translator entry; the
/// dedup-key emit path may not coerce decimal-typed key columns,
/// producing lex-based dedup ('10.5' != '10.50') instead of numeric.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctByDecimalColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DbdcItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO DbdcItem VALUES
                (1, '10.5',   'a'),
                (2, '10.50',  'b'),
                (3, '10.500', 'c'),
                (4, '2.0',    'd'),
                (5, '2.00',   'e');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<DbdcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task DistinctBy_decimal_key_dedups_numerically_not_lexically()
    {
        var rows = await _ctx.Query<DbdcItem>()
            .OrderBy(p => p.Id)
            .DistinctBy(p => p.V)
            .ToListAsync();
        // Numeric distinct keys: 10.5, 2.0 -> 2 rows.
        // Lex distinct keys (bug): 10.5, 10.50, 10.500, 2.0, 2.00 -> 5 rows.
        Assert.Equal(2, rows.Count);
    }

    [Table("DbdcItem")]
    public sealed class DbdcItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
