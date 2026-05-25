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
/// Strict pin exposing the silent-wrongness in decimal column WHERE
/// comparisons when values straddle magnitude boundaries. Decimal
/// stored as TEXT; SQLite affinity makes `col &gt; @p0` a lex compare
/// when both are TEXT. '10.5' lex-compares as '1' &lt; '2' so wrongly
/// excluded from `col &gt; 2`.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDecimalMixedMagnitudeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdmmItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO WdmmItem VALUES
                (1, '10.5'),  -- numerically larger than 2; lex starts '1' < '2'
                (2, '5.0'),
                (3, '1.5'),
                (4, '100.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdmmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_decimal_column_greater_than_two_returns_numerically_greater_rows()
    {
        var ids = await _ctx.Query<WdmmItem>()
            .Where(p => p.V > 2m)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 4 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WdmmItem")]
    public sealed class WdmmItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
