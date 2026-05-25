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
/// Probe pin for `Where(p =&gt; p.D &gt; new DateTime(2020,1,1))`. Sister of
/// the SCV constructor-literal fix (982fd9d) -- ETSV (the WHERE-side
/// visitor) likely has the same NewExpression gap that fell through to
/// malformed SQL. Verify the WHERE path also constant-folds embedded
/// DateTime constructor literals.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeLiteralTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdlItem (Id INTEGER PRIMARY KEY, D TEXT NOT NULL);
            INSERT INTO WdlItem VALUES
                (1, '2010-06-15 00:00:00'),
                (2, '2025-04-01 00:00:00'),
                (3, '2019-12-31 00:00:00'),
                (4, '2021-07-04 00:00:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdlItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_decimal_column_greater_than_DateTime_literal_filters_numerically()
    {
        var rows = await _ctx.Query<WdlItem>()
            .Where(p => p.D > new DateTime(2020, 1, 1))
            .OrderBy(p => p.Id)
            .ToListAsync();
        // Rows after 2020-01-01: Id 2 (2025), Id 4 (2021).
        Assert.Equal(2, rows.Count);
        Assert.Equal(2, rows[0].Id);
        Assert.Equal(4, rows[1].Id);
    }

    [Table("WdlItem")]
    public sealed class WdlItem
    {
        [Key] public int Id { get; set; }
        public DateTime D { get; set; }
    }
}
