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
/// Probe to isolate where decimal-column WHERE comparisons go wrong --
/// is it the bare `col > literal` shape or only the `ABS(col) > literal`
/// shape from dea27c8?
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDecimalCompareLiteralTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdclItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO WdclItem VALUES
                (1, '2.7'),
                (2, '0.5'),
                (3, '-3.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdclItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_bare_decimal_column_greater_than_literal_filters_correctly()
    {
        var ids = await _ctx.Query<WdclItem>()
            .Where(p => p.V > 1m)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WdclItem")]
    public sealed class WdclItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
