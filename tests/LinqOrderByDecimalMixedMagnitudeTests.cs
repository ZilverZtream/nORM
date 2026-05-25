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
/// Strict pin exposing whether <c>OrderBy(p =&gt; p.DecimalCol)</c> on
/// mixed-magnitude values lex-orders ('10.5' before '2' because
/// '1' &lt; '2') or numeric-orders correctly. Sister to the WHERE
/// fix in 8d795f4.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByDecimalMixedMagnitudeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OdmmItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO OdmmItem VALUES
                (1, '10.5'),
                (2, '2.0'),
                (3, '1.5'),
                (4, '100.0'),
                (5, '5.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<OdmmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task OrderBy_decimal_column_ascending_returns_numerically_ordered_rows()
    {
        var ids = await _ctx.Query<OdmmItem>()
            .OrderBy(p => p.V)
            .Select(p => new { p.Id })
            .ToListAsync();
        // Numeric order: 1.5, 2.0, 5.0, 10.5, 100.0 -> ids 3,2,5,1,4
        Assert.Equal(new[] { 3, 2, 5, 1, 4 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("OdmmItem")]
    public sealed class OdmmItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
