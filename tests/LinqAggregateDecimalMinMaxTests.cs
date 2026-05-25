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
/// Strict pin exposing the silent-wrongness in <c>MinAsync</c> /
/// <c>MaxAsync</c> aggregates over a decimal (TEXT-stored) column with
/// mixed-magnitude values. SQL MIN/MAX as aggregate functions inherit
/// the column's storage class -- TEXT -- and compare lex, so '10.5'
/// lex-orders below '2.0'.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAggregateDecimalMinMaxTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AdmmItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO AdmmItem VALUES
                (1, '10.5'),
                (2, '2.0'),
                (3, '1.5'),
                (4, '100.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<AdmmItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task MinAsync_decimal_column_returns_numerically_minimum_value()
    {
        var v = await _ctx.Query<AdmmItem>().MinAsync(p => p.V);
        Assert.Equal(1.5m, v);
    }

    [Fact]
    public async Task MaxAsync_decimal_column_returns_numerically_maximum_value()
    {
        var v = await _ctx.Query<AdmmItem>().MaxAsync(p => p.V);
        Assert.Equal(100.0m, v);
    }

    [Table("AdmmItem")]
    public sealed class AdmmItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
