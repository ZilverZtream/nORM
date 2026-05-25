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
/// Strict pin for unary bitwise NOT (<c>~x</c>) on integer / enum
/// columns in projection. Common mask-invert shape. SQLite has the
/// `~` operator natively for INTEGER, so the emit is direct.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionBitwiseNotTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PbnItem (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO PbnItem VALUES
                (1, 0),
                (2, 5),
                (3, -1);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PbnItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_bitwise_NOT_int_column_returns_inverted_bits_per_row_baseline()
    {
        // Baseline: confirm 2-column projection without ~ works.
        var b = await _ctx.Query<PbnItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, OrigV = p.V }).ToListAsync();
        Assert.Equal(3, b.Count);
    }

    [Fact]
    public async Task Select_bitwise_NOT_int_column_returns_inverted_bits_per_row()
    {
        var r = await _ctx.Query<PbnItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, N = ~p.V }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(~0, r[0].N);   // -1
        Assert.Equal(~5, r[1].N);   // -6
        Assert.Equal(~-1, r[2].N);  // 0
    }

    [Table("PbnItem")]
    public sealed class PbnItem
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
