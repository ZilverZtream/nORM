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
/// Strict pin + implement-first for <c>string.IsNullOrEmpty</c> /
/// <c>IsNullOrWhiteSpace</c> in projection. These work in Where via
/// ExpressionToSqlVisitor's inline emission at ~line 1166 but were
/// missing from the provider's projection path. Same parity-gap shape
/// as d1e9fc5's char.IsDigit/get_Chars fix.
///
/// Silent-wrongness shapes:
///   * IsNullOrEmpty collapsing to (col IS NULL) -> empty strings count
///     as not-empty -> wrong bool per row.
///   * IsNullOrWhiteSpace collapsing to IsNullOrEmpty -> tab/space-only
///     rows misclassified.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringIsNullOrEmptyTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PineItem (Id INTEGER PRIMARY KEY, Name TEXT NULL);
            INSERT INTO PineItem VALUES
                (1, NULL),
                (2, ''),
                (3, '   '),
                (4, 'real');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PineItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_IsNullOrEmpty_projects_bool_per_row()
    {
        var result = await _ctx.Query<PineItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, B = string.IsNullOrEmpty(p.Name) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.True(result[0].B);   // NULL
        Assert.True(result[1].B);   // ''
        Assert.False(result[2].B);  // '   '
        Assert.False(result[3].B);  // 'real'
    }

    [Fact]
    public async Task Select_string_IsNullOrWhiteSpace_projects_bool_per_row()
    {
        // Silent-wrongness: collapsing to IsNullOrEmpty would return
        // {true, true, false, false} -- the whitespace-only row (Id 3)
        // would be misclassified.
        var result = await _ctx.Query<PineItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, B = string.IsNullOrWhiteSpace(p.Name) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.True(result[0].B);   // NULL
        Assert.True(result[1].B);   // ''
        Assert.True(result[2].B);   // '   ' (whitespace-only)
        Assert.False(result[3].B);  // 'real'
    }

    [Table("PineItem")]
    public sealed class PineItem
    {
        [Key] public int Id { get; set; }
        public string? Name { get; set; }
    }
}
