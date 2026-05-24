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
/// Strict pin + implement-first for <c>char.IsUpper(c)</c> /
/// <c>IsLower(c)</c> in projection AND Where. Companion to the
/// IsDigit/IsLetter/IsWhiteSpace set already wired in d1e9fc5. ASCII-
/// range tests mirror the existing handlers; same provider entry shape.
///
/// Silent-wrongness shapes:
///   * IsUpper collapsing to IsLetter returns true for lowercase too.
///   * IsLower collapsing to IsLetter returns true for uppercase too.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionCharIsUpperIsLowerTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PciuItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PciuItem VALUES
                (1, 'Alpha'),
                (2, 'beta'),
                (3, '7seven'),
                (4, 'GAMMA');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PciuItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_char_IsUpper_first_char_returns_true_only_for_uppercase()
    {
        var result = await _ctx.Query<PciuItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, U = char.IsUpper(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.True(result[0].U);   // 'A'
        Assert.False(result[1].U);  // 'b'
        Assert.False(result[2].U);  // '7'
        Assert.True(result[3].U);   // 'G'
    }

    [Fact]
    public async Task Select_char_IsLower_first_char_returns_true_only_for_lowercase()
    {
        var result = await _ctx.Query<PciuItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, L = char.IsLower(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.False(result[0].L);  // 'A'
        Assert.True(result[1].L);   // 'b'
        Assert.False(result[2].L);  // '7'
        Assert.False(result[3].L);  // 'G'
    }

    [Fact]
    public async Task Where_with_char_IsUpper_filters_first_uppercase_rows()
    {
        var result = await _ctx.Query<PciuItem>()
            .Where(p => char.IsUpper(p.Name[0]))
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PciuItem")]
    public sealed class PciuItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
