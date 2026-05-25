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
/// Strict pin for <c>char.IsSymbol</c> in projection AND Where. Sister to
/// 5bb7520's IsPunctuation. .NET treats <c>$ + &lt; = &gt; ^ ` | ~</c> as
/// Symbols (distinct from Punctuation). SQLite emission uses unicode()
/// codepoint comparisons.
///
/// Silent-wrongness shape:
///   * IsSymbol collapsing to IsPunctuation -> returns true for '!' / '.'
///     etc. instead of the symbol set.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionCharIsSymbolTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PcisItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PcisItem VALUES
                (1, '$100'),
                (2, '=eq'),
                (3, '+inc'),
                (4, '!alert'),
                (5, 'plain');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PcisItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_char_IsSymbol_first_char_returns_true_only_for_symbol_rows()
    {
        var result = await _ctx.Query<PcisItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = char.IsSymbol(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.True(result[0].S);   // '$'
        Assert.True(result[1].S);   // '='
        Assert.True(result[2].S);   // '+'
        Assert.False(result[3].S);  // '!' (punctuation, not symbol)
        Assert.False(result[4].S);  // 'p'
    }

    [Fact]
    public async Task Where_char_IsSymbol_first_char_filters_symbol_rows()
    {
        var result = await _ctx.Query<PcisItem>()
            .Where(p => char.IsSymbol(p.Name[0]))
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PcisItem")]
    public sealed class PcisItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
