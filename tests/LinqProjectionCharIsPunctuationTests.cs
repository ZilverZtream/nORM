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
/// Strict pin + implement-first for <c>char.IsPunctuation</c> in projection
/// and Where. Sister to the existing char.IsDigit/IsLetter/IsWhiteSpace/
/// IsUpper/IsLower set. Common pattern: validate or filter rows whose
/// leading character is punctuation (e.g. "is this an inline-quote line?").
///
/// ASCII punctuation per Unicode: ! " # % &amp; ' ( ) * , - . / : ; ? @ [ \\ ] _
/// { } -- the printable-ASCII punctuation block. We don't try to match the
/// full Unicode definition (P* category) since no provider can express it.
///
/// Silent-wrongness shape:
///   * Untranslated -> client-eval / throw / 'no such function: ISPUNCTUATION'.
///   * Collapsing to IsLetter -> returns true for letter rows too.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionCharIsPunctuationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PcipItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PcipItem VALUES
                (1, '!hi'),
                (2, '?ok'),
                (3, '.csv'),
                (4, 'plain'),
                (5, '7seven');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PcipItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_char_IsPunctuation_first_char_returns_true_only_for_punctuation_rows()
    {
        var result = await _ctx.Query<PcipItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, P = char.IsPunctuation(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.True(result[0].P);   // '!'
        Assert.True(result[1].P);   // '?'
        Assert.True(result[2].P);   // '.'
        Assert.False(result[3].P);  // 'p'
        Assert.False(result[4].P);  // '7'
    }

    [Fact]
    public async Task Where_char_IsPunctuation_first_char_filters_punctuation_rows()
    {
        var result = await _ctx.Query<PcipItem>()
            .Where(p => char.IsPunctuation(p.Name[0]))
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PcipItem")]
    public sealed class PcipItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
