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
/// Strict pin + implement-first for <c>string.Trim(params char[])</c>
/// overload in projection. The no-arg form (default whitespace trim)
/// already works; this is the explicit-char-set variant -- e.g.
/// <c>name.Trim('!', '?')</c> strips leading/trailing '!' and '?'
/// chars (any combination).
///
/// SQLite TRIM(s, t) accepts a string of chars to trim. Construct the
/// chars string at translation time from the constant char[] array.
///
/// Silent-wrongness shape:
///   * Untranslated -> client-eval / throw.
///   * Char-array silently dropped -> default whitespace trim applied
///     instead -- leading '!' and '?' remain.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringTrimCharArrayTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PstcaItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PstcaItem VALUES
                (1, '!hi!'),
                (2, '?ok?'),
                (3, '?!!both!?'),
                (4, 'plain');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PstcaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Trim_with_explicit_char_set_strips_matching_leading_trailing_chars()
    {
        var result = await _ctx.Query<PstcaItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.Trim('!', '?'))
            .ToListAsync();
        Assert.Equal(new[] { "hi", "ok", "both", "plain" }, result.ToArray());
    }

    [Table("PstcaItem")]
    public sealed class PstcaItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
