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
/// Strict pin for <c>string.Replace</c> in projection. The analyzer admits
/// Replace and SqliteProvider's TranslateFunction routes it to SQLite's
/// REPLACE(s, old, new). Pinning to prevent regressions where the
/// translation drops one of the arguments or escapes the literals
/// incorrectly.
///
/// Silent-wrongness shapes:
///   * Replace's new-string arg dropped -> result returns the source untouched.
///   * Replace's old-string treated as a regex/like pattern -> matches more
///     than the literal occurrences expected.
///   * Replace in Where predicate not symmetric with projection.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringReplaceTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsrItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PsrItem VALUES
                (1, 'foo-bar-baz'),
                (2, 'no-replace-here'),
                (3, 'multiple foo foo foo'),
                (4, '');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PsrItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Replace_substitutes_literal_in_each_row()
    {
        // Replace 'foo' -> 'qux'. Row 1: foo->qux. Row 2: no match. Row 3:
        // multiple matches. Row 4: empty source stays empty.
        var result = await _ctx.Query<PsrItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.Replace("foo", "qux"))
            .ToListAsync();
        Assert.Equal(
            new[] { "qux-bar-baz", "no-replace-here", "multiple qux qux qux", "" },
            result.ToArray());
    }

    [Fact]
    public async Task Where_with_string_Replace_compares_result_to_literal()
    {
        // Where filter on the Replace result. Row 1 Replace('foo', 'qux') =
        // 'qux-bar-baz'; only Id 1 matches.
        var result = await _ctx.Query<PsrItem>()
            .Where(p => p.Name.Replace("foo", "qux") == "qux-bar-baz")
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Select_string_Replace_with_empty_old_string_does_not_explode()
    {
        // SQLite REPLACE(s, '', new) returns the source unchanged (the .NET
        // equivalent throws ArgumentException on empty old-string at runtime
        // -- but the server-side path skips that validation since the SQL
        // function never inspects). Pin the SQLite behavior so a future
        // pre-flight that adds .NET-parity validation surfaces here.
        var result = await _ctx.Query<PsrItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.Replace("", "Z"))
            .ToListAsync();
        // Either SQLite returns rows unchanged (current behavior) or nORM
        // adds a pre-check and throws -- both acceptable. The pin asserts
        // that the row count + at least the empty row remain correct.
        Assert.Equal(4, result.Count);
        Assert.Equal("", result[3]);
    }

    [Table("PsrItem")]
    public sealed class PsrItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
