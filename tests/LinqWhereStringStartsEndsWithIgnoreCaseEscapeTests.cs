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
/// Direct sister of e4718b8 (Contains/StartsWith/EndsWith escape pin). The
/// StringComparison-overload variants (e.g. <c>col.StartsWith(prefix,
/// StringComparison.OrdinalIgnoreCase)</c>) go through a separate visitor
/// arm (HandleStringStartsWith/EndsWith/ContainsWithComparison around
/// ExpressionToSqlVisitor.cs:2188) that takes a 2-arg signature and an
/// IsIgnoreCase computed flag. Same LIKE-pattern emit at the end -- but
/// the metacharacter-escape pin in e4718b8 only exercised the no-
/// comparison overload, so this file pins the ignore-case path too.
///
/// Silent-wrongness shapes:
///   * Translator routes ignore-case to a different LIKE-pattern build that
///     forgets to escape '%' / '_'.
///   * Translator drops the LIKE wrapper entirely and emits LOWER(col) =
///     LOWER(@p0) (acts as case-insensitive equality, NOT case-insensitive
///     prefix/suffix matching).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringStartsEndsWithIgnoreCaseEscapeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WseiEscItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO WseiEscItem VALUES
                (1, 'AlphaSuffix'),
                (2, 'alphaSuffix'),
                (3, 'ALPHASUFFIX'),
                (4, 'PrefixAlpha'),
                (5, '_underscoredStart'),
                (6, 'plain');
            -- Ids 1,2,3 share 'alpha' prefix in different cases.
            -- Id 4 has 'alpha' as suffix (case-sensitive).
            -- Id 5 starts with literal '_' to probe the underscore-as-wildcard escape.
            -- Id 6 is unrelated baseline.
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WseiEscItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_StartsWith_OrdinalIgnoreCase_matches_all_case_variants()
    {
        // Case-insensitive prefix 'alpha' -> match Ids 1, 2, 3.
        // Silent-wrongness: dropped StringComparison -> case-sensitive prefix
        // returns only Id 2.
        var result = await _ctx.Query<WseiEscItem>()
            .Where(i => i.Name.StartsWith("alpha", StringComparison.OrdinalIgnoreCase))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_EndsWith_OrdinalIgnoreCase_matches_all_case_variants()
    {
        // Case-insensitive suffix 'suffix' -> match Ids 1, 2, 3.
        var result = await _ctx.Query<WseiEscItem>()
            .Where(i => i.Name.EndsWith("suffix", StringComparison.OrdinalIgnoreCase))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_StartsWith_OrdinalIgnoreCase_escapes_underscore_metacharacter()
    {
        // Discriminating probe: literal '_' prefix. Only Id 5 starts with '_'.
        // Silent-wrongness if '_' is treated as a single-char wildcard:
        //   '_' matches "any single character", so EVERY row starting with
        //   any character (i.e. all 6 rows) would match.
        var result = await _ctx.Query<WseiEscItem>()
            .Where(i => i.Name.StartsWith("_", StringComparison.OrdinalIgnoreCase))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_EndsWith_OrdinalIgnoreCase_escapes_underscore_metacharacter()
    {
        // No row ends with '_' (Id 5 starts with one but ends with 't').
        // Silent-wrongness: '_' as wildcard would match every row (any
        // single-char suffix), returning all 6.
        var result = await _ctx.Query<WseiEscItem>()
            .Where(i => i.Name.EndsWith("_", StringComparison.OrdinalIgnoreCase))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Empty(result);
    }

    [Table("WseiEscItem")]
    public sealed class WseiEscItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
