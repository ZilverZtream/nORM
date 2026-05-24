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
/// Strict pin for SQL-wildcard-escape in Where-side StartsWith with a
/// CLOSURE-CAPTURED variable pattern. Companion to 04731df (projection
/// constant pattern). The Where path's HandleStringStartsWith routes to
/// EmitLikePredicate which uses provider.GetLikeEscapeSql for variable
/// patterns. Pin verifies the runtime-escape branch matches the constant
/// branch's behavior so users can't accidentally inject wildcards via a
/// pattern they sourced from user input or a config column.
///
/// Silent-wrongness shape: caller binds prefix="100%" expecting literal
/// match; without escape, the % wildcard matches Id 2/3 too.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringStartsWithVariablePatternEscapeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WswpItem (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            INSERT INTO WswpItem VALUES
                (1, '100%off'),
                (2, '100off'),
                (3, '1000off');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WswpItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_string_StartsWith_with_closure_captured_percent_pattern_only_matches_literal()
    {
        // Closure-captured pattern containing % wildcard. Must only match
        // Id 1 ("100%off"); silent-wrongness would also match Id 2/3.
        var prefix = "100%";
        var result = await _ctx.Query<WswpItem>()
            .Where(p => p.Code.StartsWith(prefix))
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_string_Contains_with_closure_captured_underscore_pattern_only_matches_literal()
    {
        // Underscore wildcard matches any single char. Closure-captured
        // pattern "0_o" should only match the literal sequence 0-underscore-o
        // which appears in no row -- silent-wrongness would match Id 1/2/3.
        var needle = "0_o";
        var result = await _ctx.Query<WswpItem>()
            .Where(p => p.Code.Contains(needle))
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Empty(result);
    }

    [Table("WswpItem")]
    public sealed class WswpItem
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }
}
