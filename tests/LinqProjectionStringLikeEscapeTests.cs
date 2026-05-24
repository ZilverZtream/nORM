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
/// Strict pin + implement-first for SQL-wildcard-escape in projection
/// <c>StartsWith</c> / <c>EndsWith</c> / <c>Contains</c>. ce826da added the
/// projection translation using <c>col LIKE pattern || '%'</c> but did NOT
/// escape <c>%</c> or <c>_</c> wildcards inside the user-supplied pattern.
/// The Where path's CreateSafeLikePattern + ESCAPE clause was not
/// duplicated -- so a user query like <c>p.Code.StartsWith("100%")</c>
/// silently matches every row whose Code starts with "100" followed by
/// anything else, instead of only rows whose Code literally starts with
/// the four characters "100%".
///
/// Silent-wrongness shapes:
///   * Caller asks "rows starting with '50%' literal" -> gets every row
///     starting with '50' -- the worst kind of bug because the result
///     looks plausible but contains rows that shouldn't be there.
///   * Underscore wildcard '_' matches any single char -- same shape for
///     codes like 'A_1' meaning literal A-underscore-1.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringLikeEscapeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsleItem (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
            INSERT INTO PsleItem VALUES
                (1, '100%off'),
                (2, '100off'),
                (3, '1000off'),
                (4, '50%coupon'),
                (5, '50coupon');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PsleItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_StartsWith_with_percent_pattern_only_matches_literal()
    {
        // 'StartsWith("100%")' must return ONLY rows whose Code literally
        // begins with the 4 characters "100%". Silent-wrongness: unescaped
        // % wildcard would match Id 2 ("100off"), Id 3 ("1000off") too.
        var result = await _ctx.Query<PsleItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, M = p.Code.StartsWith("100%") })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.True(result[0].M);   // '100%off' literally starts with "100%"
        Assert.False(result[1].M);  // '100off'  -- silent-wrongness shape: would return true
        Assert.False(result[2].M);  // '1000off' -- silent-wrongness shape: would return true
        Assert.False(result[3].M);
        Assert.False(result[4].M);
    }

    [Fact]
    public async Task Select_string_Contains_with_percent_pattern_only_matches_literal()
    {
        // 'Contains("0%c")' -> literal substring "0%c". Only Id 4 ("50%coupon")
        // contains the literal 3-char substring "0%c"; Id 1 ("100%off") doesn't.
        // Silent-wrongness: unescaped % would match '0', any chars, 'c'.
        var result = await _ctx.Query<PsleItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, M = p.Code.Contains("0%c") })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.False(result[0].M); // '100%off'
        Assert.False(result[1].M);
        Assert.False(result[2].M);
        Assert.True(result[3].M);  // '50%coupon' contains '0%c'
        Assert.False(result[4].M);
    }

    [Table("PsleItem")]
    public sealed class PsleItem
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
    }
}
