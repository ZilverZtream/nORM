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
/// Pins case-insensitive string comparison via <c>string.Equals(other, StringComparison.X)</c>
/// in Where predicates. Two common spellings:
///   * <c>i.Name.Equals(value, StringComparison.OrdinalIgnoreCase)</c>
///   * <c>string.Equals(i.Name, value, StringComparison.OrdinalIgnoreCase)</c>
///
/// SQLite default is case-sensitive equality for TEXT (NOCASE collation
/// must be applied explicitly). The translator must emit something
/// equivalent to UPPER(col) = UPPER(@p0) (or LOWER/LOWER, or COLLATE NOCASE)
/// when the StringComparison argument requests insensitive matching.
///
/// Silent-wrongness shapes:
///   * Translator drops the StringComparison argument silently -> case-
///     sensitive match, returns fewer rows than expected
///   * Translator treats StringComparison as an unsupported argument and
///     falls through to client-eval -> works but materializes the table
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringEqualsIgnoreCaseTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WseiItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO WseiItem VALUES
                (1, 'Alpha'),
                (2, 'alpha'),
                (3, 'ALPHA'),
                (4, 'Beta'),
                (5, 'beta');
            -- Three case variants of 'alpha' (Ids 1-3) and two of 'beta' (Ids 4-5).
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WseiItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_instance_string_Equals_OrdinalIgnoreCase_matches_all_case_variants()
    {
        // i.Name.Equals("alpha", StringComparison.OrdinalIgnoreCase) -> match
        // Ids 1, 2, 3 (Alpha, alpha, ALPHA).
        // Silent-wrongness: case-sensitive match returns only Id 2; dropped
        // predicate returns all 5 rows.
        var result = await _ctx.Query<WseiItem>()
            .Where(i => i.Name.Equals("alpha", StringComparison.OrdinalIgnoreCase))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_static_string_Equals_OrdinalIgnoreCase_matches_all_case_variants()
    {
        // Static form `string.Equals(a, b, sc)`. Different argument shape;
        // translator must recognize both spellings.
        var result = await _ctx.Query<WseiItem>()
            .Where(i => string.Equals(i.Name, "beta", StringComparison.OrdinalIgnoreCase))
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_case_sensitive_string_equality_baseline_matches_only_exact()
    {
        // Baseline / regression guard: case-sensitive == operator must still
        // match only the exact case. If a future tweak makes equality case-
        // insensitive by default, this assertion catches the change.
        var result = await _ctx.Query<WseiItem>()
            .Where(i => i.Name == "alpha")
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WseiItem")]
    public sealed class WseiItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
