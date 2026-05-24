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
/// Strict pin for <c>string.Trim</c>, <c>TrimStart</c>, <c>TrimEnd</c> in
/// projection. TranslatabilityAnalyzer admits all three; SqliteProvider's
/// TranslateFunction routes to TRIM / LTRIM / RTRIM. Pinning to catch
/// regressions where one of the variants drops or the SQL function call
/// arguments get reordered.
///
/// Silent-wrongness shapes:
///   * Trim collapses to TrimStart -> trailing whitespace stays.
///   * TrimStart collapses to Trim -> trailing whitespace also stripped.
///   * Char-array overload silently dropped -> whitespace-only trim run.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringTrimTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PstItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PstItem VALUES
                (1, '  leading'),
                (2, 'trailing  '),
                (3, '  both  '),
                (4, 'inner spaces stay'),
                (5, '');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PstItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Trim_strips_both_ends_only()
    {
        var result = await _ctx.Query<PstItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.Trim())
            .ToListAsync();
        Assert.Equal(
            new[] { "leading", "trailing", "both", "inner spaces stay", "" },
            result.ToArray());
    }

    [Fact]
    public async Task Select_string_TrimStart_strips_leading_only_preserves_trailing()
    {
        // Silent-wrongness: collapsing to Trim would strip trailing too --
        // Id 2 result would change from "trailing  " to "trailing".
        var result = await _ctx.Query<PstItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.TrimStart())
            .ToListAsync();
        Assert.Equal(
            new[] { "leading", "trailing  ", "both  ", "inner spaces stay", "" },
            result.ToArray());
    }

    [Fact]
    public async Task Select_string_TrimEnd_strips_trailing_only_preserves_leading()
    {
        var result = await _ctx.Query<PstItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.TrimEnd())
            .ToListAsync();
        Assert.Equal(
            new[] { "  leading", "trailing", "  both", "inner spaces stay", "" },
            result.ToArray());
    }

    [Fact]
    public async Task Where_with_string_Trim_compares_to_literal()
    {
        // Trim() result == "both" should match Id 3 (which has whitespace
        // on both sides of "both"). Silent-wrongness: Trim dropped ->
        // compares the raw "  both  " against "both" -> 0 rows.
        var result = await _ctx.Query<PstItem>()
            .Where(p => p.Name.Trim() == "both")
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PstItem")]
    public sealed class PstItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
