using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins server-side translation of chained / less-common string method shapes
/// that commonly appear in user predicates but historically threw
/// `NormUnsupportedFeatureException` from ExpressionToSqlVisitor:
/// <list type="bullet">
///   <item>Chained `s.ToLower().Contains("...")` — case-insensitive substring</item>
///   <item>`s.Trim().Length` — pipeline ending in Length</item>
///   <item>`s.Substring(n).StartsWith("...")` — substring then prefix check</item>
/// </list>
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqStringMethodTranslationTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SmRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO SmRow VALUES
                (1, '  Alice  '),
                (2, 'BOB'),
                (3, 'charlie'),
                (4, 'David');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Lowered_string_contains_filters_case_insensitively()
    {
        // ToLower().Contains("bob") matches "BOB" (lowered to "bob") but not "Alice" / "charlie" / "David".
        var rows = await _ctx.Query<SmRow>()
            .Where(r => r.Name.ToLower().Contains("bob"))
            .ToListAsync();
        Assert.Single(rows);
        Assert.Equal("BOB", rows[0].Name);
    }

    [Fact]
    public async Task Trim_then_length_filters_by_trimmed_length()
    {
        // "  Alice  ".Trim().Length == 5; "BOB".Trim().Length == 3; "charlie".Trim().Length == 7;
        // "David".Trim().Length == 5. Match length == 5 → 2 rows.
        var rows = (await _ctx.Query<SmRow>()
            .Where(r => r.Name.Trim().Length == 5)
            .ToListAsync())
            .OrderBy(r => r.Id).ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("  Alice  ", rows[0].Name);
        Assert.Equal("David", rows[1].Name);
    }

    [Fact]
    public async Task Substring_then_starts_with_filters_by_chopped_prefix()
    {
        // r.Name.Substring(1) chops the first char. "BOB".Substring(1) = "OB", starts with "O" → match.
        // Only "BOB" begins with B such that its tail starts with "O".
        var rows = await _ctx.Query<SmRow>()
            .Where(r => r.Name.Substring(1).StartsWith("O"))
            .ToListAsync();
        Assert.Single(rows);
        Assert.Equal("BOB", rows[0].Name);
    }

    [Table("SmRow")]
    public sealed class SmRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
