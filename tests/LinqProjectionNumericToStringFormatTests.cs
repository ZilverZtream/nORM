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
/// Probe + strict pin for <c>numericColumn.ToString(format)</c> in
/// projection. The single-arg overload (e.g. <c>Score.ToString("N2")</c>,
/// <c>Score.ToString("F1")</c>) is a common shape for rendering decimals
/// with a fixed precision. No portable SQL primitive matches the full
/// .NET format-string vocabulary, but the most-common case -- a
/// fixed-decimal "F&lt;N&gt;" format -- maps cleanly to SQLite's
/// printf('%.&lt;N&gt;f', col) (Postgres TO_CHAR, MySQL FORMAT, SQL
/// Server FORMAT).
///
/// Implementation scope here: handle the "F&lt;N&gt;" / "f&lt;N&gt;"
/// fixed-decimal format string at translation time when foldable; defer
/// other formats with NormUnsupportedFeatureException pointing at the
/// supported subset.
///
/// Silent-wrongness shapes:
///   * Format silently ignored -> CAST AS TEXT returns "3.14159" with full
///     stored precision instead of "3.14".
///   * Wrong rounding -> printf rounds half-to-even on SQLite (banker)
///     vs .NET's away-from-zero by default; document the difference.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionNumericToStringFormatTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PntsItem (Id INTEGER PRIMARY KEY, Score REAL NOT NULL);
            INSERT INTO PntsItem VALUES
                (1, 3.14159),
                (2, 100.0),
                (3, 0.999);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PntsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_double_ToString_F2_returns_two_decimal_text_per_row()
    {
        var result = await _ctx.Query<PntsItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = p.Score.ToString("F2") })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal("3.14", result[0].S);
        Assert.Equal("100.00", result[1].S);
        Assert.Equal("1.00", result[2].S);   // 0.999 rounded to 2 dp -> 1.00
    }

    [Table("PntsItem")]
    public sealed class PntsItem
    {
        [Key] public int Id { get; set; }
        public double Score { get; set; }
    }
}
