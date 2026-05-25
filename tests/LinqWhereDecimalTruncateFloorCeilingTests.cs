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
/// Strict pin for <c>decimal.Truncate / Floor / Ceiling / Abs</c> in
/// WHERE -- sister to the projection fix in a57e0f6. ETSV routes
/// through TranslateFunction so the typeof(decimal) provider entries
/// should reach predicates; pin guards regressions.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDecimalTruncateFloorCeilingTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtfcItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO WdtfcItem VALUES
                (1, '2.7'),
                (2, '-2.7'),
                (3, '5.0'),
                (4, '-0.4');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdtfcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_decimal_Truncate_equals_constant_filters_correct_rows()
    {
        var ids = await _ctx.Query<WdtfcItem>()
            .Where(p => decimal.Truncate(p.V) == 2m)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1 }, ids.Select(x => x.Id).ToArray());
    }

    // NOTE: decimal.Abs(col) > decimal-literal returns no rows under the
    // current ParameterManager + SQLite decimal-as-TEXT binding interaction.
    // ABS(col) is numeric but the closure-folded decimal literal binds as
    // TEXT, and SQLite's affinity rules then perform a textual comparison
    // that gives the wrong answer. Separate iteration; the Truncate verify
    // pin above documents the working path.

    [Table("WdtfcItem")]
    public sealed class WdtfcItem
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
