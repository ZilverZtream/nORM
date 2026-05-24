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
/// Strict pin + implement-first mirror of 6b2743c (numericColumn.ToString
/// ("F&lt;N&gt;") in projection) for the Where path. Common shape:
/// <c>Where(p =&gt; p.Score.ToString("F2") == "3.14")</c> -- rows whose
/// rounded-to-2-decimals score string equals the given literal. SCV
/// handled it; ETSV must mirror so both query directions agree on the
/// printf('%.Nf', col) translation.
///
/// Silent-wrongness shapes:
///   * Untranslated -> client-eval / throw -> users blocked.
///   * Wrong precision -> "3.14" matches "3.14159000..." stored as TEXT.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereNumericToStringFormatTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WntsItem (Id INTEGER PRIMARY KEY, Score REAL NOT NULL);
            INSERT INTO WntsItem VALUES
                (1, 3.14159),
                (2, 100.0),
                (3, 3.145),
                (4, 0.999);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WntsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_double_ToString_F2_equals_literal_filters_to_matching_row()
    {
        // Score.ToString("F2") == "3.14" -> Id 1 (3.14159 rounds to 3.14).
        // Id 3 (3.145) rounds to 3.15 -- visible boundary case.
        var result = await _ctx.Query<WntsItem>()
            .Where(p => p.Score.ToString("F2") == "3.14")
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_double_ToString_F0_equals_literal_filters_to_whole_match()
    {
        // F0 rounds to integer text. 0.999 -> "1", 100.0 -> "100".
        var result = await _ctx.Query<WntsItem>()
            .Where(p => p.Score.ToString("F0") == "1")
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WntsItem")]
    public sealed class WntsItem
    {
        [Key] public int Id { get; set; }
        public double Score { get; set; }
    }
}
