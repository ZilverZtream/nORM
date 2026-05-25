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
/// Probe pin for DateTime column WHERE comparison when stored rows mix
/// timezone offsets ('Z' / '+02:00' / '+03:00' / '-02:00'). SQLite stores
/// DateTime as TEXT; without normalization the comparison lex-compares
/// the raw strings, which silently mis-orders rows whose UTC-equivalent
/// time differs from their lex order. The fix wraps the COLUMN side
/// (not the parameter side) with <c>datetime(...)</c> so the column is
/// normalized to canonical UTC-as-text before comparison; the parameter
/// is bound from .NET DateTime and already lex-compares correctly
/// against the canonical output.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeMixedTimezoneTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtmtItem (Id INTEGER PRIMARY KEY, D TEXT NOT NULL);
            INSERT INTO WdtmtItem VALUES
                (1, '2024-01-01T15:00:00+02:00'),  -- 13:00 UTC
                (2, '2024-01-01T14:00:00Z'),       -- 14:00 UTC
                (3, '2024-01-01T16:00:00+03:00'),  -- 13:00 UTC
                (4, '2024-01-01T10:00:00-02:00');  -- 12:00 UTC
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdtmtItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_DateTime_column_compared_to_UTC_literal_uses_chronological_semantics()
    {
        // Threshold = 13:30 UTC. Chronologically less: 1 (13:00), 3 (13:00), 4 (12:00) -> 3 rows.
        var threshold = new DateTime(2024, 1, 1, 13, 30, 0, DateTimeKind.Utc);
        var rows = await _ctx.Query<WdtmtItem>()
            .Where(p => p.D < threshold)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal(new[] { 1, 3, 4 }, rows.Select(r => r.Id).ToArray());
    }

    [Table("WdtmtItem")]
    public sealed class WdtmtItem
    {
        [Key] public int Id { get; set; }
        public DateTime D { get; set; }
    }
}
