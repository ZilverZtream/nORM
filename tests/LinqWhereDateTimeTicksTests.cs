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
/// Strict pin + implement-first for <c>DateTime.Ticks</c> in Where
/// comparisons. SQLite has no native ticks-since-MinValue function, but
/// (julianday(col) - julianday('0001-01-01 00:00:00')) * 86400 * 1e7
/// gives ticks at REAL precision (~15 significant digits — usable for
/// comparison ranges within a few hundred years).
///
/// Pin focuses on Where (where comparison range tolerance is fine);
/// projection round-trip back to long ticks loses precision past ~us
/// scale so we don't pin projection here.
///
/// Silent-wrongness shape:
///   * Untranslated -> client-eval / throw.
///   * Wrong scale (seconds instead of ticks) -> comparison threshold
///     mismatches by 10^7.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeTicksTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdttItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var anchor = new DateTime(2026, 5, 24, 12, 0, 0, DateTimeKind.Utc);
        var stamps = new (int id, DateTime stamp)[]
        {
            (1, anchor.AddDays(-30)),
            (2, anchor),
            (3, anchor.AddDays(10)),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO WdttItem (Id, Stamp) VALUES ($id, $s)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        foreach (var (id, stamp) in stamps)
        {
            pid.Value = id;
            ps.Value = stamp.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdttItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_DateTime_Ticks_greater_than_threshold_filters_rows()
    {
        // Threshold = anchor.Ticks; rows with Stamp.Ticks > threshold -> Id 3.
        var threshold = new DateTime(2026, 5, 24, 12, 0, 0, DateTimeKind.Utc).Ticks;
        var result = await _ctx.Query<WdttItem>()
            .Where(p => p.Stamp.Ticks > threshold)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WdttItem")]
    public sealed class WdttItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
