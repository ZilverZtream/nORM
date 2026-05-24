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
/// Probe + strict pin for <c>DateTime.TimeOfDay</c> in projection AND Where.
/// Returns a TimeSpan -- the time portion (HH:mm:ss.fff) of the DateTime.
/// SQLite has time() returning 'HH:MM:SS'; the materializer needs to parse
/// that text into TimeSpan (or the provider needs to emit a TimeSpan-binding
/// format that matches how ParameterManager serializes TimeSpan params).
///
/// Silent-wrongness shapes:
///   * .TimeOfDay not admitted in TranslatabilityAnalyzer -> client-eval
///     throw on projection.
///   * TimeOfDay emits 'HH:MM:SS' but TimeSpan param binds via DbType.Time
///     (Microsoft.Data.Sqlite TimeSpan -> text, hours:minutes:seconds...).
///     Text format mismatch -> Where comparison silently returns 0 rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeTimeOfDayTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtdItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // 3 rows with distinct times-of-day on the same date so projection +
        // Where on TimeOfDay produce visibly different result sets.
        var date = new DateTime(2026, 5, 24, 0, 0, 0, DateTimeKind.Utc);
        var stamps = new (int id, DateTime stamp)[]
        {
            (1, date.AddHours(9).AddMinutes(0)),    // 09:00:00
            (2, date.AddHours(13).AddMinutes(30)),  // 13:30:00
            (3, date.AddHours(23).AddMinutes(45)),  // 23:45:00
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PtdItem (Id, Stamp) VALUES ($id, $s)";
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
            OnModelCreating = mb => mb.Entity<PtdItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_column_TimeOfDay_projects_time_portion_as_TimeSpan()
    {
        var result = await _ctx.Query<PtdItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, T = i.Stamp.TimeOfDay })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.Equal(new TimeSpan(9, 0, 0), result[0].T);
        Assert.Equal(new TimeSpan(13, 30, 0), result[1].T);
        Assert.Equal(new TimeSpan(23, 45, 0), result[2].T);
    }

    [Fact]
    public async Task Where_with_column_TimeOfDay_compares_against_TimeSpan_constant()
    {
        // Stamp.TimeOfDay > 12:00 -> {Id 2, Id 3}.
        // Silent-wrongness: format mismatch -> 0 rows. Suppressed-rows is the
        // worst kind of bug since the query looks valid.
        var noon = new TimeSpan(12, 0, 0);
        var result = await _ctx.Query<PtdItem>()
            .Where(i => i.Stamp.TimeOfDay > noon)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PtdItem")]
    public sealed class PtdItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
