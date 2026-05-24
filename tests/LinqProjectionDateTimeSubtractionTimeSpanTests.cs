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
/// Strict pin + implement-first for <c>DateTime - DateTime</c> in projection
/// returning <c>TimeSpan</c>. The most common shape is computing a duration
/// between two timestamp columns (e.g. <c>End - Start</c>). SQLite has
/// julianday() returning days since the Julian epoch; the difference times
/// 86400 gives total seconds, which TimeSpan.FromSeconds reconstructs.
///
/// Silent-wrongness shapes:
///   * Untranslated -> client-eval / throw -> blocks the query.
///   * Wrong scale -> off by orders of magnitude (julianday returns days,
///     not seconds).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeSubtractionTimeSpanTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdsItem (Id INTEGER PRIMARY KEY, Start TEXT NOT NULL, End TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // Each row uses Start = anchor, End = anchor + N hours for distinct
        // TimeSpan results.
        var anchor = new DateTime(2026, 5, 24, 9, 0, 0, DateTimeKind.Utc);
        var rows = new (int id, DateTime start, DateTime end)[]
        {
            (1, anchor, anchor.AddHours(1)),       // 1h
            (2, anchor, anchor.AddHours(2.5)),     // 2h30m
            (3, anchor, anchor.AddMinutes(15)),    // 15m
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PdsItem (Id, Start, End) VALUES ($id, $s, $e)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        var pe = insert.CreateParameter(); pe.ParameterName = "$e"; insert.Parameters.Add(pe);
        foreach (var (id, st, en) in rows)
        {
            pid.Value = id;
            ps.Value = st.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            pe.Value = en.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_subtraction_projects_TimeSpan_per_row()
    {
        // julianday()-based arithmetic carries IEEE-754 double precision (~15
        // significant digits); diffs lose precision past ~10us. Assert with
        // 1ms tolerance which is the practical resolution for duration use cases.
        var result = await _ctx.Query<PdsItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = p.End - p.Start })
            .ToListAsync();
        Assert.Equal(3, result.Count);
        Assert.InRange((result[0].D - TimeSpan.FromHours(1)).TotalMilliseconds, -1.0, 1.0);
        Assert.InRange((result[1].D - TimeSpan.FromHours(2.5)).TotalMilliseconds, -1.0, 1.0);
        Assert.InRange((result[2].D - TimeSpan.FromMinutes(15)).TotalMilliseconds, -1.0, 1.0);
    }

    [Table("PdsItem")]
    public sealed class PdsItem
    {
        [Key] public int Id { get; set; }
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
    }
}
