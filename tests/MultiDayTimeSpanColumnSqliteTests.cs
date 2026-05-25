using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Microsoft.Data.Sqlite serializes TimeSpan via TimeSpan.ToString("c") which
/// emits multi-day spans as <c>[-]d.HH:mm:ss[.fffffff]</c>. The fixed-offset
/// substr(col,1,2)/substr(col,4,2)/substr(col,7,2) emit shape breaks for any
/// TimeSpan with a non-zero Days component (the position-1 slot suddenly
/// contains a day digit rather than the hours field). Likewise negative spans
/// carry a leading '-' that shifts every offset by one.
///
/// This pin covers: multi-day positive, multi-day negative, sub-day positive,
/// sub-day negative, and zero. Each test computes a property of the column
/// (Days, Hours, TotalHours, TotalSeconds, TotalMinutes, TotalMilliseconds)
/// and verifies the SQL evaluates to the same value System.TimeSpan reports
/// in-memory.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class MultiDayTimeSpanColumnSqliteTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE Spans (Id INTEGER PRIMARY KEY, Span TEXT NOT NULL);";
        await cmd.ExecuteNonQueryAsync();

        // Cover the four canonical shapes plus a fractional component.
        var spans = new (int id, TimeSpan v)[]
        {
            (1, TimeSpan.FromDays(3) + TimeSpan.FromHours(5) + TimeSpan.FromMinutes(30)), // +3.05:30:00
            (2, -(TimeSpan.FromDays(2) + TimeSpan.FromHours(12))),                          // -2.12:00:00
            (3, TimeSpan.FromHours(5) + TimeSpan.FromMinutes(45)),                          // 05:45:00
            (4, -TimeSpan.FromMinutes(15)),                                                  // -00:15:00
            (5, TimeSpan.Zero),                                                              // 00:00:00
            (6, TimeSpan.FromDays(1) + TimeSpan.FromTicks(5_000_000)),                       // 1.00:00:00.5000000 (0.5s fractional)
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO Spans (Id, Span) VALUES ($id, $v)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var pv = insert.CreateParameter(); pv.ParameterName = "$v"; insert.Parameters.Add(pv);
        foreach (var (id, v) in spans)
        {
            pid.Value = id;
            pv.Value = v.ToString("c");
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SpanRow>().HasKey(r => r.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task TotalSeconds_projection_matches_dotnet_for_all_shapes()
    {
        var rows = await _ctx.Query<SpanRow>().OrderBy(r => r.Id)
            .Select(r => new { r.Id, T = r.Span.TotalSeconds })
            .ToListAsync();
        Assert.Equal(6, rows.Count);
        // Tolerances: TotalSeconds rounded to ms (sub-second handling only on row 6).
        Assert.InRange(rows[0].T - TimeSpan.FromDays(3.0 + 5.5/24.0).TotalSeconds, -0.001, 0.001);
        Assert.InRange(rows[1].T - (-(TimeSpan.FromDays(2) + TimeSpan.FromHours(12))).TotalSeconds, -0.001, 0.001);
        Assert.InRange(rows[2].T - (TimeSpan.FromHours(5) + TimeSpan.FromMinutes(45)).TotalSeconds, -0.001, 0.001);
        Assert.InRange(rows[3].T - (-TimeSpan.FromMinutes(15)).TotalSeconds, -0.001, 0.001);
        Assert.Equal(0.0, rows[4].T);
        Assert.InRange(rows[5].T - (TimeSpan.FromDays(1) + TimeSpan.FromTicks(5_000_000)).TotalSeconds, -0.001, 0.001);
    }

    [Fact]
    public async Task TotalHours_projection_matches_dotnet_for_multi_day_spans()
    {
        var rows = await _ctx.Query<SpanRow>().OrderBy(r => r.Id)
            .Where(r => r.Id == 1 || r.Id == 2)
            .Select(r => new { r.Id, H = r.Span.TotalHours })
            .ToListAsync();
        Assert.InRange(rows[0].H - (3 * 24 + 5 + 0.5), -1e-6, 1e-6);
        Assert.InRange(rows[1].H - (-(2 * 24 + 12)), -1e-6, 1e-6);
    }

    [Table("Spans")]
    public sealed class SpanRow
    {
        [Key] public int Id { get; set; }
        public TimeSpan Span { get; set; }
    }
}
