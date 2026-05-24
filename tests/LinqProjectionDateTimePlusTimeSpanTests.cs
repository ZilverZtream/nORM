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
/// Strict pin + implement-first for <c>DateTime + TimeSpan</c> (and the
/// subtraction sister, <c>DateTime - TimeSpan</c>) in projection AND Where.
/// Common pattern: shift a column by a constant or closure-captured
/// TimeSpan, e.g. <c>stamp + TimeSpan.FromHours(1)</c>. Without an
/// explicit handler the SQL '+' on TEXT coerces to numeric 0 -> result
/// is the bound TimeSpan-seconds value instead of a shifted DateTime.
///
/// Silent-wrongness shapes:
///   * Untranslated -> client-eval / throw.
///   * SQL '+' on TEXT returns 0 or the TimeSpan numeric value -- not a
///     shifted DateTime.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimePlusTimeSpanTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtsItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var anchor = new DateTime(2026, 5, 24, 9, 0, 0, DateTimeKind.Utc);
        var stamps = new (int id, DateTime s)[]
        {
            (1, anchor),
            (2, anchor.AddHours(2)),
            (3, anchor.AddDays(1)),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PdtsItem (Id, Stamp) VALUES ($id, $s)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        foreach (var (id, st) in stamps)
        {
            pid.Value = id;
            ps.Value = st.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_plus_TimeSpan_constant_projects_shifted_DateTime()
    {
        var shift = TimeSpan.FromHours(1);
        var result = await _ctx.Query<PdtsItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = p.Stamp + shift })
            .ToListAsync();
        var anchor = new DateTime(2026, 5, 24, 9, 0, 0, DateTimeKind.Utc);
        Assert.Equal(3, result.Count);
        Assert.Equal(anchor.AddHours(1), result[0].S);
        Assert.Equal(anchor.AddHours(3), result[1].S);
        Assert.Equal(anchor.AddDays(1).AddHours(1), result[2].S);
    }

    [Fact]
    public async Task Where_DateTime_minus_TimeSpan_compared_to_constant_filters_rows()
    {
        // Stamp - 30m > anchor -> Stamp > anchor + 30m -> Id 2 (+2h), 3 (+1d).
        // Silent-wrongness shape this catches: without the placeholder compiled-param
        // slot in ETSV's branch, the ParameterValueExtractor's value list shifts by
        // one, binding @cp0 to the TimeSpan shift value instead of the anchor RHS,
        // which makes the comparison ALWAYS-TRUE and returns every row.
        var shift = TimeSpan.FromMinutes(30);
        var anchor = new DateTime(2026, 5, 24, 9, 0, 0, DateTimeKind.Utc);
        var result = await _ctx.Query<PdtsItem>()
            .Where(p => p.Stamp - shift > anchor)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PdtsItem")]
    public sealed class PdtsItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
