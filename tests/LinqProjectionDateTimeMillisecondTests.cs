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
/// Probe + strict pin for <c>DateTime.Millisecond</c> in projection AND Where.
/// SQLite has strftime('%f', col) returning the seconds-with-fractional
/// portion (e.g. '56.789'); subtracting strftime('%S', col) gives the
/// fractional seconds which times 1000 yields the integer milliseconds.
///
/// Silent-wrongness shapes:
///   * Not admitted in analyzer -> client-eval throw on projection.
///   * Returned as fractional seconds (0..1 range) instead of integer
///     milliseconds (0..999) -> rows compare against the wrong scale.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeMillisecondTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmsItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // Distinct millisecond values: 0, 123, 456, 789, 999.
        var date = new DateTime(2026, 5, 24, 12, 30, 45, 0, DateTimeKind.Utc);
        var stamps = new (int id, DateTime stamp)[]
        {
            (1, date.AddMilliseconds(0)),
            (2, date.AddMilliseconds(123)),
            (3, date.AddMilliseconds(456)),
            (4, date.AddMilliseconds(789)),
            (5, date.AddMilliseconds(999)),
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO PmsItem (Id, Stamp) VALUES ($id, $s)";
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
            OnModelCreating = mb => mb.Entity<PmsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_column_Millisecond_projects_integer_ms_per_row()
    {
        var result = await _ctx.Query<PmsItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, M = i.Stamp.Millisecond })
            .ToListAsync();
        Assert.Equal(5, result.Count);
        Assert.Equal(0, result[0].M);
        Assert.Equal(123, result[1].M);
        Assert.Equal(456, result[2].M);
        Assert.Equal(789, result[3].M);
        Assert.Equal(999, result[4].M);
    }

    [Fact]
    public async Task Where_with_column_Millisecond_threshold_filters_rows()
    {
        // Stamp.Millisecond >= 500 -> {Id 4 (789), Id 5 (999)}.
        var result = await _ctx.Query<PmsItem>()
            .Where(i => i.Stamp.Millisecond >= 500)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PmsItem")]
    public sealed class PmsItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
