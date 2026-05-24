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
/// Strict pin for column-side <c>DateTime.AddTicks</c> in projection AND
/// Where. Sister to 6711271's AddMilliseconds work; AddTicks is the
/// finest-grained Add* (1 tick = 100ns) and was missing alongside
/// AddMilliseconds before this pass.
///
/// Implementation scales ticks to seconds via /1e7 and reuses the same
/// strftime + RTRIM trailing-zero/dot trim that matches Microsoft.Data.
/// Sqlite's FFFFFFF DateTime binding.
///
/// Silent-wrongness shapes:
///   * Wrong scale factor (e.g. /1e6 or /1e8) -> off-by-an-order-of-
///     magnitude shifts that pass build but fail equality every row.
///   * Sign handling: negative ticks must work without the '+-N' bug
///     that bit AddDays/AddMonths/AddYears per memory item #75.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeAddTicksStrictTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        // Anchor at exact second; rows separated by 2.5M ticks (250 ms each).
        var anchor = new DateTime(2026, 5, 24, 12, 0, 0, 0, DateTimeKind.Utc);
        var stamps = new (int id, DateTime stamp)[]
        {
            (1, anchor.AddTicks(0)),
            (2, anchor.AddTicks(2_500_000L)),    // +250 ms
            (3, anchor.AddTicks(5_000_000L)),    // +500 ms
            (4, anchor.AddTicks(7_500_000L)),    // +750 ms
            (5, anchor.AddTicks(10_000_000L)),   // +1000 ms (whole second)
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO WdtItem (Id, Stamp) VALUES ($id, $s)";
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
            OnModelCreating = mb => mb.Entity<WdtItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_with_column_AddTicks_positive_threshold_filters_rows()
    {
        // Stamp.AddTicks(5_000_000) > cutoff (anchor + 1000ms) -> Stamp >
        // anchor + 500ms -> {Id 4 (+750ms), Id 5 (+1000ms)}.
        var cutoff = new DateTime(2026, 5, 24, 12, 0, 1, 0, DateTimeKind.Utc);
        var result = await _ctx.Query<WdtItem>()
            .Where(i => i.Stamp.AddTicks(5_000_000L) > cutoff)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_with_column_AddTicks_negative_threshold_filters_rows()
    {
        // Stamp.AddTicks(-2_500_000) >= anchor -> Stamp >= anchor + 250ms ->
        // {Id 2, 3, 4, 5}. Silent-wrongness: sign-drop -> {3, 4, 5} or {}.
        var anchor = new DateTime(2026, 5, 24, 12, 0, 0, 0, DateTimeKind.Utc);
        var result = await _ctx.Query<WdtItem>()
            .Where(i => i.Stamp.AddTicks(-2_500_000L) >= anchor)
            .OrderBy(i => i.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3, 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Select_column_AddTicks_projects_shifted_DateTime_per_row()
    {
        // Project shifted DateTime; each row's Stamp + 1_000_000 ticks (100 ms)
        // should round-trip through the materializer to the expected DateTime.
        var result = await _ctx.Query<WdtItem>()
            .OrderBy(i => i.Id)
            .Select(i => new { i.Id, S = i.Stamp.AddTicks(1_000_000L) })
            .ToListAsync();
        var anchor = new DateTime(2026, 5, 24, 12, 0, 0, 0, DateTimeKind.Utc);
        Assert.Equal(5, result.Count);
        Assert.Equal(anchor.AddTicks(1_000_000L), result[0].S);
        Assert.Equal(anchor.AddTicks(3_500_000L), result[1].S);
        Assert.Equal(anchor.AddTicks(6_000_000L), result[2].S);
        Assert.Equal(anchor.AddTicks(8_500_000L), result[3].S);
        Assert.Equal(anchor.AddTicks(11_000_000L), result[4].S);
    }

    [Table("WdtItem")]
    public sealed class WdtItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }
}
