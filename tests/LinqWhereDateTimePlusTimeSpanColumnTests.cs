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
/// Strict pin mirroring 7f91efc to the Where path: <c>p.Stamp + p.Duration
/// &gt; cutoff</c>. Without the ETSV mirror branch the same silent-wrongness
/// shape applies -- SQL '+' on TEXT coerces to numeric and the predicate
/// evaluates against garbage.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimePlusTimeSpanColumnTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtcItem (Id INTEGER PRIMARY KEY, Stamp TEXT NOT NULL, Duration TEXT NOT NULL);
            """;
        await cmd.ExecuteNonQueryAsync();
        var anchor = new DateTime(2026, 5, 24, 9, 0, 0, DateTimeKind.Utc);
        var rows = new (int id, DateTime stamp, TimeSpan dur)[]
        {
            (1, anchor,             TimeSpan.FromMinutes(30)),  // Stamp+30m  = 09:30
            (2, anchor,             TimeSpan.FromHours(2)),     // Stamp+2h   = 11:00
            (3, anchor.AddHours(1), TimeSpan.FromHours(1)),     // Stamp+1h   = 11:00 (10:00+1h)
            (4, anchor,             TimeSpan.FromMinutes(15)),  // Stamp+15m  = 09:15
        };
        await using var insert = _cn.CreateCommand();
        insert.CommandText = "INSERT INTO WdtcItem (Id, Stamp, Duration) VALUES ($id, $s, $d)";
        var pid = insert.CreateParameter(); pid.ParameterName = "$id"; insert.Parameters.Add(pid);
        var ps = insert.CreateParameter(); ps.ParameterName = "$s"; insert.Parameters.Add(ps);
        var pd = insert.CreateParameter(); pd.ParameterName = "$d"; insert.Parameters.Add(pd);
        foreach (var (id, st, du) in rows)
        {
            pid.Value = id;
            ps.Value = st.ToString("yyyy-MM-dd HH:mm:ss.fffffff");
            pd.Value = du.ToString("c");
            await insert.ExecuteNonQueryAsync();
        }
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdtcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_DateTime_plus_TimeSpan_column_greater_than_cutoff_filters_correctly()
    {
        // Stamp + Duration > 10:00 -> {2 (11:00), 3 (11:00)}.
        var cutoff = new DateTime(2026, 5, 24, 10, 0, 0, DateTimeKind.Utc);
        var result = await _ctx.Query<WdtcItem>()
            .Where(p => p.Stamp + p.Duration > cutoff)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WdtcItem")]
    public sealed class WdtcItem
    {
        [Key] public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public TimeSpan Duration { get; set; }
    }
}
