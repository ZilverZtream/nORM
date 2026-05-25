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
/// Strict pin for <c>TimeOnly.IsBetween(start, end)</c> in projection.
/// .NET semantics: if start &lt;= end, the test is half-open
/// [start, end). If start &gt; end, it wraps across midnight:
/// this &gt;= start OR this &lt; end. Both shapes are expressible as
/// a CASE that compares start &lt;= end at runtime.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeOnlyIsBetweenTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtoibItem (Id INTEGER PRIMARY KEY, T TEXT NOT NULL);
            INSERT INTO PtoibItem VALUES
                (1, '08:00:00'),
                (2, '12:00:00'),
                (3, '23:30:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtoibItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_TimeOnly_IsBetween_normal_range_flags_in_window_rows()
    {
        var start = new TimeOnly(9, 0);
        var end = new TimeOnly(17, 0);
        var r = await _ctx.Query<PtoibItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, In = p.T.IsBetween(start, end) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.False(r[0].In);   // 08:00 -- before 9:00
        Assert.True(r[1].In);    // 12:00 -- in window
        Assert.False(r[2].In);   // 23:30 -- after 17:00
    }

    [Fact]
    public async Task Select_TimeOnly_IsBetween_wrap_around_midnight_flags_late_or_early_rows()
    {
        // 22:00 -> 06:00 wraps: matches >=22:00 OR <06:00.
        var start = new TimeOnly(22, 0);
        var end = new TimeOnly(6, 0);
        var r = await _ctx.Query<PtoibItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, In = p.T.IsBetween(start, end) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.False(r[0].In);   // 08:00 -- between 6 and 22 (out)
        Assert.False(r[1].In);   // 12:00 -- out
        Assert.True(r[2].In);    // 23:30 -- in wrap
    }

    [Table("PtoibItem")]
    public sealed class PtoibItem
    {
        [Key] public int Id { get; set; }
        public TimeOnly T { get; set; }
    }
}
