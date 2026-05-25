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
/// Verify pin for <c>(a - b).Days &gt; n</c> in WHERE -- sister to the
/// projection fix in bf2736e. ETSV already has TryEmitTimeSpanMember;
/// pin guards that the WHERE-side path works end-to-end.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDateTimeSubtractDaysTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdtsdItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO WdtsdItem VALUES
                (1, '2026-05-25 12:00:00', '2026-05-20 12:00:00'),  -- 5 days
                (2, '2026-12-31 23:00:00', '2026-12-01 00:00:00'),  -- 30 days
                (3, '2026-05-21 12:00:00', '2026-05-20 12:00:00');  -- 1 day
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdtsdItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_DateTime_Subtract_Days_greater_than_threshold_filters_correctly()
    {
        var ids = await _ctx.Query<WdtsdItem>()
            .Where(p => (p.A - p.B).Days > 3)
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WdtsdItem")]
    public sealed class WdtsdItem
    {
        [Key] public int Id { get; set; }
        public DateTime A { get; set; }
        public DateTime B { get; set; }
    }
}
