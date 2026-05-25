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
/// Strict pin for <c>(a - b).Days</c> in projection -- DateTime
/// subtraction yields a TimeSpan whose Days property is the integer
/// day count. Verify the existing TimeSpan-member emit handles the
/// chained shape.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDateTimeSubtractDaysTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdtsdItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO PdtsdItem VALUES
                (1, '2026-05-25 12:00:00', '2026-05-20 12:00:00'),
                (2, '2026-12-31 23:00:00', '2026-12-01 00:00:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdtsdItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_DateTime_Subtract_Days_returns_integer_day_count_per_row()
    {
        var r = await _ctx.Query<PdtsdItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, D = (p.A - p.B).Days }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(5, r[0].D);
        Assert.Equal(30, r[1].D);
    }

    [Table("PdtsdItem")]
    public sealed class PdtsdItem
    {
        [Key] public int Id { get; set; }
        public DateTime A { get; set; }
        public DateTime B { get; set; }
    }
}
