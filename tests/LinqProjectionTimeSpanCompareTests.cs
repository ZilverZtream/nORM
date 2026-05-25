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
/// Strict pin for <c>TimeSpan.Compare(a, b)</c> in projection. Sister to
/// DateTime.Compare (acc04c8). Microsoft.Data.Sqlite stores TimeSpan as
/// canonical 'HH:mm:ss[.fffffff]' text which is lexicographically
/// sortable within a single day, so a CASE-on-&lt;/&gt;/= yields the
/// expected -1/0/1.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionTimeSpanCompareTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PtscItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO PtscItem VALUES
                (1, '02:30:00', '01:00:00'),  -- A > B
                (2, '05:15:00', '05:15:00'),  -- A == B
                (3, '00:45:00', '03:00:00');  -- A < B
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtscItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_TimeSpan_Compare_two_columns_returns_sign_per_row()
    {
        var r = await _ctx.Query<PtscItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = TimeSpan.Compare(p.A, p.B) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(1, r[0].C);
        Assert.Equal(0, r[1].C);
        Assert.Equal(-1, r[2].C);
    }

    [Table("PtscItem")]
    public sealed class PtscItem
    {
        [Key] public int Id { get; set; }
        public TimeSpan A { get; set; }
        public TimeSpan B { get; set; }
    }
}
