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
/// Probe pin for `Select(p =&gt; p.D &gt; new DateTime(2020,1,1) ? "old" : "new")`.
/// Exercises (a) DateTime constant literal embedded as a NewExpression in the
/// expression tree, (b) ternary lowering to SQL CASE WHEN, and (c) string
/// literal result branches. The translator should constant-fold the
/// `new DateTime(...)` to a SQL parameter or literal -- if it passes through
/// as a NewExpression the visitor will reject it.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqConditionalProjectionDateTimeLiteralTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CpdItem (Id INTEGER PRIMARY KEY, D TEXT NOT NULL);
            INSERT INTO CpdItem VALUES
                (1, '2010-06-15 00:00:00'),
                (2, '2025-04-01 00:00:00'),
                (3, '2019-12-31 00:00:00');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CpdItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_ternary_DateTime_literal_constant_per_row_branches()
    {
        var rows = await _ctx.Query<CpdItem>().OrderBy(p => p.Id)
            .Select(p => new
            {
                p.Id,
                Era = p.D > new DateTime(2020, 1, 1) ? "new" : "old"
            })
            .ToListAsync();
        Assert.Equal(3, rows.Count);
        Assert.Equal("old", rows[0].Era);  // 2010 < 2020
        Assert.Equal("new", rows[1].Era);  // 2025 > 2020
        Assert.Equal("old", rows[2].Era);  // 2019 < 2020
    }

    [Table("CpdItem")]
    public sealed class CpdItem
    {
        [Key] public int Id { get; set; }
        public DateTime D { get; set; }
    }
}
