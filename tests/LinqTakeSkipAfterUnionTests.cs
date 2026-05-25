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
/// Probe pin for Take / Skip applied AFTER a set op
/// (Union / Concat / Intersect / Except). The current translator
/// throws NormUnsupportedFeatureException because the naive emit
/// `SELECT V FROM L UNION SELECT V FROM R LIMIT n` lets SQLite parse
/// LIMIT against the right arm only, returning incorrect rows. Fix:
/// wrap the unioned SELECT in a subquery so LIMIT applies to the full
/// set: `SELECT * FROM (SELECT V FROM L UNION SELECT V FROM R) AS T0
/// LIMIT n`.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqTakeSkipAfterUnionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TsauLeft  (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            CREATE TABLE TsauRight (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO TsauLeft  VALUES (1, 10), (2, 20), (3, 30);
            INSERT INTO TsauRight VALUES (1, 40), (2, 50);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<TsauLeft>().HasKey(i => i.Id);
                mb.Entity<TsauRight>().HasKey(i => i.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Take_after_Union_returns_first_N_rows_of_unioned_set()
    {
        var left  = _ctx.Query<TsauLeft >().Select(p => new { V = p.V });
        var right = _ctx.Query<TsauRight>().Select(p => new { V = p.V });
        var rows = await left.Union(right).Take(2).ToListAsync();
        // Unioned distinct values: {10, 20, 30, 40, 50}. Take(2) returns 2 of them
        // (SQLite doesn't guarantee order without ORDER BY, but count must be 2).
        Assert.Equal(2, rows.Count);
        // All taken values must come from the unioned set.
        foreach (var r in rows)
            Assert.Contains(r.V, new[] { 10, 20, 30, 40, 50 });
    }

    [Fact]
    public async Task Skip_after_Union_skips_first_N_rows_of_unioned_set()
    {
        var left  = _ctx.Query<TsauLeft >().Select(p => new { V = p.V });
        var right = _ctx.Query<TsauRight>().Select(p => new { V = p.V });
        var rows = await left.Union(right).Skip(3).ToListAsync();
        // 5 distinct unioned values, Skip(3) -> 2 remaining.
        Assert.Equal(2, rows.Count);
        foreach (var r in rows)
            Assert.Contains(r.V, new[] { 10, 20, 30, 40, 50 });
    }

    [Fact]
    public async Task Take_and_Skip_after_Union_windows_the_unioned_set()
    {
        var left  = _ctx.Query<TsauLeft >().Select(p => new { V = p.V });
        var right = _ctx.Query<TsauRight>().Select(p => new { V = p.V });
        var rows = await left.Union(right).Skip(1).Take(2).ToListAsync();
        Assert.Equal(2, rows.Count);
    }

    [Table("TsauLeft")]
    public sealed class TsauLeft
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Table("TsauRight")]
    public sealed class TsauRight
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
