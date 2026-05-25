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
/// Probe pin for CountAsync over the result of a set op. The set-op
/// translator builds `<left> UNION <right>` and stashes the projection,
/// then HandleDirectAggregate / the COUNT path needs to wrap the unioned
/// SELECT in a subquery (`SELECT COUNT(*) FROM (... UNION ...)`) or it
/// returns a wrong / malformed count.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqCountAfterUnionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE CauLeft  (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            CREATE TABLE CauRight (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO CauLeft  VALUES (1, 10), (2, 20), (3, 30);
            INSERT INTO CauRight VALUES (1, 20), (2, 40);  -- 20 overlaps Left
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<CauLeft>().HasKey(i => i.Id);
                mb.Entity<CauRight>().HasKey(i => i.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task CountAsync_after_Union_returns_distinct_unioned_row_count()
    {
        var left  = _ctx.Query<CauLeft >().Select(p => new { V = p.V });
        var right = _ctx.Query<CauRight>().Select(p => new { V = p.V });
        var count = await left.Union(right).CountAsync();
        // Union dedups: {10, 20, 30, 40} -> 4
        Assert.Equal(4, count);
    }

    [Table("CauLeft")]
    public sealed class CauLeft
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Table("CauRight")]
    public sealed class CauRight
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
