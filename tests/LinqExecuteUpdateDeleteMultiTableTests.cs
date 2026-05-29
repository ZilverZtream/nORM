using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// ExecuteDeleteAsync / ExecuteUpdateAsync with join sources.
/// Joined queries translate to portably-safe IN subqueries:
///   DELETE FROM t WHERE pk IN (SELECT T0.pk FROM t T0 JOIN other T1 ON ...)
///   UPDATE t SET ... WHERE pk IN (SELECT T0.pk FROM t T0 JOIN other T1 ON ...)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqExecuteUpdateDeleteMultiTableTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EumLeft  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Active INTEGER NOT NULL DEFAULT 1);
            CREATE TABLE EumRight (Id INTEGER PRIMARY KEY, LeftId INTEGER NOT NULL, Flag INTEGER NOT NULL);
            INSERT INTO EumLeft  VALUES (1,'a',1),(2,'b',1),(3,'c',1);
            INSERT INTO EumRight VALUES (10, 1, 1),(20, 2, 0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task ExecuteDeleteAsync_over_joined_query_deletes_outer_rows()
    {
        // Delete EumLeft rows that have a corresponding EumRight (Id=1 and Id=2); row 3 has none.
        await ((INormQueryable<EumLeft>)_ctx.Query<EumLeft>())
            .Join(_ctx.Query<EumRight>(), l => l.Id, r => r.LeftId, (l, r) => l)
            .ExecuteDeleteAsync();

        var remaining = await _ctx.Query<EumLeft>().ToListAsync();
        Assert.Single(remaining);
        Assert.Equal("c", remaining[0].Name);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_over_joined_query_updates_outer_rows()
    {
        // Deactivate EumLeft rows that have a corresponding EumRight (Id=1 and Id=2).
        await ((INormQueryable<EumLeft>)_ctx.Query<EumLeft>())
            .Join(_ctx.Query<EumRight>(), l => l.Id, r => r.LeftId, (l, r) => l)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Active, false));

        var rows = await _ctx.Query<EumLeft>().ToListAsync();
        Assert.False(rows.First(r => r.Name == "a").Active);
        Assert.False(rows.First(r => r.Name == "b").Active);
        Assert.True(rows.First(r => r.Name == "c").Active);
    }

    [Fact]
    public async Task ExecuteDeleteAsync_grouped_joined_query_throws_deterministically()
    {
        // GroupBy on a joined source is still unsupported — must fail clearly, not silently.
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(async () =>
            await _ctx.Query<EumLeft>().GroupBy(l => l.Name).SelectMany(g => g)
                .ExecuteDeleteAsync());
        Assert.Contains("ExecuteUpdate/Delete", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Table("EumLeft")]
    public sealed class EumLeft
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool Active { get; set; }
    }

    [Table("EumRight")]
    public sealed class EumRight
    {
        [Key] public int Id { get; set; }
        public int LeftId { get; set; }
        public int Flag { get; set; }
    }
}
