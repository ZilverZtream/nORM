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
/// Probes what happens when Where / OrderBy / Skip / Take are chained AFTER Union / Concat /
/// Intersect / Except. The auditor flagged these as a real-world need; this test pins the
/// current behaviour so we know whether to widen support or document the constraint.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSetOpCompositionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ScRow (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL);
            INSERT INTO ScRow VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d'),(5,'e');
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
    public async Task Union_followed_by_OrderBy_sorts_combined_result()
    {
        var left  = _ctx.Query<ScRow>().Where(r => r.Id <= 2);
        var right = _ctx.Query<ScRow>().Where(r => r.Id >= 4);
        var ids = (await left.Union(right).OrderByDescending(r => r.Id).ToListAsync())
                  .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 5, 4, 2, 1 }, ids);
    }

    [Fact]
    public async Task Where_after_Union_throws_with_actionable_rewrite_hint()
    {
        // Where applied to the unified rows would need a subquery wrap; that's post-v1. We
        // throw deterministically with a message that names the working alternative.
        var left  = _ctx.Query<ScRow>().Where(r => r.Id <= 2);
        var right = _ctx.Query<ScRow>().Where(r => r.Id >= 4);
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await left.Union(right).Where(r => r.Id != 4).ToListAsync();
        });
        Assert.Contains("Union", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Where_pushed_into_each_side_of_Union_works_today()
    {
        var left  = _ctx.Query<ScRow>().Where(r => r.Id <= 2 && r.Id != 4);
        var right = _ctx.Query<ScRow>().Where(r => r.Id >= 4 && r.Id != 4);
        var ids = (await left.Union(right).OrderBy(r => r.Id).ToListAsync())
                  .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 2, 5 }, ids);
    }

    [Fact]
    public async Task Union_followed_by_Skip_and_Take_pages_combined_result()
    {
        var left  = _ctx.Query<ScRow>().Where(r => r.Id <= 2);
        var right = _ctx.Query<ScRow>().Where(r => r.Id >= 4);
        var ids = (await left.Union(right).OrderBy(r => r.Id).Skip(1).Take(2).ToListAsync())
                  .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 4 }, ids);
    }

    [Table("ScRow")]
    public sealed class ScRow
    {
        [Key] public int Id { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
