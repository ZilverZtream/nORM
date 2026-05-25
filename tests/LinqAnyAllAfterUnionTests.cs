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
/// Probe pin for AnyAsync / AllAsync over Union. Same compositional path
/// as Count after Union (4949de9) -- the predicate needs to evaluate
/// against the unioned set, not just one arm. Without subquery wrapping
/// the SQL becomes malformed and returns wrong rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAnyAllAfterUnionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE AauLeft  (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            CREATE TABLE AauRight (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO AauLeft  VALUES (1, 10), (2, 20);
            INSERT INTO AauRight VALUES (1, 40), (2, 50);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<AauLeft>().HasKey(i => i.Id);
                mb.Entity<AauRight>().HasKey(i => i.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task AnyAsync_after_Union_returns_true_when_unioned_set_is_non_empty()
    {
        var left  = _ctx.Query<AauLeft >().Select(p => new { V = p.V });
        var right = _ctx.Query<AauRight>().Select(p => new { V = p.V });
        var any = await left.Union(right).AnyAsync();
        Assert.True(any);
    }

    [Fact]
    public async Task AnyAsync_after_Union_returns_false_when_both_arms_empty()
    {
        await using (var c = _cn.CreateCommand())
        {
            c.CommandText = "DELETE FROM AauLeft; DELETE FROM AauRight;";
            await c.ExecuteNonQueryAsync();
        }
        var left  = _ctx.Query<AauLeft >().Select(p => new { V = p.V });
        var right = _ctx.Query<AauRight>().Select(p => new { V = p.V });
        var any = await left.Union(right).AnyAsync();
        Assert.False(any);
    }

    [Table("AauLeft")]
    public sealed class AauLeft
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Table("AauRight")]
    public sealed class AauRight
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
