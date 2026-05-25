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
/// Probe pin for OrderBy applied after Union. Surfaced as a translator
/// gap during the Union decimal probe (3d74beb): the OrderByTranslator's
/// member resolver couldn't bind `r.V` because after a set-op the source
/// is the unioned result, not the original mapping. Expected:
/// `SELECT ... UNION SELECT ... ORDER BY V`.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByAfterUnionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ObauLeft  (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            CREATE TABLE ObauRight (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO ObauLeft  VALUES (1, 30), (2, 10);
            INSERT INTO ObauRight VALUES (1, 20), (2, 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<ObauLeft>().HasKey(i => i.Id);
                mb.Entity<ObauRight>().HasKey(i => i.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task OrderBy_after_Union_sorts_the_unioned_set_ascending()
    {
        var left  = _ctx.Query<ObauLeft >().Select(p => new { V = p.V });
        var right = _ctx.Query<ObauRight>().Select(p => new { V = p.V });
        var rows = await left.Union(right).OrderBy(r => r.V).ToListAsync();
        Assert.Equal(new[] { 10, 20, 30, 40 }, rows.Select(r => r.V).ToArray());
    }

    [Table("ObauLeft")]
    public sealed class ObauLeft
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Table("ObauRight")]
    public sealed class ObauRight
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
