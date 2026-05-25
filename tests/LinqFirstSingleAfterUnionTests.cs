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
/// Probe pin for FirstAsync / SingleAsync / FirstOrDefaultAsync over a
/// projected Union. SetOperationTranslator lifts the anonymous-type
/// projection from the left source onto the outer translator; the
/// terminal First/Single emits LIMIT 1 and must materialize the row
/// shape back to the anonymous type. Sister to the AnyAsync fix
/// (1033de9) which clears the projection for the EXISTS-scalar path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqFirstSingleAfterUnionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE FsuLeft  (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            CREATE TABLE FsuRight (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO FsuLeft  VALUES (1, 10), (2, 20);
            INSERT INTO FsuRight VALUES (1, 30), (2, 40);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<FsuLeft>().HasKey(i => i.Id);
                mb.Entity<FsuRight>().HasKey(i => i.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task FirstAsync_after_projected_Union_materializes_one_anonymous_row()
    {
        var left  = _ctx.Query<FsuLeft >().Select(p => new { V = p.V });
        var right = _ctx.Query<FsuRight>().Select(p => new { V = p.V });
        var row = await left.Union(right).FirstAsync();
        // Any row from the unioned set is acceptable; we only verify the
        // materializer round-tripped a real value (not throw, not default).
        Assert.Contains(row.V, new[] { 10, 20, 30, 40 });
    }

    [Table("FsuLeft")]
    public sealed class FsuLeft
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Table("FsuRight")]
    public sealed class FsuRight
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
