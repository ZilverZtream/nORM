using System.Collections.Generic;
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
/// Pins <c>SetProperty(r =&gt; r.Total, r =&gt; r.Items.Sum(i =&gt; i.Amount))</c>
/// — denormalising-rollup pattern where the new column value is the SUM of a
/// navigation collection. Should emit a correlated SUM subquery in the SET
/// clause. Silent-wrongness shapes: (1) value lambda translation throws,
/// (2) emits a literal that ignores child data, (3) emits an uncorrelated
/// SUM that gives every parent the global total.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqExecuteUpdateNavAggregateValueTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EunParent (Id INTEGER PRIMARY KEY, Total INTEGER NOT NULL);
            CREATE TABLE EunChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            -- Parent totals start at 0; expected after roll-up:
            --   Parent 1 -> 10 + 20 + 30 = 60
            --   Parent 2 -> 5
            --   Parent 3 -> 0 (no children)
            INSERT INTO EunParent VALUES (1, 0), (2, 0), (3, 0);
            INSERT INTO EunChild  VALUES
              (10, 1, 10),
              (11, 1, 20),
              (12, 1, 30),
              (13, 2,  5);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<EunParent>().HasKey(p => p.Id);
                mb.Entity<EunChild>().HasKey(c => c.Id);
                mb.Entity<EunParent>()
                    .HasMany(p => p.Items!)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task ExecuteUpdate_setproperty_to_nav_collection_sum_throws_actionable_guidance()
    {
        // Correlated subquery in the SET clause isn't supported yet; the
        // error must point the user at the two working paths (read+loop or
        // raw SQL) rather than just say "Call node not supported".
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(async () =>
        {
            await _ctx.Query<EunParent>()
                .ExecuteUpdateAsync(s => s.SetProperty(p => p.Total, p => p.Items!.Sum(i => i.Amount)));
        });
        Assert.Contains("Navigation-collection aggregate", ex.Message);
        Assert.Contains("Items.Sum", ex.Message);
        Assert.Contains("Workarounds", ex.Message);
        Assert.Contains("loop and", ex.Message);
        Assert.Contains("raw SQL", ex.Message);
    }

    [Fact]
    public async Task ExecuteUpdate_loop_workaround_writes_per_parent_total()
    {
        // Confirms the recommended workaround actually works: project the
        // rolled-up totals, then loop and update each entity individually.
        // SCV recognises the Select+Sum nav-aggregate shape (efba58f); the
        // direct Sum(selector) form is parked behind a separate path.
        var rollups = await _ctx.Query<EunParent>()
            .Select(p => new { p.Id, Total = p.Items!.Select(i => i.Amount).Sum() })
            .ToListAsync();
        foreach (var r in rollups)
        {
            var id = r.Id;
            var total = r.Total;
            await _ctx.Query<EunParent>()
                .Where(p => p.Id == id)
                .ExecuteUpdateAsync(s => s.SetProperty(p => p.Total, total));
        }

        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Total FROM EunParent ORDER BY Id";
        await using var rdr = await cmd.ExecuteReaderAsync();
        var rows = new List<(int Id, int Total)>();
        while (await rdr.ReadAsync()) rows.Add((rdr.GetInt32(0), rdr.GetInt32(1)));
        Assert.Equal(new[] { (1, 60), (2, 5), (3, 0) }, rows);
    }

    [Table("EunParent")]
    public sealed class EunParent
    {
        [Key] public int Id { get; set; }
        public int Total { get; set; }
        public List<EunChild>? Items { get; set; }
    }

    [Table("EunChild")]
    public sealed class EunChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
