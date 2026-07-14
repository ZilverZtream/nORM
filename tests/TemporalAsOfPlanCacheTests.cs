using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The AsOf timestamp is captured from a local, but one query call site produces
/// ONE expression shape — so every execution shares a cached plan. The timestamp
/// must re-bind per execution: a plan that baked the first timestamp silently
/// answers every later AsOf query with the FIRST point in time's version.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalAsOfPlanCacheTests
{
    [Table("AsOfPc_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Fact]
    public async Task AsOf_timestamp_rebinds_across_cached_plans()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AsOfPc_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
        opts.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var row = new Row { Id = 1, V = 1 };
        ctx.Add(row);
        await ctx.SaveChangesAsync();
        await Task.Delay(150);
        var ts1 = DateTime.UtcNow;
        await Task.Delay(150);

        row.V = 2;
        await ctx.SaveChangesAsync();
        await Task.Delay(150);
        var ts2 = DateTime.UtcNow;
        await Task.Delay(150);

        row.V = 3;
        await ctx.SaveChangesAsync();

        // ONE call site → one expression shape → shared cached plan. The captured
        // timestamp differs per iteration and must re-bind.
        foreach (var (ts, expected) in new[] { (ts1, 1), (ts2, 2) })
        {
            var versions = await ctx.Query<Row>().AsOf(ts).Where(r => r.Id == 1).ToListAsync();
            Assert.True(versions.Count == 1,
                $"ts={ts:O} expected one version, got {versions.Count}");
            Assert.True(versions[0].V == expected,
                $"ts={ts:O}: expected V={expected} got V={versions[0].V} — cached plan replayed a stale AsOf timestamp");
        }
    }
}
