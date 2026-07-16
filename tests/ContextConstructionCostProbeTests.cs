using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class ContextConstructionCostProbeTests
{
    [Table("CtorCostA")] public class A { [Key] public int Id { get; set; } public string N { get; set; } = ""; public B? Child { get; set; } }
    [Table("CtorCostB")] public class B { [Key] public int Id { get; set; } public int AId { get; set; } public int V { get; set; } }

    [Fact]
    public async System.Threading.Tasks.Task Fresh_context_per_query_vs_reused_context()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CtorCostA (Id INTEGER PRIMARY KEY, N TEXT NOT NULL);" +
                "CREATE TABLE CtorCostB (Id INTEGER PRIMARY KEY, AId INTEGER NOT NULL, V INTEGER NOT NULL);" +
                "INSERT INTO CtorCostA VALUES (1,'x'),(2,'y');" +
                "INSERT INTO CtorCostB VALUES (1,1,10),(2,2,20);";
            cmd.ExecuteNonQuery();
        }

        DbContextOptions MakeOpts() => new()
        {
            OnModelCreating = mb =>
            {
                mb.Entity<A>().HasKey(a => a.Id);
                mb.Entity<B>().HasKey(b => b.Id);
            }
        };

        const int iterations = 300;

        // Warm-up both paths once (JIT, static caches).
        await using (var warm = new DbContext(cn, new SqliteProvider(), MakeOpts(), ownsConnection: false))
            _ = await warm.Query<A>().Where(a => a.Id == 1).ToListAsync();

        // Path 1: fresh context per query (per-request pattern without pooling).
        var swFresh = Stopwatch.StartNew();
        for (var i = 0; i < iterations; i++)
        {
            await using var ctx = new DbContext(cn, new SqliteProvider(), MakeOpts(), ownsConnection: false);
            _ = await ctx.Query<A>().Where(a => a.Id == 1).ToListAsync();
        }
        swFresh.Stop();

        // Path 2: one warm context reused (what pooling approximates).
        var reused = new DbContext(cn, new SqliteProvider(), MakeOpts(), ownsConnection: false);
        await using (reused)
        {
            _ = await reused.Query<A>().Where(a => a.Id == 1).ToListAsync();
            var swReused = Stopwatch.StartNew();
            for (var i = 0; i < iterations; i++)
                _ = await reused.Query<A>().Where(a => a.Id == 1).ToListAsync();
            swReused.Stop();

            Console.WriteLine($"fresh-per-query: {swFresh.Elapsed.TotalMilliseconds / iterations:F3} ms/op");
            Console.WriteLine($"reused-context: {swReused.Elapsed.TotalMilliseconds / iterations:F3} ms/op");
            Console.WriteLine($"pooling would save ~{(swFresh.Elapsed.TotalMilliseconds - swReused.Elapsed.TotalMilliseconds) / iterations:F3} ms/op");
        }
    }
}
