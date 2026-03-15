using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class ConcurrencyExecutionStressTests
{
    [Table("StressItem")]
    private sealed class StressItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Score { get; set; }
    }

    // ── Plan cache concurrency ────────────────────────────────────────────

    /// <summary>
    /// Many concurrent queries against independent connections must all produce correct
    /// results without corrupting the shared static plan cache.
    /// </summary>
    [Fact]
    public async Task ConcurrentQueries_IndependentConnections_AllReturnCorrectResults()
    {
        const int degree = 50;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, degree).Select(async i =>
        {
            using var cn = CreateConnection();
            await using var ctx = new DbContext(cn, new SqliteProvider());
            var results = await ctx.Query<StressItem>().Where(x => x.Id == 1).ToListAsync();
            if (results.Count != 1 || results[0].Name != "Alpha")
                errors.Add($"Task {i}: unexpected result");
        });

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    /// <summary>
    /// 50 concurrent CountAsync calls on the same DbContext instance must all return the
    /// correct count and must not throw due to command state races.
    /// </summary>
    [Fact]
    public async Task ConcurrentCountAsync_SameContext_AllReturnCorrectCount()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        const int degree = 50;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, degree).Select(async i =>
        {
            try
            {
                var count = await ctx.Query<StressItem>().CountAsync();
                if (count != 3)
                    errors.Add($"Task {i}: expected 3, got {count}");
            }
            catch (Exception ex)
            {
                errors.Add($"Task {i}: {ex.GetType().Name}: {ex.Message}");
            }
        });

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    /// <summary>
    /// Concurrent materializer cache access from 30 parallel tasks must all produce
    /// correctly hydrated entities — no partial-write corruption of the static cache.
    /// </summary>
    [Fact]
    public async Task ConcurrentMaterializerCacheAccess_AllHydrationCorrect()
    {
        const int degree = 30;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, degree).Select(async i =>
        {
            using var cn = CreateConnection();
            await using var ctx = new DbContext(cn, new SqliteProvider());
            var all = await ctx.Query<StressItem>().ToListAsync();
            if (all.Count != 3)
                errors.Add($"Task {i}: expected 3 items, got {all.Count}");
            else if (all[0].Id == 0 || string.IsNullOrEmpty(all[0].Name))
                errors.Add($"Task {i}: item not fully hydrated");
        });

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    /// <summary>
    /// 20 concurrent compiled-query executions across separate contexts must each produce
    /// correct results — the compiled plan cache must not confuse contexts.
    /// </summary>
    [Fact]
    public async Task ConcurrentCompiledQuery_SeparateContexts_EachReceivesCorrectRows()
    {
        const int degree = 20;
        var errors = new ConcurrentBag<string>();
        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<StressItem>().Where(x => x.Id == id));

        var tasks = Enumerable.Range(0, degree).Select(async i =>
        {
            using var cn = CreateConnection();
            await using var ctx = new DbContext(cn, new SqliteProvider());
            var results = await compiled(ctx, 2);
            if (results.Count != 1 || results[0].Name != "Beta")
                errors.Add($"Task {i}: wrong result");
        });

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    /// <summary>
    /// Mixed concurrent workload: simultaneous queries, counts, and fast-path takes on
    /// independent connections must all return consistent data.
    /// </summary>
    [Fact]
    public async Task ConcurrentMixedWorkload_AllResultsConsistent()
    {
        const int degree = 60;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, degree).Select(async i =>
        {
            using var cn = CreateConnection();
            await using var ctx = new DbContext(cn, new SqliteProvider());
            try
            {
                if (i % 3 == 0)
                {
                    var count = await ctx.Query<StressItem>().CountAsync();
                    if (count != 3) errors.Add($"[count] Task {i}: {count}");
                }
                else if (i % 3 == 1)
                {
                    var top = await ctx.Query<StressItem>().Take(2).ToListAsync();
                    if (top.Count != 2) errors.Add($"[take] Task {i}: {top.Count}");
                }
                else
                {
                    var where = await ctx.Query<StressItem>().Where(x => x.Score == 20).ToListAsync();
                    if (where.Count != 1) errors.Add($"[where] Task {i}: {where.Count}");
                }
            }
            catch (Exception ex)
            {
                errors.Add($"Task {i}: {ex.GetType().Name}: {ex.Message}");
            }
        });

        await Task.WhenAll(tasks);
        Assert.Empty(errors);
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static SqliteConnection CreateConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE StressItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL);" +
            "INSERT INTO StressItem VALUES (1, 'Alpha', 10);" +
            "INSERT INTO StressItem VALUES (2, 'Beta', 20);" +
            "INSERT INTO StressItem VALUES (3, 'Gamma', 30);";
        cmd.ExecuteNonQuery();
        return cn;
    }
}
