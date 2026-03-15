using System;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Internal;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests for the compiled-delegate cache in ExpressionCompiler.
/// Covers cap enforcement, cache-hit correctness, and concurrent access safety.
/// </summary>
public class ExpressionCacheTests
{
    [Fact]
    public void CompiledDelegateCache_PlateausAt512Entries()
    {
        var cache = ExpressionCompiler._compiledDelegateCache;
        var before = cache.Count;

        // Each Expression.Constant(n) produces a structurally distinct fingerprint,
        // so every iteration tries to add a new entry. After 512 unique additions
        // the cache evicts rather than growing further.
        var param = Expression.Parameter(typeof(int), "x");
        for (int n = before; n < before + 600; n++)
        {
            var body = Expression.Equal(param, Expression.Constant(n));
            var expr = Expression.Lambda<Func<int, bool>>(body, param);
            ExpressionCompiler.CompileExpression(expr);
        }

        Assert.True(cache.Count <= 512,
            $"Cache grew to {cache.Count}, expected ≤ 512");
    }

    [Fact]
    public void SameExpression_ReturnsSameDelegateInstance()
    {
        // const values are inlined by the compiler, so both lambdas produce identical
        // expression trees with the same constant node — same fingerprint, same cache slot.
        const int sentinel = 99_999_001;
        Expression<Func<int, bool>> expr1 = x => x == sentinel;
        Expression<Func<int, bool>> expr2 = x => x == sentinel;

        var d1 = ExpressionCompiler.CompileExpression(expr1);
        var d2 = ExpressionCompiler.CompileExpression(expr2);

        Assert.Same(d1, d2);
    }

    [Fact]
    public async Task CacheMissAfterEviction_StillProducesCorrectResult()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setupCmd = cn.CreateCommand();
        setupCmd.CommandText =
            "CREATE TABLE CacheTestItem (Id INTEGER PRIMARY KEY, Name TEXT);" +
            "INSERT INTO CacheTestItem VALUES (1,'Alpha');" +
            "INSERT INTO CacheTestItem VALUES (2,'Beta');";
        setupCmd.ExecuteNonQuery();

        // Flood the cache with 600 structurally distinct expressions to trigger eviction.
        var evictParam = Expression.Parameter(typeof(int), "x");
        for (int n = 1; n <= 600; n++)
        {
            var evictBody = Expression.Equal(evictParam, Expression.Constant(n + 1_000_000));
            var evictExpr = Expression.Lambda<Func<int, bool>>(evictBody, evictParam);
            ExpressionCompiler.CompileExpression(evictExpr);
        }

        // A freshly compiled real query must still work correctly after eviction pressure.
        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<CacheTestItem>().Where(i => i.Id == id));
        using var ctx = new DbContext(cn, new SqliteProvider());

        var result = await compiled(ctx, 1);
        Assert.Single(result);
        Assert.Equal("Alpha", result[0].Name);
    }

    [Fact]
    public async Task HighConcurrency_UniqueShapes_CacheRemainsWithinBounds()
    {
        const int tasks = 30;
        const int shapesPerTask = 25;
        var cache = ExpressionCompiler._compiledDelegateCache;

        var work = Enumerable.Range(0, tasks).Select(t => Task.Run(() =>
        {
            for (int n = 0; n < shapesPerTask; n++)
            {
                int captured = t * 10_000 + n + 2_000_000;
                var xp = Expression.Parameter(typeof(int), "x");
                var expr = Expression.Lambda<Func<int, bool>>(
                    Expression.Equal(xp, Expression.Constant(captured)), xp);
                var del = ExpressionCompiler.CompileExpression(expr);
                Assert.False(del(0));
                Assert.True(del(captured));
            }
        }));

        await Task.WhenAll(work);

        Assert.True(cache.Count <= 512,
            $"Cache exceeded cap under concurrency: {cache.Count}");
    }

    [Fact]
    public async Task ConcurrentSameExpression_AllReturnCorrectDelegate()
    {
        const int concurrency = 40;
        const int sentinel = 88_776_655;
        var delegates = new System.Delegate[concurrency];

        var work = Enumerable.Range(0, concurrency).Select(i => Task.Run(() =>
        {
            Expression<Func<int, bool>> expr = x => x == sentinel;
            delegates[i] = ExpressionCompiler.CompileExpression(expr);
        }));

        await Task.WhenAll(work);

        foreach (var d in delegates)
        {
            Assert.NotNull(d);
            var typed = (Func<int, bool>)d;
            Assert.False(typed(0));
            Assert.True(typed(sentinel));
        }
    }

    public class CacheTestItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }
}
