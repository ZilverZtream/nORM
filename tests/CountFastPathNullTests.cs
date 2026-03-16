using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Q1 regression — count fast-path cache key must distinguish null vs non-null
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Regression tests for Q1: the count fast-path SQL cache key was using only the
/// member name, causing col==null and col==value to share the same cache entry.
/// The first query to execute primes the cache; subsequent queries with the opposite
/// null-shape would reuse the wrong SQL and return incorrect counts.
/// </summary>
public class CountFastPathNullTests
{
    [Table("CountNullItem")]
    private class CountNullItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string? NullableTag { get; set; }
        public int Value { get; set; }
    }

    private static (SqliteConnection cn, DbContext ctx) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CountNullItem (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                NullableTag TEXT,
                Value INTEGER NOT NULL DEFAULT 0
            );";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── TryDirectCountAsync path (.Where(pred).CountAsync()) ──────────────────

    [Fact]
    public async Task DirectCount_NullFirst_ThenNonNull_CorrectCounts()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new CountNullItem { NullableTag = null,    Value = 1 });
        await ctx.InsertAsync(new CountNullItem { NullableTag = null,    Value = 2 });
        await ctx.InsertAsync(new CountNullItem { NullableTag = "hello", Value = 3 });

        // Prime cache with IS NULL query.
        var nullCount = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == null).CountAsync();
        Assert.Equal(2, nullCount);

        // Must NOT reuse IS NULL SQL — must emit = @p0.
        var helloCount = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == "hello").CountAsync();
        Assert.Equal(1, helloCount);

        // Reverse: non-null value with zero matches.
        var missingCount = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == "missing").CountAsync();
        Assert.Equal(0, missingCount);

        // IS NULL should still work after both shapes are in the cache.
        var nullCount2 = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == null).CountAsync();
        Assert.Equal(2, nullCount2);
    }

    [Fact]
    public async Task DirectCount_NonNullFirst_ThenNull_CorrectCounts()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new CountNullItem { NullableTag = "a",  Value = 1 });
        await ctx.InsertAsync(new CountNullItem { NullableTag = "a",  Value = 2 });
        await ctx.InsertAsync(new CountNullItem { NullableTag = null, Value = 3 });

        // Prime cache with parameterized = @p0 query.
        var aCount = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == "a").CountAsync();
        Assert.Equal(2, aCount);

        // Must NOT reuse = @p0 SQL — must emit IS NULL.
        var nullCount = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == null).CountAsync();
        Assert.Equal(1, nullCount);

        // Sanity: parameterized path still works after IS NULL was also cached.
        var aCount2 = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == "a").CountAsync();
        Assert.Equal(2, aCount2);
    }

    [Fact]
    public async Task DirectCount_AlternatingNullNonNull_ConsistentResults()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        // 3 null rows, 2 "x" rows, 1 "y" row.
        for (int i = 0; i < 3; i++)
            await ctx.InsertAsync(new CountNullItem { NullableTag = null, Value = i });
        for (int i = 0; i < 2; i++)
            await ctx.InsertAsync(new CountNullItem { NullableTag = "x", Value = i });
        await ctx.InsertAsync(new CountNullItem { NullableTag = "y", Value = 0 });

        for (int round = 0; round < 4; round++)
        {
            Assert.Equal(3, await ctx.Query<CountNullItem>().Where(x => x.NullableTag == null).CountAsync());
            Assert.Equal(2, await ctx.Query<CountNullItem>().Where(x => x.NullableTag == "x").CountAsync());
            Assert.Equal(1, await ctx.Query<CountNullItem>().Where(x => x.NullableTag == "y").CountAsync());
        }
    }

    // ── TryGetCountQuery path (Queryable.Count(Where(source, pred)) shape) ─────

    [Fact]
    public async Task LongCount_NullFirst_ThenNonNull_CorrectCounts()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new CountNullItem { NullableTag = null,  Value = 1 });
        await ctx.InsertAsync(new CountNullItem { NullableTag = "tag", Value = 2 });

        // Prime with IS NULL.
        var nullCount = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == null).CountAsync();
        // Then parameterized.
        var tagCount  = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == "tag").CountAsync();

        Assert.Equal(1, nullCount);
        Assert.Equal(1, tagCount);
    }

    [Fact]
    public async Task LongCount_NonNullFirst_ThenNull_CorrectCounts()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        await ctx.InsertAsync(new CountNullItem { NullableTag = null,  Value = 1 });
        await ctx.InsertAsync(new CountNullItem { NullableTag = "tag", Value = 2 });

        // Prime with parameterized.
        var tagCount  = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == "tag").CountAsync();
        var nullCount = await ctx.Query<CountNullItem>().Where(x => x.NullableTag == null).CountAsync();

        Assert.Equal(1, tagCount);
        Assert.Equal(1, nullCount);
    }

    // ── All-null / all-non-null edge cases ────────────────────────────────────

    [Fact]
    public async Task DirectCount_AllRowsNull_NonNullQueryReturnsZero()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        for (int i = 0; i < 5; i++)
            await ctx.InsertAsync(new CountNullItem { NullableTag = null, Value = i });

        // Prime with IS NULL (all rows match).
        Assert.Equal(5, await ctx.Query<CountNullItem>().Where(x => x.NullableTag == null).CountAsync());

        // Non-null query must return 0, not 5.
        Assert.Equal(0, await ctx.Query<CountNullItem>().Where(x => x.NullableTag == "something").CountAsync());
    }

    [Fact]
    public async Task DirectCount_AllRowsNonNull_NullQueryReturnsZero()
    {
        var (cn, ctx) = Build();
        await using var _ = ctx; using var __ = cn;

        for (int i = 0; i < 5; i++)
            await ctx.InsertAsync(new CountNullItem { NullableTag = $"tag{i}", Value = i });

        // Prime with parameterized (1 row matches tag0).
        Assert.Equal(1, await ctx.Query<CountNullItem>().Where(x => x.NullableTag == "tag0").CountAsync());

        // Null query must return 0, not 1 or 5.
        Assert.Equal(0, await ctx.Query<CountNullItem>().Where(x => x.NullableTag == null).CountAsync());
    }
}
