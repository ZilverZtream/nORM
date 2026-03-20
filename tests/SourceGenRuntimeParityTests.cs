using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Section 8 gate 4.0→4.5: Source-generated (compiled) vs runtime-query equivalence tests.
/// Verifies that Norm.CompileQuery (precompiled path) and runtime LINQ produce identical
/// results for the same input data, using SQLite in-memory databases.
/// </summary>
public class SourceGenRuntimeParityTests
{
    // ── Entity ────────────────────────────────────────────────────────────────

    public class ParityItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
        public string? Category { get; set; }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection CreateDb(int rowCount = 10)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE ParityItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL, Category TEXT);";
        cmd.ExecuteNonQuery();

        for (int i = 1; i <= rowCount; i++)
        {
            using var ins = cn.CreateCommand();
            var cat = i % 3 == 0 ? "NULL" : $"'cat{i % 3}'";
            ins.CommandText = $"INSERT INTO ParityItem VALUES ({i}, 'item{i}', {i * 10}, {cat})";
            ins.ExecuteNonQuery();
        }

        return cn;
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 1. Basic select all
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_SelectAll_SameRowCountAndValues()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Compiled: select all by passing a dummy param
        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>());

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>().ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
            Assert.Equal(runtimeResult[i].Value, compiledResult[i].Value);
            Assert.Equal(runtimeResult[i].Category, compiledResult[i].Category);
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 2. Select with Where
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_WhereFilter_SameFilteredResults()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int minVal) =>
            c.Query<ParityItem>().Where(x => x.Value > minVal));

        var threshold = 50;
        var compiledResult = await compiled(ctx, threshold);
        var runtimeResult = await ctx.Query<ParityItem>()
            .Where(x => x.Value > threshold)
            .ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        Assert.True(compiledResult.Count > 0, "Filter should match at least one row");
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
            Assert.Equal(runtimeResult[i].Value, compiledResult[i].Value);
        }
    }

    [Fact]
    public async Task Parity_WhereEquality_SameResults()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<ParityItem>().Where(x => x.Id == id));

        for (int id = 1; id <= 10; id++)
        {
            var compiledResult = await compiled(ctx, id);
            var runtimeResult = await ctx.Query<ParityItem>()
                .Where(x => x.Id == id)
                .ToListAsync();

            Assert.Equal(runtimeResult.Count, compiledResult.Count);
            Assert.Single(compiledResult);
            Assert.Equal(runtimeResult[0].Id, compiledResult[0].Id);
            Assert.Equal(runtimeResult[0].Name, compiledResult[0].Name);
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 3. OrderBy + Skip + Take (paging parity)
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_OrderBySkipTake_SamePagingResults()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Compiled: page 2 (skip 3, take 3), ordered by Value descending
        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().OrderByDescending(x => x.Value).Skip(3).Take(3));

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>()
            .OrderByDescending(x => x.Value)
            .Skip(3)
            .Take(3)
            .ToListAsync();

        Assert.Equal(3, compiledResult.Count);
        Assert.Equal(3, runtimeResult.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Value, compiledResult[i].Value);
        }
    }

    [Fact]
    public async Task Parity_OrderByAsc_TakeN_SameResults()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().OrderBy(x => x.Id).Take(5));

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>()
            .OrderBy(x => x.Id)
            .Take(5)
            .ToListAsync();

        Assert.Equal(5, compiledResult.Count);
        Assert.Equal(5, runtimeResult.Count);
        for (int i = 0; i < 5; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 4. Count aggregate
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_Count_CompiledListCountMatchesRuntimeCountAsync()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Compiled query returns List<T>; count its elements
        var compiled = Norm.CompileQuery((DbContext c, int minVal) =>
            c.Query<ParityItem>().Where(x => x.Value > minVal));

        var threshold = 30;
        var compiledResult = await compiled(ctx, threshold);
        var runtimeCount = await ctx.Query<ParityItem>()
            .Where(x => x.Value > threshold)
            .CountAsync();

        Assert.Equal(runtimeCount, compiledResult.Count);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 5. FirstOrDefault
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_FirstOrDefault_ExistingRow_SameEntity()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<ParityItem>().Where(x => x.Id == id));

        var compiledResult = await compiled(ctx, 1);
        var runtimeResult = await ctx.Query<ParityItem>()
            .Where(x => x.Id == 1)
            .FirstOrDefaultAsync();

        Assert.Single(compiledResult);
        Assert.NotNull(runtimeResult);
        Assert.Equal(runtimeResult!.Id, compiledResult[0].Id);
        Assert.Equal(runtimeResult.Name, compiledResult[0].Name);
        Assert.Equal(runtimeResult.Value, compiledResult[0].Value);
    }

    [Fact]
    public async Task Parity_FirstOrDefault_NonExistentRow_BothEmpty()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<ParityItem>().Where(x => x.Id == id));

        var compiledResult = await compiled(ctx, 9999);
        var runtimeResult = await ctx.Query<ParityItem>()
            .Where(x => x.Id == 9999)
            .FirstOrDefaultAsync();

        Assert.Empty(compiledResult);
        Assert.Null(runtimeResult);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 6. Select projection
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_SelectProjection_SameProjectedValues()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Compiled query projects subset of columns via anonymous type
        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().OrderBy(x => x.Id).Select(x => new { x.Id, x.Name }));

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>()
            .OrderBy(x => x.Id)
            .Select(x => new { x.Id, x.Name })
            .ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 7. Where with null comparison
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_WhereNullCategory_SameIsNullBehavior()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Rows where Category IS NULL (every 3rd row: id 3, 6, 9)
        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().Where(x => x.Category == null));

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>()
            .Where(x => x.Category == null)
            .ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        Assert.True(compiledResult.Count > 0, "Should find null-category rows");
        foreach (var item in compiledResult)
            Assert.Null(item.Category);
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
        }
    }

    [Fact]
    public async Task Parity_WhereNotNullCategory_SameResults()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().Where(x => x.Category != null));

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>()
            .Where(x => x.Category != null)
            .ToListAsync();

        Assert.Equal(runtimeResult.Count, compiledResult.Count);
        Assert.True(compiledResult.Count > 0, "Should find non-null-category rows");
        foreach (var item in compiledResult)
            Assert.NotNull(item.Category);
        for (int i = 0; i < runtimeResult.Count; i++)
        {
            Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
            Assert.Equal(runtimeResult[i].Category, compiledResult[i].Category);
        }
    }

    // ════════════════════════════════════════════════════════════════════════════
    // 8. Multiple queries same context
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_MultipleQueriesSameContext_BothCorrect()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<ParityItem>().Where(x => x.Id == id));

        // Run compiled first
        var compiledResult1 = await compiled(ctx, 1);
        var compiledResult5 = await compiled(ctx, 5);

        // Then runtime on the same context
        var runtimeResult1 = await ctx.Query<ParityItem>()
            .Where(x => x.Id == 1)
            .ToListAsync();
        var runtimeResult5 = await ctx.Query<ParityItem>()
            .Where(x => x.Id == 5)
            .ToListAsync();

        // Compiled and runtime must agree for both parameter values
        Assert.Single(compiledResult1);
        Assert.Single(runtimeResult1);
        Assert.Equal(runtimeResult1[0].Id, compiledResult1[0].Id);
        Assert.Equal(runtimeResult1[0].Name, compiledResult1[0].Name);

        Assert.Single(compiledResult5);
        Assert.Single(runtimeResult5);
        Assert.Equal(runtimeResult5[0].Id, compiledResult5[0].Id);
        Assert.Equal(runtimeResult5[0].Name, compiledResult5[0].Name);
    }

    [Fact]
    public async Task Parity_InterleavedCompiledAndRuntime_AllCorrect()
    {
        using var cn = CreateDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiledById = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<ParityItem>().Where(x => x.Id == id));

        var compiledByVal = Norm.CompileQuery((DbContext c, int minVal) =>
            c.Query<ParityItem>().Where(x => x.Value >= minVal));

        // Interleave: compiled → runtime → compiled → runtime
        var cr1 = await compiledById(ctx, 3);
        var rr1 = await ctx.Query<ParityItem>().Where(x => x.Id == 3).ToListAsync();
        Assert.Single(cr1);
        Assert.Single(rr1);
        Assert.Equal(rr1[0].Id, cr1[0].Id);

        var cr2 = await compiledByVal(ctx, 90);
        var rr2 = await ctx.Query<ParityItem>().Where(x => x.Value >= 90).ToListAsync();
        Assert.Equal(rr2.Count, cr2.Count);
        for (int i = 0; i < rr2.Count; i++)
            Assert.Equal(rr2[i].Id, cr2[i].Id);
    }

    // ════════════════════════════════════════════════════════════════════════════
    // Additional parity tests
    // ════════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Parity_EmptyTable_BothReturnEmpty()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE ParityItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL, Category TEXT)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>());

        var compiledResult = await compiled(ctx, 0);
        var runtimeResult = await ctx.Query<ParityItem>().ToListAsync();

        Assert.Empty(compiledResult);
        Assert.Empty(runtimeResult);
    }

    [Fact]
    public async Task Parity_RepeatedCompiledCalls_StableResults()
    {
        using var cn = CreateDb(5);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int _dummy) =>
            c.Query<ParityItem>().OrderBy(x => x.Id));

        var runtimeResult = await ctx.Query<ParityItem>().OrderBy(x => x.Id).ToListAsync();

        // Run compiled 20 times — each invocation must yield the same stable result
        for (int iter = 0; iter < 20; iter++)
        {
            var compiledResult = await compiled(ctx, iter);
            Assert.Equal(runtimeResult.Count, compiledResult.Count);
            for (int i = 0; i < runtimeResult.Count; i++)
            {
                Assert.Equal(runtimeResult[i].Id, compiledResult[i].Id);
                Assert.Equal(runtimeResult[i].Name, compiledResult[i].Name);
                Assert.Equal(runtimeResult[i].Value, compiledResult[i].Value);
            }
        }
    }
}
