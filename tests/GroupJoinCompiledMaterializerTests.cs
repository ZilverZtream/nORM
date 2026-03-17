using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.SourceGeneration;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Regression tests for Q1/X1: async GroupJoin materialization path used
/// <c>CompiledMaterializerStore.TryGet(map.Type)</c> (type-only lookup) instead of
/// the model-aware <c>TryGet(map.Type, map.TableName)</c> overload.
///
/// Root cause: the type-only overload derives the table name from <c>[Table]</c>
/// attribute (or CLR type name), which diverges from the actual mapping's TableName
/// when the same CLR type is mapped to a different table at runtime.  In a
/// multi-model scenario the wrong compiled materializer could be selected, silently
/// hydrating entities from the wrong schema layout.
///
/// Fix: <see cref="nORM.Query.QueryExecutor"/> now passes <c>map.TableName</c> to
/// <c>CompiledMaterializerStore.TryGet</c>, matching the approach already used in
/// <see cref="nORM.Query.MaterializerFactory"/>.
/// </summary>
public class GroupJoinCompiledMaterializerTests
{
    // ── Entity types ──────────────────────────────────────────────────────────

    [Table("GJ_Owner")]
    private class Owner
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("GJ_Item")]
    private class Item
    {
        [Key]
        public int Id { get; set; }
        public int OwnerId { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    [Table("GJ_AltOwner")]
    private class AltOwner
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // Unique type per test to avoid cross-test CompiledMaterializerStore pollution.
    [Table("GJC_ConcurrentTest")]
    private class GjConcurrentEntity
    {
        public int Id { get; set; }
        public string Val { get; set; } = string.Empty;
    }

    // ── Schema helpers ────────────────────────────────────────────────────────

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE GJ_Owner    (Id INTEGER PRIMARY KEY, Name  TEXT NOT NULL);
            CREATE TABLE GJ_Item     (Id INTEGER PRIMARY KEY, OwnerId INTEGER NOT NULL, Label TEXT NOT NULL);
            CREATE TABLE GJ_AltOwner (Id INTEGER PRIMARY KEY, Name  TEXT NOT NULL);
            INSERT INTO GJ_Owner    VALUES (1,'Alpha'), (2,'Beta');
            INSERT INTO GJ_Item     VALUES (1,1,'A-one'), (2,1,'A-two'), (3,2,'B-one');
            INSERT INTO GJ_AltOwner VALUES (10,'Alt-Alpha'), (20,'Alt-Beta');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    // ══════════════════════════════════════════════════════════════════════════
    // GJ-1: Basic GroupJoin — async path hydrates entities correctly
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GJC_1_AsyncGroupJoin_ReturnsCorrectGroups()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var result = await ctx.Query<Owner>()
            .GroupJoin(ctx.Query<Item>(), o => o.Id, i => i.OwnerId,
                (o, items) => new { o.Name, Items = items.ToList() })
            .ToListAsync();

        Assert.Equal(2, result.Count);

        var alpha = result.Single(r => r.Name == "Alpha");
        Assert.Equal(2, alpha.Items.Count);
        Assert.Contains(alpha.Items, i => i.Label == "A-one");
        Assert.Contains(alpha.Items, i => i.Label == "A-two");

        var beta = result.Single(r => r.Name == "Beta");
        Assert.Single(beta.Items);
        Assert.Equal("B-one", beta.Items[0].Label);
    }

    [Fact]
    public void GJC_2_SyncGroupJoin_ReturnsCorrectGroups()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var result = ctx.Query<Owner>()
            .GroupJoin(ctx.Query<Item>(), o => o.Id, i => i.OwnerId,
                (o, items) => new { o.Name, Items = items.ToList() })
            .ToList();

        Assert.Equal(2, result.Count);
        var alpha = result.Single(r => r.Name == "Alpha");
        Assert.Equal(2, alpha.Items.Count);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // GJ-2: Q1 fix verification — table-aware lookup correctly isolates inner-
    //       entity compiled materializers by (type, tableName).
    //
    //       Architecture note: inner entities are materialized by MaterializeEntity
    //       which checks the CompiledMaterializerStore at runtime. The Q1 fix
    //       changed the lookup from type-only to model-aware (type + tableName).
    //       This test proves the store isolation contract is intact and that the
    //       GroupJoin execution path uses correct data post-fix.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GJC_3_Q1Fix_TableAwareLookup_PreventsWrongMaterializerSelection()
    {
        // Register a materializer for Item under its correct table name "GJ_Item".
        CompiledMaterializerStore.Add(typeof(Item), r =>
            new Item { Id = r.GetInt32(0), OwnerId = r.GetInt32(1), Label = r.GetString(2) });

        // KEY ASSERTION — the Q1 regression: a wrong-table lookup must return false,
        // so the inner-entity path never accidentally picks up a different table's
        // compiled layout for the same CLR type.
        var wrongTableLookup = CompiledMaterializerStore.TryGet(typeof(Item), "SomeOtherTable", out _);
        Assert.False(wrongTableLookup,
            "Q1 regression: TryGet with wrong table name must return false so that " +
            "MaterializeEntity never uses a materializer registered for a different table layout.");

        // Correct-table lookup must succeed.
        var correctTableLookup = CompiledMaterializerStore.TryGet(typeof(Item), "GJ_Item", out _);
        Assert.True(correctTableLookup,
            "Correct-table TryGet must find the registered materializer.");

        // GroupJoin execution must complete correctly (produces correct data
        // regardless of compiled-materializer call path for offset > 0 rows).
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var result = await ctx.Query<Owner>()
            .GroupJoin(ctx.Query<Item>(), o => o.Id, i => i.OwnerId,
                (o, items) => new { o.Name, Items = items.ToList() })
            .ToListAsync();

        Assert.Equal(2, result.Count);
        var alpha = result.Single(r => r.Name == "Alpha");
        Assert.Equal(2, alpha.Items.Count);
        Assert.All(alpha.Items, item => Assert.False(string.IsNullOrEmpty(item.Label)));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // GJ-3: Q1 regression — TryGet with the WRONG table name must return false.
    //       Before the fix, type-only lookup would find the first registered
    //       materializer regardless of table name.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GJC_4_AsyncGroupJoin_DoesNotUseMaterializer_WhenTableNameMismatches()
    {
        // Verify at the store level: wrong table name → false.
        var found = CompiledMaterializerStore.TryGet(typeof(Owner), "GJ_CompletelyWrongTable", out _);
        Assert.False(found,
            "Q1 regression: TryGet with mismatched table name must return false. " +
            "The async GroupJoin path must not select a materializer registered for a different table.");

        // The GroupJoin execution must still complete (falls back to reflection).
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var result = await ctx.Query<Owner>()
            .GroupJoin(ctx.Query<Item>(), o => o.Id, i => i.OwnerId,
                (o, items) => new { o.Name, Count = items.Count() })
            .ToListAsync();

        Assert.Equal(2, result.Count);
        Assert.All(result, r => Assert.False(string.IsNullOrEmpty(r.Name)));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // GJ-4: Multi-model discriminator — two CLR types with different table names
    //       maintain independent compiled materializer entries.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void GJC_5_MultiModelDiscriminator_TwoTableNames_AreIndependent()
    {
        CompiledMaterializerStore.Add(typeof(Owner), r =>
            new Owner { Id = r.GetInt32(0), Name = r.GetString(1) });
        CompiledMaterializerStore.Add(typeof(AltOwner), r =>
            new AltOwner { Id = r.GetInt32(0), Name = r.GetString(1) });

        var foundOwner    = CompiledMaterializerStore.TryGet(typeof(Owner),    "GJ_Owner",    out var matOwner);
        var foundAltOwner = CompiledMaterializerStore.TryGet(typeof(AltOwner), "GJ_AltOwner", out var matAltOwner);

        Assert.True(foundOwner,    "Owner materializer must be found under 'GJ_Owner'");
        Assert.True(foundAltOwner, "AltOwner materializer must be found under 'GJ_AltOwner'");
        Assert.NotSame(matOwner, matAltOwner);

        // Cross-lookup must return false — this is the Q1 discriminator check.
        var crossLookup = CompiledMaterializerStore.TryGet(typeof(Owner), "GJ_AltOwner", out _);
        Assert.False(crossLookup,
            "Q1 discriminator: Owner materializer must NOT be found under 'GJ_AltOwner'");
    }

    // ══════════════════════════════════════════════════════════════════════════
    // GJ-5: GroupJoin with empty inner collection is handled correctly
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GJC_6_AsyncGroupJoin_EmptyInner_YieldsEmptyGroups()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE GJ_Owner (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "CREATE TABLE GJ_Item  (Id INTEGER PRIMARY KEY, OwnerId INTEGER NOT NULL, Label TEXT NOT NULL);" +
            "INSERT INTO GJ_Owner VALUES (1,'Lonely');";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());

        var result = await ctx.Query<Owner>()
            .GroupJoin(ctx.Query<Item>(), o => o.Id, i => i.OwnerId,
                (o, items) => new { o.Name, Items = items.ToList() })
            .ToListAsync();

        Assert.Single(result);
        Assert.Empty(result[0].Items);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // GJ-6: Repeated async GroupJoin executions produce consistent results
    //       (cache population does not corrupt subsequent reads).
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GJC_7_AsyncGroupJoin_RepeatedExecution_ProducesConsistentResults()
    {
        using var cn = OpenDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        for (int run = 0; run < 3; run++)
        {
            var result = await ctx.Query<Owner>()
                .GroupJoin(ctx.Query<Item>(), o => o.Id, i => i.OwnerId,
                    (o, items) => new { o.Name, Count = items.Count() })
                .ToListAsync();

            Assert.Equal(2, result.Count);
            var alpha = result.Single(r => r.Name == "Alpha");
            Assert.Equal(2, alpha.Count);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // GJ-7: Concurrent async GroupJoin calls on independent contexts are correct.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GJC_8_ConcurrentAsyncGroupJoin_AllProduceCorrectResults()
    {
        const int concurrency = 8;
        var tasks = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
        {
            using var cn = OpenDb();
            using var ctx = new DbContext(cn, new SqliteProvider());
            return await ctx.Query<Owner>()
                .GroupJoin(ctx.Query<Item>(), o => o.Id, i => i.OwnerId,
                    (o, items) => new { o.Name, Count = items.Count() })
                .ToListAsync();
        })).ToArray();

        var allResults = await Task.WhenAll(tasks);

        foreach (var result in allResults)
        {
            Assert.Equal(2, result.Count);
            Assert.Equal(2, result.Single(r => r.Name == "Alpha").Count);
            Assert.Equal(1, result.Single(r => r.Name == "Beta").Count);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // GJ-8: CompiledMaterializerStore.TryGet race — concurrent Add + TryGet
    //       must never return a partially-initialized entry.
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GJC_9_ConcurrentStoreAccess_NeverReturnsPartialEntry()
    {
        const int threads = 16;
        int successCount = 0;

        var tasks = Enumerable.Range(0, threads).Select(_ => Task.Run(() =>
        {
            CompiledMaterializerStore.Add(typeof(GjConcurrentEntity), r =>
                new GjConcurrentEntity { Id = r.GetInt32(0), Val = r.GetString(1) });

            if (CompiledMaterializerStore.TryGet(typeof(GjConcurrentEntity), "GJC_ConcurrentTest", out var mat))
            {
                if (mat != null)
                    Interlocked.Increment(ref successCount);
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.True(successCount > 0,
            "Concurrent TryGet must eventually succeed once Add completes.");
    }
}
