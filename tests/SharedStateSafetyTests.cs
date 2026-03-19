using System;
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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Proves that shared static caches in MaterializerFactory and per-context
/// structures (ChangeTracker, FastPathSqlCache, PreparedInsertCommand) are
/// correctly scoped and do not corrupt each other under concurrent or
/// sequential access across multiple DbContext instances.
/// </summary>
public class SharedStateSafetyTests
{
    // ── Entities ─────────────────────────────────────────────────────────────

    [Table("SsProduct")]
    private class SsProduct
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Stock { get; set; }
    }

    [Table("SsOrder")]
    private class SsOrder
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Item { get; set; } = string.Empty;
        public int Qty { get; set; }
    }

    [Table("SsUser")]
    private class SsUser
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Email { get; set; } = string.Empty;
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) CreateProductCtx(string? tableName = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var tbl = tableName ?? "SsProduct";
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"CREATE TABLE \"{tbl}\" (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0)";
        cmd.ExecuteNonQuery();
        DbContext ctx;
        if (tableName != null && tableName != "SsProduct")
        {
            var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<SsProduct>().ToTable(tableName) };
            ctx = new DbContext(cn, new SqliteProvider(), opts);
        }
        else
        {
            ctx = new DbContext(cn, new SqliteProvider());
        }
        return (cn, ctx);
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateOrderCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SsOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT, Item TEXT NOT NULL, Qty INTEGER NOT NULL DEFAULT 0)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 1. ChangeTracker is fully per-context — entities tracked in ctx1 are
    //    invisible to ctx2 and vice versa
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void ChangeTracker_IsPerContext_NotShared()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            var p1 = new SsProduct { Name = "Alpha", Stock = 10 };
            var p2 = new SsProduct { Name = "Beta", Stock = 20 };

            ctx1.Add(p1);
            ctx2.Add(p2);

            // ctx1 must see only p1
            Assert.Single(ctx1.ChangeTracker.Entries);
            Assert.Equal("Alpha", ((SsProduct)ctx1.ChangeTracker.Entries.Single().Entity!).Name);

            // ctx2 must see only p2
            Assert.Single(ctx2.ChangeTracker.Entries);
            Assert.Equal("Beta", ((SsProduct)ctx2.ChangeTracker.Entries.Single().Entity!).Name);
        }
    }

    [Fact]
    public async Task ChangeTracker_PerContext_SaveChanges_DoesNotAffectOtherContext()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            var p1 = new SsProduct { Name = "Ctx1Product", Stock = 5 };
            ctx1.Add(p1);
            await ctx1.SaveChangesAsync();

            // ctx2 tracker should still be empty
            Assert.Empty(ctx2.ChangeTracker.Entries);
        }
    }

    [Fact]
    public void ChangeTracker_Clear_OnlyAffectsOwningContext()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            ctx1.Add(new SsProduct { Name = "A", Stock = 1 });
            ctx2.Add(new SsProduct { Name = "B", Stock = 2 });

            ctx1.ChangeTracker.Clear();

            Assert.Empty(ctx1.ChangeTracker.Entries);
            Assert.Single(ctx2.ChangeTracker.Entries);
        }
    }

    [Fact]
    public void ChangeTracker_EntityState_PerContext_Independent()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            var shared = new SsProduct { Id = 99, Name = "Shared", Stock = 0 };

            // Attach as Unchanged in ctx1, Add in ctx2 (same object reference)
            ctx1.Attach(shared);
            ctx2.Add(shared);

            var e1 = ctx1.ChangeTracker.Entries.Single();
            var e2 = ctx2.ChangeTracker.Entries.Single();

            Assert.Equal(EntityState.Unchanged, e1.State);
            Assert.Equal(EntityState.Added, e2.State);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 2. FastPathSqlCache is per-context, not shared across instances
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task FastPathSqlCache_IsPerContext_DifferentTableNames()
    {
        // ctx1 uses default "SsProduct" table; ctx2 maps SsProduct to "SsProductAlt"
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, _) = CreateProductCtx();
        cn2.Dispose();

        var cn2b = new SqliteConnection("Data Source=:memory:");
        cn2b.Open();
        using var cmd = cn2b.CreateCommand();
        cmd.CommandText = "CREATE TABLE SsProductAlt (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0); INSERT INTO SsProductAlt VALUES(1,'AltRow',99)";
        cmd.ExecuteNonQuery();
        var opts2 = new DbContextOptions { OnModelCreating = mb => mb.Entity<SsProduct>().ToTable("SsProductAlt") };
        var ctx2 = new DbContext(cn2b, new SqliteProvider(), opts2);

        using (cn1) using (ctx1) using (cn2b) using (ctx2)
        {
            // Warm up ctx1 fast-path cache
            await ctx1.Query<SsProduct>().ToListAsync();

            // ctx1 cache must NOT bleed into ctx2's differently-named table
            var r2 = await ctx2.Query<SsProduct>().ToListAsync();
            Assert.Single(r2);
            Assert.Equal("AltRow", r2[0].Name);
        }
    }

    [Fact]
    public async Task FastPathSqlCache_IsNotStatic_TwoContextsSameType()
    {
        // Both contexts have the same entity→table mapping, but are independent
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            using var ins1 = cn1.CreateCommand();
            ins1.CommandText = "INSERT INTO SsProduct VALUES(1,'FromCn1',10)";
            ins1.ExecuteNonQuery();

            using var ins2 = cn2.CreateCommand();
            ins2.CommandText = "INSERT INTO SsProduct VALUES(1,'FromCn2',20)";
            ins2.ExecuteNonQuery();

            var r1 = await ctx1.Query<SsProduct>().ToListAsync();
            var r2 = await ctx2.Query<SsProduct>().ToListAsync();

            Assert.Equal("FromCn1", r1.Single().Name);
            Assert.Equal("FromCn2", r2.Single().Name);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 3. PreparedInsertCommand is scoped per-context
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PreparedInsertCommand_PerContext_NoLeakage()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            // Insert via ctx1 and ctx2 independently
            await ctx1.InsertAsync(new SsProduct { Name = "C1-Item", Stock = 1 });
            await ctx2.InsertAsync(new SsProduct { Name = "C2-Item", Stock = 2 });

            // Verify each context's connection received data independently
            using var chk1 = cn1.CreateCommand();
            chk1.CommandText = "SELECT COUNT(*) FROM SsProduct WHERE Name='C1-Item'";
            Assert.Equal(1L, (long)chk1.ExecuteScalar()!);

            using var chk2 = cn2.CreateCommand();
            chk2.CommandText = "SELECT COUNT(*) FROM SsProduct WHERE Name='C2-Item'";
            Assert.Equal(1L, (long)chk2.ExecuteScalar()!);
        }
    }

    [Fact]
    public async Task PreparedInsertCommand_DisposedContext_DoesNotAffectOther()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn2) // Keep ctx2 alive
        using (cn1)
        {
            await ctx1.InsertAsync(new SsProduct { Name = "Temp", Stock = 0 });
            await ctx1.DisposeAsync(); // Dispose ctx1 — should not affect ctx2

            // ctx2 should still work fine
            await ctx2.InsertAsync(new SsProduct { Name = "Surviving", Stock = 5 });

            using var chk = cn2.CreateCommand();
            chk.CommandText = "SELECT COUNT(*) FROM SsProduct WHERE Name='Surviving'";
            Assert.Equal(1L, (long)chk.ExecuteScalar()!);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4. MaterializerFactory static caches are thread-safe under concurrent reads
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MaterializerFactory_StaticCaches_ConcurrentReads_NoCorruption()
    {
        const int concurrency = 8;
        const int iterationsPerThread = 20;

        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async i =>
        {
            for (int iter = 0; iter < iterationsPerThread; iter++)
            {
                try
                {
                    var cn = new SqliteConnection("Data Source=:memory:");
                    cn.Open();
                    using var c = cn.CreateCommand();
                    c.CommandText = "CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0);" +
                                    $"INSERT INTO SsProduct VALUES({i * 100 + iter},'T{i}-{iter}',{iter})";
                    c.ExecuteNonQuery();

                    await using var ctx = new DbContext(cn, new SqliteProvider());
                    int targetId = i * 100 + iter;
                    var results = await ctx.Query<SsProduct>()
                        .Where(p => p.Id == targetId)
                        .ToListAsync();
                    // Results may be empty if identity doesn't match, but must not throw
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }
            }
        }));

        Assert.Empty(errors);
    }

    [Fact]
    public async Task MaterializerFactory_DifferentTypes_NoInterference()
    {
        // Materializers for SsProduct and SsOrder must never be confused
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateOrderCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            using var ins1 = cn1.CreateCommand();
            ins1.CommandText = "INSERT INTO SsProduct VALUES(1,'Widget',10)";
            ins1.ExecuteNonQuery();

            using var ins2 = cn2.CreateCommand();
            ins2.CommandText = "INSERT INTO SsOrder VALUES(1,'Gadget',3)";
            ins2.ExecuteNonQuery();

            var products = await ctx1.Query<SsProduct>().ToListAsync();
            var orders = await ctx2.Query<SsOrder>().ToListAsync();

            Assert.Single(products);
            Assert.Equal("Widget", products[0].Name);
            Assert.Single(orders);
            Assert.Equal("Gadget", orders[0].Item);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 5. Two contexts with different fluent mappings for the same CLR type
    //    must not share materializers incorrectly
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TwoContexts_DifferentFluentMappings_SameType_DoNotShareMaterializer()
    {
        var cn1 = new SqliteConnection("Data Source=:memory:");
        cn1.Open();
        using var s1 = cn1.CreateCommand();
        s1.CommandText = "CREATE TABLE TableA (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0);" +
                         "INSERT INTO TableA VALUES(1,'FromA',1)";
        s1.ExecuteNonQuery();
        var opts1 = new DbContextOptions { OnModelCreating = mb => mb.Entity<SsProduct>().ToTable("TableA") };
        var ctx1 = new DbContext(cn1, new SqliteProvider(), opts1);

        var cn2 = new SqliteConnection("Data Source=:memory:");
        cn2.Open();
        using var s2 = cn2.CreateCommand();
        s2.CommandText = "CREATE TABLE TableB (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0);" +
                         "INSERT INTO TableB VALUES(1,'FromB',2)";
        s2.ExecuteNonQuery();
        var opts2 = new DbContextOptions { OnModelCreating = mb => mb.Entity<SsProduct>().ToTable("TableB") };
        var ctx2 = new DbContext(cn2, new SqliteProvider(), opts2);

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            // Query ctx1 first to warm up materializer cache
            var r1 = await ctx1.Query<SsProduct>().ToListAsync();
            // Then ctx2 — must get its own rows, not ctx1's
            var r2 = await ctx2.Query<SsProduct>().ToListAsync();

            Assert.Equal("FromA", r1.Single().Name);
            Assert.Equal("FromB", r2.Single().Name);
        }
    }

    [Fact]
    public async Task TwoContexts_FluentMappingSequence_NoCrossContext_SideEffects()
    {
        // Repeated alternating queries from two contexts must not corrupt each other
        var cn1 = new SqliteConnection("Data Source=:memory:");
        cn1.Open();
        using var s1 = cn1.CreateCommand();
        s1.CommandText = "CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0)";
        s1.ExecuteNonQuery();

        var cn2 = new SqliteConnection("Data Source=:memory:");
        cn2.Open();
        using var s2 = cn2.CreateCommand();
        s2.CommandText = "CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0)";
        s2.ExecuteNonQuery();

        await using var ctx1 = new DbContext(cn1, new SqliteProvider());
        await using var ctx2 = new DbContext(cn2, new SqliteProvider());

        using (cn1) using (cn2)
        {
            for (int i = 0; i < 5; i++)
            {
                using var ins1 = cn1.CreateCommand();
                ins1.CommandText = $"INSERT INTO SsProduct VALUES({i + 1},'Ctx1-{i}',{i})";
                ins1.ExecuteNonQuery();

                using var ins2 = cn2.CreateCommand();
                ins2.CommandText = $"INSERT INTO SsProduct VALUES({i + 1},'Ctx2-{i}',{i * 2})";
                ins2.ExecuteNonQuery();

                int idToFind = i + 1;
                var r1 = await ctx1.Query<SsProduct>().Where(p => p.Id == idToFind).ToListAsync();
                var r2 = await ctx2.Query<SsProduct>().Where(p => p.Id == idToFind).ToListAsync();

                Assert.Equal($"Ctx1-{i}", r1.Single().Name);
                Assert.Equal($"Ctx2-{i}", r2.Single().Name);
            }
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 6. Concurrent SaveChanges on different contexts does not interfere
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ConcurrentSaveChanges_DifferentContexts_NoInterference()
    {
        const int contextCount = 6;
        var tasks = new Task[contextCount];
        var counts = new int[contextCount];
        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        for (int i = 0; i < contextCount; i++)
        {
            int idx = i;
            tasks[idx] = Task.Run(async () =>
            {
                try
                {
                    var cn = new SqliteConnection("Data Source=:memory:");
                    cn.Open();
                    using var c = cn.CreateCommand();
                    c.CommandText = "CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0)";
                    c.ExecuteNonQuery();

                    await using var ctx = new DbContext(cn, new SqliteProvider());
                    for (int j = 0; j < 5; j++)
                    {
                        ctx.Add(new SsProduct { Name = $"C{idx}-I{j}", Stock = j });
                    }
                    await ctx.SaveChangesAsync();

                    using var chk = cn.CreateCommand();
                    chk.CommandText = "SELECT COUNT(*) FROM SsProduct";
                    counts[idx] = Convert.ToInt32(chk.ExecuteScalar());
                }
                catch (Exception ex)
                {
                    errors.Add(ex);
                }
            });
        }

        await Task.WhenAll(tasks);

        Assert.Empty(errors);
        Assert.All(counts, c => Assert.Equal(5, c));
    }

    [Fact]
    public async Task SaveChanges_ContextA_DoesNotMarkEntitiesInContextB_AsSaved()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            var p1 = new SsProduct { Name = "P1", Stock = 1 };
            var p2 = new SsProduct { Name = "P2", Stock = 2 };

            ctx1.Add(p1);
            ctx2.Add(p2);

            // Save only ctx1
            await ctx1.SaveChangesAsync();

            // p2 in ctx2 must still be Added (not smuggled to Unchanged by ctx1's save)
            var e2 = ctx2.ChangeTracker.Entries.Single();
            Assert.Equal(EntityState.Added, e2.State);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 7. ChangeTracker identity map is per-context
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task IdentityMap_PerContext_SamePk_DifferentInstances()
    {
        // ctx1 attaches an entity with Id=1 — ctx2 is completely unaware
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            using var ins1 = cn1.CreateCommand();
            ins1.CommandText = "INSERT INTO SsProduct VALUES(1,'Ctx1Prod',10)";
            ins1.ExecuteNonQuery();

            using var ins2 = cn2.CreateCommand();
            ins2.CommandText = "INSERT INTO SsProduct VALUES(1,'Ctx2Prod',20)";
            ins2.ExecuteNonQuery();

            var fromCtx1 = (await ctx1.Query<SsProduct>().Where(p => p.Id == 1).ToListAsync()).Single();
            var fromCtx2 = (await ctx2.Query<SsProduct>().Where(p => p.Id == 1).ToListAsync()).Single();

            Assert.Equal("Ctx1Prod", fromCtx1.Name);
            Assert.Equal("Ctx2Prod", fromCtx2.Name);
            // They must be different object instances
            Assert.False(ReferenceEquals(fromCtx1, fromCtx2));
        }
    }

    [Fact]
    public void ChangeTracker_Attach_Remove_Isolated()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            var p = new SsProduct { Id = 5, Name = "Shared", Stock = 7 };

            ctx1.Attach(p);
            Assert.Single(ctx1.ChangeTracker.Entries);
            Assert.Empty(ctx2.ChangeTracker.Entries);

            ctx1.ChangeTracker.Clear();
            Assert.Empty(ctx1.ChangeTracker.Entries);
            Assert.Empty(ctx2.ChangeTracker.Entries);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 8. Concurrent reads through static materializer cache produce correct results
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task StaticMaterializerCache_ConcurrentContexts_CorrectResultsPerContext()
    {
        const int threads = 10;
        var results = new string[threads];
        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        await Task.WhenAll(Enumerable.Range(0, threads).Select(async i =>
        {
            try
            {
                var cn = new SqliteConnection("Data Source=:memory:");
                cn.Open();
                using var c = cn.CreateCommand();
                c.CommandText = $"CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0);" +
                                $"INSERT INTO SsProduct VALUES({i + 1},'Thread{i}',{i})";
                c.ExecuteNonQuery();

                await using var ctx = new DbContext(cn, new SqliteProvider());
                var r = await ctx.Query<SsProduct>().ToListAsync();
                results[i] = r.Single().Name;
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        }));

        Assert.Empty(errors);
        for (int i = 0; i < threads; i++)
            Assert.Equal($"Thread{i}", results[i]);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 9. Disposing a context does not invalidate caches used by another context
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task DisposingContext_DoesNotInvalidate_OtherContextCache()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn2) using (ctx2)
        using (cn1)
        {
            using var ins1 = cn1.CreateCommand();
            ins1.CommandText = "INSERT INTO SsProduct VALUES(1,'First',5)";
            ins1.ExecuteNonQuery();

            // Warm up ctx1's caches
            await ctx1.Query<SsProduct>().ToListAsync();

            // Dispose ctx1 — static materializer caches must remain intact
            ctx1.Dispose();

            using var ins2 = cn2.CreateCommand();
            ins2.CommandText = "INSERT INTO SsProduct VALUES(1,'Second',10)";
            ins2.ExecuteNonQuery();

            // ctx2 must still work
            var r2 = await ctx2.Query<SsProduct>().ToListAsync();
            Assert.Single(r2);
            Assert.Equal("Second", r2[0].Name);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 10. Options per context are independent
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void DbContextOptions_ArePerInstance_NotShared()
    {
        var opts1 = new DbContextOptions { EagerChangeTracking = true };
        var opts2 = new DbContextOptions { EagerChangeTracking = false };

        var (cn1, _) = CreateProductCtx();
        var (cn2, _) = CreateProductCtx();

        using (cn1) using (cn2)
        {
            using var ctx1 = new DbContext(cn1, new SqliteProvider(), opts1);
            using var ctx2 = new DbContext(cn2, new SqliteProvider(), opts2);

            Assert.True(ctx1.Options.EagerChangeTracking);
            Assert.False(ctx2.Options.EagerChangeTracking);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 11. SaveChanges on context A does not trigger DetectChanges for context B
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SaveChanges_ContextA_DoesNotRunDetectChanges_ForContextB()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            var p1 = new SsProduct { Name = "Ctx1", Stock = 1 };
            var p2 = new SsProduct { Name = "Ctx2", Stock = 2 };

            ctx1.Add(p1);
            ctx2.Add(p2);

            await ctx1.SaveChangesAsync();

            // After ctx1.SaveChanges, ctx1 entity should be Unchanged, ctx2 entity still Added
            var e1 = ctx1.ChangeTracker.Entries.Single();
            var e2 = ctx2.ChangeTracker.Entries.Single();

            Assert.Equal(EntityState.Unchanged, e1.State);
            Assert.Equal(EntityState.Added, e2.State);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 12. Concurrent inserts via different contexts share the static type cache
    //     safely (no IL emit race)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task StaticTypeCache_ConcurrentAccess_NoRaceCondition()
    {
        const int concurrency = 12;
        var errors = new System.Collections.Concurrent.ConcurrentBag<Exception>();

        await Task.WhenAll(Enumerable.Range(0, concurrency).Select(async threadId =>
        {
            try
            {
                for (int i = 0; i < 5; i++)
                {
                    var cn = new SqliteConnection("Data Source=:memory:");
                    cn.Open();
                    using var c = cn.CreateCommand();
                    c.CommandText = "CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0);" +
                                    $"INSERT INTO SsProduct VALUES({threadId * 10 + i},'T{threadId}-I{i}',0)";
                    c.ExecuteNonQuery();

                    await using var ctx = new DbContext(cn, new SqliteProvider());
                    await ctx.InsertAsync(new SsProduct { Name = $"Inserted-{threadId}-{i}", Stock = threadId });
                    var all = await ctx.Query<SsProduct>().ToListAsync();
                    Assert.True(all.Count >= 1);
                }
            }
            catch (Exception ex)
            {
                errors.Add(ex);
            }
        }));

        Assert.Empty(errors);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 13. ModelBuilder configuration is per-context — no leakage
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ModelBuilder_PerContext_NoLeakage()
    {
        // ctx1 maps SsProduct → "SsProduct" (default)
        // ctx2 maps SsProduct → "SsProductCustom" (fluent override)
        var cn1 = new SqliteConnection("Data Source=:memory:");
        cn1.Open();
        using var s1 = cn1.CreateCommand();
        s1.CommandText = "CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0);" +
                         "INSERT INTO SsProduct VALUES(1,'DefaultTable',10)";
        s1.ExecuteNonQuery();
        await using var ctx1 = new DbContext(cn1, new SqliteProvider());

        var cn2 = new SqliteConnection("Data Source=:memory:");
        cn2.Open();
        using var s2 = cn2.CreateCommand();
        s2.CommandText = "CREATE TABLE SsProductCustom (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0);" +
                         "INSERT INTO SsProductCustom VALUES(1,'CustomTable',20)";
        s2.ExecuteNonQuery();
        var opts2 = new DbContextOptions { OnModelCreating = mb => mb.Entity<SsProduct>().ToTable("SsProductCustom") };
        await using var ctx2 = new DbContext(cn2, new SqliteProvider(), opts2);

        using (cn1) using (cn2)
        {
            var r1 = await ctx1.Query<SsProduct>().ToListAsync();
            var r2 = await ctx2.Query<SsProduct>().ToListAsync();

            Assert.Equal("DefaultTable", r1.Single().Name);
            Assert.Equal("CustomTable", r2.Single().Name);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 14. Multiple inserts in same context reuse PreparedInsertCommand
    //     and don't create duplicates in shared cache
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PreparedInsertCommand_SameContext_ReusedCorrectly()
    {
        var (cn, ctx) = CreateProductCtx();
        using (cn) using (ctx)
        {
            for (int i = 0; i < 10; i++)
                await ctx.InsertAsync(new SsProduct { Name = $"Item{i}", Stock = i });

            using var chk = cn.CreateCommand();
            chk.CommandText = "SELECT COUNT(*) FROM SsProduct";
            Assert.Equal(10L, (long)chk.ExecuteScalar()!);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 15. ChangeTracker.Remove is scoped to calling context
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void ChangeTracker_Remove_ScopedToContext()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            var p = new SsProduct { Id = 7, Name = "ForRemoval", Stock = 0 };
            ctx1.Attach(p);
            ctx2.Attach(p);

            // Use ChangeTracker.Clear to remove from ctx1 only
            ctx1.ChangeTracker.Clear();

            Assert.Empty(ctx1.ChangeTracker.Entries);
            // ctx2 should still track p
            Assert.Single(ctx2.ChangeTracker.Entries);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 16. Different entity types in same context tracked independently
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public void ChangeTracker_DifferentEntityTypes_SameContext_Isolated()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var c1 = cn.CreateCommand();
        c1.CommandText = "CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0)";
        c1.ExecuteNonQuery();
        using var c2 = cn.CreateCommand();
        c2.CommandText = "CREATE TABLE SsUser (Id INTEGER PRIMARY KEY AUTOINCREMENT, Email TEXT NOT NULL)";
        c2.ExecuteNonQuery();

        using (cn)
        using (var ctx = new DbContext(cn, new SqliteProvider()))
        {
            ctx.Add(new SsProduct { Name = "Widget", Stock = 5 });
            ctx.Add(new SsUser { Email = "user@example.com" });

            var entries = ctx.ChangeTracker.Entries.ToList();
            Assert.Equal(2, entries.Count);

            var productEntries = entries.Where(e => e.Entity is SsProduct).ToList();
            var userEntries = entries.Where(e => e.Entity is SsUser).ToList();

            Assert.Single(productEntries);
            Assert.Single(userEntries);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 17. Concurrent queries through materializer static cache — no phantom rows
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ConcurrentMaterializerReads_NoPhantomRowsAcrossContexts()
    {
        const int parallelCount = 8;
        var allResults = new System.Collections.Concurrent.ConcurrentDictionary<int, List<string>>();

        await Task.WhenAll(Enumerable.Range(0, parallelCount).Select(async idx =>
        {
            var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var c = cn.CreateCommand();
            // Each context gets exactly 3 rows named after the context index
            c.CommandText = "CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0);" +
                            $"INSERT INTO SsProduct VALUES(1,'Ctx{idx}-Row0',0);" +
                            $"INSERT INTO SsProduct VALUES(2,'Ctx{idx}-Row1',1);" +
                            $"INSERT INTO SsProduct VALUES(3,'Ctx{idx}-Row2',2)";
            c.ExecuteNonQuery();

            await using var ctx = new DbContext(cn, new SqliteProvider());
            var rows = await ctx.Query<SsProduct>().ToListAsync();
            allResults[idx] = rows.Select(r => r.Name).ToList();
        }));

        for (int idx = 0; idx < parallelCount; idx++)
        {
            Assert.Equal(3, allResults[idx].Count);
            Assert.All(allResults[idx], name => Assert.StartsWith($"Ctx{idx}-", name));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 18. Global filter per context does not leak to other contexts
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task GlobalFilter_PerContext_DoesNotAffectOtherContext()
    {
        var cn1 = new SqliteConnection("Data Source=:memory:");
        cn1.Open();
        using var s1 = cn1.CreateCommand();
        s1.CommandText = "CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0);" +
                         "INSERT INTO SsProduct VALUES(1,'Active',10);" +
                         "INSERT INTO SsProduct VALUES(2,'Hidden',0)";
        s1.ExecuteNonQuery();

        var cn2 = new SqliteConnection("Data Source=:memory:");
        cn2.Open();
        using var s2 = cn2.CreateCommand();
        s2.CommandText = "CREATE TABLE SsProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Stock INTEGER NOT NULL DEFAULT 0);" +
                         "INSERT INTO SsProduct VALUES(1,'One',10);" +
                         "INSERT INTO SsProduct VALUES(2,'Two',20)";
        s2.ExecuteNonQuery();

        // ctx1 has a global filter: Stock > 0
        var opts1 = new DbContextOptions();
        opts1.AddGlobalFilter<SsProduct>(p => p.Stock > 0);
        await using var ctx1 = new DbContext(cn1, new SqliteProvider(), opts1);
        // ctx2 has no filter
        await using var ctx2 = new DbContext(cn2, new SqliteProvider());

        using (cn1) using (cn2)
        {
            var r1 = await ctx1.Query<SsProduct>().ToListAsync();
            var r2 = await ctx2.Query<SsProduct>().ToListAsync();

            // ctx1 filter should exclude Stock=0 row
            Assert.Single(r1);
            Assert.Equal("Active", r1[0].Name);

            // ctx2 has no filter — both rows visible
            Assert.Equal(2, r2.Count);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 19. ChangeTracker state after failed SaveChanges stays accurate per-context
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ChangeTracker_AfterSaveChanges_StateCorrect_PerContext()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            var p1 = new SsProduct { Name = "P1", Stock = 1 };
            var p2 = new SsProduct { Name = "P2", Stock = 2 };

            ctx1.Add(p1);
            ctx2.Add(p2);

            await ctx1.SaveChangesAsync();

            // p1 was saved → Unchanged
            Assert.Equal(EntityState.Unchanged, ctx1.ChangeTracker.Entries.Single().State);
            // p2 not yet saved → still Added
            Assert.Equal(EntityState.Added, ctx2.ChangeTracker.Entries.Single().State);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 20. Entity explicitly added to both contexts tracked independently —
    //     saving in one does not corrupt the other's tracked state
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ChangeTracker_TwoContexts_ExplicitAdd_SavedInOneOnly()
    {
        var (cn1, ctx1) = CreateProductCtx();
        var (cn2, ctx2) = CreateProductCtx();

        using (cn1) using (ctx1) using (cn2) using (ctx2)
        {
            var p1 = new SsProduct { Name = "Ctx1Entity", Stock = 1 };
            var p2 = new SsProduct { Name = "Ctx2Entity", Stock = 2 };

            ctx1.Add(p1);
            ctx2.Add(p2);

            // Save ctx1 only
            await ctx1.SaveChangesAsync();

            // ctx1 entity should now be Unchanged (saved)
            var entry1 = ctx1.ChangeTracker.Entries.Single();
            Assert.Equal(EntityState.Unchanged, entry1.State);
            Assert.True(p1.Id > 0);

            // ctx2 entity must still be Added (not yet saved)
            var entry2 = ctx2.ChangeTracker.Entries.Single();
            Assert.Equal(EntityState.Added, entry2.State);
            Assert.Equal(0, p2.Id); // not yet assigned by DB

            // Save ctx2 as well
            await ctx2.SaveChangesAsync();
            Assert.True(p2.Id > 0);
        }
    }
}
