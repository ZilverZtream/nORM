using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial ChangeTracker stress tests. Exercises the identity map, state machine,
/// and FK-ordering logic under heavy realistic workloads. Tests assert data OUTCOMES,
/// not internal implementation details.
/// </summary>
public class ChangeTrackerStressTests
{
    // ── Domain models ──────────────────────────────────────────────────────

    [Table("CtWidget")]
    private class CtWidget
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Score { get; set; }
        public bool Active { get; set; } = true;
    }

    // FK chain: Principal → CtNodeA → CtNodeB → CtNodeC (deepest)
    [Table("CtPrincipal")]
    private class CtPrincipal
    {
        [Key]
        public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    [Table("CtNodeA")]
    private class CtNodeA
    {
        [Key]
        public int Id { get; set; }
        public int CtPrincipalId { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    [Table("CtNodeB")]
    private class CtNodeB
    {
        [Key]
        public int Id { get; set; }
        public int CtNodeAId { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    [Table("CtNodeC")]
    private class CtNodeC
    {
        [Key]
        public int Id { get; set; }
        public int CtNodeBId { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    // ── Helpers ─────────────────────────────────────────────────────────

    private static (SqliteConnection Cn, DbContext Ctx) CreateWidgetContext(bool eager = true)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CtWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Score INTEGER NOT NULL DEFAULT 0, Active INTEGER NOT NULL DEFAULT 1);";
        cmd.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = eager });
        return (cn, ctx);
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateFkChainContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "PRAGMA foreign_keys = ON;" +
            "CREATE TABLE CtPrincipal (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);" +
            "CREATE TABLE CtNodeA (Id INTEGER PRIMARY KEY, CtPrincipalId INTEGER NOT NULL REFERENCES CtPrincipal(Id), Label TEXT NOT NULL);" +
            "CREATE TABLE CtNodeB (Id INTEGER PRIMARY KEY, CtNodeAId INTEGER NOT NULL REFERENCES CtNodeA(Id), Label TEXT NOT NULL);" +
            "CREATE TABLE CtNodeC (Id INTEGER PRIMARY KEY, CtNodeBId INTEGER NOT NULL REFERENCES CtNodeB(Id), Label TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    private static void MarkDirty(DbContext ctx, object entity)
    {
        var entry = ctx.ChangeTracker.Entries.FirstOrDefault(e => ReferenceEquals(e.Entity, entity));
        if (entry == null) return;
        var md = typeof(ChangeTracker).GetMethod("MarkDirty", BindingFlags.Instance | BindingFlags.NonPublic);
        md?.Invoke(ctx.ChangeTracker, new object[] { entry });
    }

    private static void InsertWidgets(SqliteConnection cn, int start, int count)
    {
        using var cmd = cn.CreateCommand();
        for (int i = start; i < start + count; i++)
        {
            cmd.CommandText = $"INSERT INTO CtWidget VALUES ({i}, 'Widget{i}', {i * 10}, 1)";
            cmd.ExecuteNonQuery();
        }
    }

    // ── Test 1: 500 track + 200 modify + 100 delete + 100 insert ──────────

    /// <summary>
    /// Track 500 entities. Modify 200, delete 100, insert 100. Single SaveChanges.
    /// Asserts correct final row count = 500 and modified rows have correct values.
    /// </summary>
    [Fact]
    public async Task MassOperations_500Track200Modify100Delete100Insert_CorrectOutcome()
    {
        var (cn, ctx) = CreateWidgetContext(eager: true);
        await using var _ = ctx;

        // Insert 500 rows directly (bypassing ORM for setup speed)
        InsertWidgets(cn, 1, 500);

        // Load and attach all 500
        var all = ctx.Query<CtWidget>().OrderBy(w => w.Id).ToList();
        Assert.Equal(500, all.Count);

        // Modify IDs 1..200
        for (int i = 0; i < 200; i++)
        {
            all[i].Score = 9999;
            MarkDirty(ctx, all[i]);
        }

        // Delete IDs 201..300
        for (int i = 200; i < 300; i++)
            ctx.Remove(all[i]);

        // Insert 100 new widgets (IDs 501..600)
        for (int i = 501; i <= 600; i++)
            ctx.Add(new CtWidget { Id = i, Name = $"NewWidget{i}", Score = i });

        // Use detectChanges:true so MarkDirty changes are picked up by the DetectChanges pass
        var saved = await ctx.SaveChangesAsync(detectChanges: true);

        // 200 updates + 100 deletes + 100 inserts = 400
        Assert.Equal(400, saved);

        // Final row count: 500 - 100 + 100 = 500
        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM CtWidget";
        Assert.Equal(500L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // Modified rows have Score=9999
        vcmd.CommandText = "SELECT COUNT(*) FROM CtWidget WHERE Score = 9999";
        Assert.Equal(200L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // Deleted rows gone
        vcmd.CommandText = "SELECT COUNT(*) FROM CtWidget WHERE Id BETWEEN 201 AND 300";
        Assert.Equal(0L, Convert.ToInt64(vcmd.ExecuteScalar()));

        // Inserted rows present
        vcmd.CommandText = "SELECT COUNT(*) FROM CtWidget WHERE Id BETWEEN 501 AND 600";
        Assert.Equal(100L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }

    // ── Test 2: DetectChanges across 100 entities ─────────────────────────

    /// <summary>
    /// Track 100 entities. Modify 50 (different fields on each). DetectChanges.
    /// Exactly 50 Modified, 50 Unchanged.
    /// </summary>
    [Fact]
    public void DetectChanges_100Entities50Modified_ExactlyCorrectStates()
    {
        var (cn, ctx) = CreateWidgetContext(eager: true);
        using var _ = ctx;

        InsertWidgets(cn, 1, 100);
        var all = ctx.Query<CtWidget>().OrderBy(w => w.Id).ToList();
        Assert.Equal(100, all.Count);

        // Modify exactly the first 50
        for (int i = 0; i < 50; i++)
        {
            all[i].Score = -1;
            MarkDirty(ctx, all[i]);
        }

        // Force DetectChanges via reflection
        var detectMethod = typeof(ChangeTracker).GetMethod("DetectChanges", BindingFlags.Instance | BindingFlags.NonPublic);
        detectMethod?.Invoke(ctx.ChangeTracker, null);

        var entries = ctx.ChangeTracker.Entries.ToList();
        int modified = entries.Count(e => e.State == EntityState.Modified);
        int unchanged = entries.Count(e => e.State == EntityState.Unchanged);

        Assert.Equal(50, modified);
        Assert.Equal(50, unchanged);
    }

    // ── Test 3: FK-ordered insert — 4-level chain added in reverse ─────────

    /// <summary>
    /// Insert Principal → NodeA → NodeB → NodeC in WRONG order (C, B, A, Principal).
    /// With FK constraints ON, nORM's topological sort must fix the order automatically.
    /// </summary>
    [Fact]
    public async Task FkOrdering_4LevelChainInsertedBackwards_SaveSucceeds()
    {
        var (cn, ctx) = CreateFkChainContext();
        await using var _ = ctx;

        // Add in dependency-violating order
        ctx.Add(new CtNodeC { Id = 4, CtNodeBId = 3, Label = "C" });
        ctx.Add(new CtNodeB { Id = 3, CtNodeAId = 2, Label = "B" });
        ctx.Add(new CtNodeA { Id = 2, CtPrincipalId = 1, Label = "A" });
        ctx.Add(new CtPrincipal { Id = 1, Label = "Root" });

        // Topological sort must reorder to: Principal, NodeA, NodeB, NodeC
        var saved = await ctx.SaveChangesAsync();
        Assert.Equal(4, saved);

        // Verify all rows committed
        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM CtPrincipal";
        Assert.Equal(1L, Convert.ToInt64(vcmd.ExecuteScalar()));
        vcmd.CommandText = "SELECT COUNT(*) FROM CtNodeA";
        Assert.Equal(1L, Convert.ToInt64(vcmd.ExecuteScalar()));
        vcmd.CommandText = "SELECT COUNT(*) FROM CtNodeB";
        Assert.Equal(1L, Convert.ToInt64(vcmd.ExecuteScalar()));
        vcmd.CommandText = "SELECT COUNT(*) FROM CtNodeC";
        Assert.Equal(1L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }

    // ── Test 4: FK-ordered delete — must delete dependents first ──────────

    /// <summary>
    /// Delete Principal (remove in wrong order: Principal first). FK constraints
    /// require deleting NodeC→NodeB→NodeA→Principal. nORM must reverse-sort.
    /// </summary>
    [Fact]
    public async Task FkOrdering_4LevelChainDeletedBackwards_SaveSucceeds()
    {
        var (cn, ctx) = CreateFkChainContext();
        await using var _ = ctx;

        // Insert seed data via raw SQL (same connection, avoids PRAGMA re-init)
        using var seedCmd = cn.CreateCommand();
        seedCmd.CommandText =
            "INSERT INTO CtPrincipal VALUES (1, 'Root');" +
            "INSERT INTO CtNodeA VALUES (2, 1, 'A');" +
            "INSERT INTO CtNodeB VALUES (3, 2, 'B');" +
            "INSERT INTO CtNodeC VALUES (4, 3, 'C');";
        seedCmd.ExecuteNonQuery();

        // Remove in Principal-first order (which violates FK constraints)
        ctx.Remove(new CtPrincipal { Id = 1, Label = "Root" });
        ctx.Remove(new CtNodeA { Id = 2, CtPrincipalId = 1, Label = "A" });
        ctx.Remove(new CtNodeB { Id = 3, CtNodeAId = 2, Label = "B" });
        ctx.Remove(new CtNodeC { Id = 4, CtNodeBId = 3, Label = "C" });

        // Reverse topological sort must reorder to: NodeC, NodeB, NodeA, Principal
        var saved = await ctx.SaveChangesAsync(detectChanges: false);
        Assert.Equal(4, saved);

        // All gone
        using var vcmd = cn.CreateCommand();
        foreach (var table in new[] { "CtPrincipal", "CtNodeA", "CtNodeB", "CtNodeC" })
        {
            vcmd.CommandText = $"SELECT COUNT(*) FROM {table}";
            Assert.Equal(0L, Convert.ToInt64(vcmd.ExecuteScalar()));
        }
    }

    // ── Test 5: Tracker state Unchanged after successful save ──────────────

    /// <summary>
    /// After a successful SaveChanges, all previously Modified/Added entries must
    /// become Unchanged (accepted changes). No lingering dirty state.
    /// </summary>
    [Fact]
    public async Task AfterSave_AllEntriesAreUnchanged()
    {
        var (cn, ctx) = CreateWidgetContext(eager: true);
        await using var _ = ctx;

        InsertWidgets(cn, 1, 10);
        var widgets = ctx.Query<CtWidget>().OrderBy(w => w.Id).ToList();

        // Modify 5 widgets
        for (int i = 0; i < 5; i++)
        {
            widgets[i].Score = 777;
            MarkDirty(ctx, widgets[i]);
        }

        await ctx.SaveChangesAsync(detectChanges: false);

        // After save: all entries should be Unchanged or removed from tracker
        var entries = ctx.ChangeTracker.Entries.ToList();
        int dirtyAfterSave = entries.Count(e => e.State == EntityState.Modified || e.State == EntityState.Added);
        Assert.Equal(0, dirtyAfterSave);
    }

    // ── Test 6: Rapid alternating save/query cycle ─────────────────────────

    /// <summary>
    /// 20 alternating write-then-read cycles. Each cycle increments a score.
    /// Final score must equal 20 (all increments committed and read back correctly).
    /// Verifies no stale context state leaks between saves.
    /// </summary>
    [Fact]
    public async Task RapidSaveQueryCycle_20Iterations_FinalValueCorrect()
    {
        var (cn, ctx) = CreateWidgetContext(eager: true);
        await using var _ = ctx;

        using var insertCmd = cn.CreateCommand();
        insertCmd.CommandText = "INSERT INTO CtWidget VALUES (1, 'Counter', 0, 1)";
        insertCmd.ExecuteNonQuery();

        const int iterations = 20;

        // Use a single context (avoids PRAGMA re-initialization on same in-memory connection).
        // Use ctx.Update() to explicitly mark the entity for update, then save.
        for (int i = 0; i < iterations; i++)
        {
            var widget = ctx.Query<CtWidget>().First(w => w.Id == 1);
            widget.Score += 1;
            ctx.Update(widget);
            await ctx.SaveChangesAsync(detectChanges: false);
        }

        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT Score FROM CtWidget WHERE Id = 1";
        var finalScore = Convert.ToInt32(vcmd.ExecuteScalar());
        Assert.Equal(iterations, finalScore);
    }

    // ── Test 7: Remove never-attached entity is safe ───────────────────────

    /// <summary>
    /// Calling ctx.Remove on an entity that was loaded via a different context instance
    /// (manual attach scenario) must correctly mark it Deleted and execute the DELETE.
    /// </summary>
    [Fact]
    public async Task Remove_ManuallyAttached_DeletesFromDb()
    {
        var (cn, ctx) = CreateWidgetContext(eager: true);
        await using var _ = ctx;

        using var insertCmd = cn.CreateCommand();
        insertCmd.CommandText = "INSERT INTO CtWidget VALUES (99, 'ToDelete', 0, 1)";
        insertCmd.ExecuteNonQuery();

        // Create entity manually (not from Query<>)
        var widget = new CtWidget { Id = 99, Name = "ToDelete", Score = 0, Active = true };
        ctx.Attach(widget);
        ctx.Remove(widget);

        await ctx.SaveChangesAsync(detectChanges: false);

        using var vcmd = cn.CreateCommand();
        vcmd.CommandText = "SELECT COUNT(*) FROM CtWidget WHERE Id = 99";
        Assert.Equal(0L, Convert.ToInt64(vcmd.ExecuteScalar()));
    }
}
