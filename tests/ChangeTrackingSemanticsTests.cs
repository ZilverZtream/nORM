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

namespace nORM.Tests;

/// <summary>
/// Verifies change tracking contract semantics:
/// attaching detached entities without PK collision,
/// and repeated load/clear cycles that must not leak tracker entries.
///
/// PK mutation detection: <see cref="PKMutationTests"/>.
/// Composite key tracking: <see cref="CompositeKeyTests"/>.
/// Constructor-bound entities: <see cref="ConstructorBoundEntityTrackingTests"/>.
/// </summary>
public class ChangeTrackingSemanticsTests
{
    [Table("TrackedNode")]
    private class TrackedNode
    {
        [Key]
        public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) MakeContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TrackedNode (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });
        return (cn, ctx);
    }

    private static void SeedRow(SqliteConnection cn, int id, string label)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO TrackedNode (Id, Label) VALUES (@id, @label)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@label", label);
        cmd.ExecuteNonQuery();
    }

    // ── Scenario 1: Attach a detached entity without PK collision ─────────────

    [Fact]
    public void Attach_DetachedEntity_NoPkCollision_IsTrackedAsUnchanged()
    {
        // An entity that has never been seen by the context before has no PK collision risk.
        // Attach must add it to the tracker as Unchanged.
        var (cn, ctx) = MakeContext();
        using var _cn = cn;
        using var _ctx = ctx;

        SeedRow(cn, 42, "existing");

        var node = new TrackedNode { Id = 42, Label = "existing" };
        ctx.Attach(node);

        var entry = ctx.ChangeTracker.Entries.Single(e => ReferenceEquals(e.Entity, node));
        Assert.Equal(EntityState.Unchanged, entry.State);
    }

    [Fact]
    public void Attach_TwoDistinctEntitiesWithDifferentPks_BothTracked()
    {
        var (cn, ctx) = MakeContext();
        using var _cn = cn;
        using var _ctx = ctx;

        SeedRow(cn, 1, "alpha");
        SeedRow(cn, 2, "beta");

        var a = new TrackedNode { Id = 1, Label = "alpha" };
        var b = new TrackedNode { Id = 2, Label = "beta" };

        ctx.Attach(a);
        ctx.Attach(b);

        Assert.Equal(2, ctx.ChangeTracker.Entries.Count());
    }

    [Fact]
    public void Attach_SamePk_SecondAttachReturnsFirstInstance()
    {
        // If two CLR instances share a PK, the tracker should de-duplicate by PK identity.
        var (cn, ctx) = MakeContext();
        using var _cn = cn;
        using var _ctx = ctx;

        SeedRow(cn, 7, "seven");

        var first = new TrackedNode { Id = 7, Label = "seven" };
        var second = new TrackedNode { Id = 7, Label = "seven" };

        var entryA = ctx.Attach(first);
        var entryB = ctx.Attach(second);

        // The tracker must return the same EntityEntry for both (first wins by PK).
        Assert.Same(entryA, entryB);
        Assert.Single(ctx.ChangeTracker.Entries);
    }

    [Fact]
    public async Task Attach_DetachedEntity_CanBeUpdated()
    {
        var (cn, ctx) = MakeContext();
        using var _cn = cn;
        using var _ctx = ctx;

        SeedRow(cn, 10, "original");

        var node = new TrackedNode { Id = 10, Label = "updated" };
        ctx.Update(node); // marks as Modified, adds to tracker

        var affected = await ctx.SaveChangesAsync(detectChanges: false);
        Assert.Equal(1, affected);

        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT Label FROM TrackedNode WHERE Id = 10";
        Assert.Equal("updated", (string)verify.ExecuteScalar()!);
    }

    // ── Scenario 5: Repeated load/clear cycles do not leak entries ────────────

    [Fact]
    public void RepeatedClear_AfterAttach_LeavesEmptyTracker()
    {
        var (cn, ctx) = MakeContext();
        using var _cn = cn;
        using var _ctx = ctx;

        SeedRow(cn, 1, "a");
        SeedRow(cn, 2, "b");

        for (var cycle = 0; cycle < 20; cycle++)
        {
            ctx.Attach(new TrackedNode { Id = 1, Label = "a" });
            ctx.Attach(new TrackedNode { Id = 2, Label = "b" });

            ctx.ChangeTracker.Clear();

            Assert.Empty(ctx.ChangeTracker.Entries);
        }
    }

    [Fact]
    public async Task RepeatedLoadClear_EntryCountDoesNotGrow()
    {
        var (cn, ctx) = MakeContext();
        using var _cn = cn;
        using var _ctx = ctx;

        for (var i = 1; i <= 5; i++)
            SeedRow(cn, i, $"node_{i}");

        for (var cycle = 0; cycle < 10; cycle++)
        {
            // Load all rows — tracker accumulates entries
            var rows = await ctx.Query<TrackedNode>().ToListAsync();
            Assert.Equal(5, rows.Count);

            var countAfterLoad = ctx.ChangeTracker.Entries.Count();
            Assert.True(countAfterLoad <= 5,
                $"Cycle {cycle}: expected at most 5 tracker entries after load, got {countAfterLoad}.");

            // Clear — all entries must go
            ctx.ChangeTracker.Clear();
            Assert.Empty(ctx.ChangeTracker.Entries);
        }
    }

    [Fact]
    public void Clear_ThenAttach_DoesNotSeeStaleEntries()
    {
        var (cn, ctx) = MakeContext();
        using var _cn = cn;
        using var _ctx = ctx;

        SeedRow(cn, 100, "stale");

        // Attach, then clear
        var stale = new TrackedNode { Id = 100, Label = "stale" };
        ctx.Attach(stale);
        ctx.ChangeTracker.Clear();

        // After clear, re-attaching a different CLR instance with the same PK
        // must be accepted as a fresh entry (no ghost state from the cleared entry).
        var fresh = new TrackedNode { Id = 100, Label = "stale" };
        var entry = ctx.Attach(fresh);

        Assert.Equal(EntityState.Unchanged, entry.State);
        Assert.Single(ctx.ChangeTracker.Entries);
        Assert.Same(fresh, entry.Entity);
    }

    [Fact]
    public void RepeatedAddAndRemove_EntryCountReturnsToBelowInitial()
    {
        var (cn, ctx) = MakeContext();
        using var _cn = cn;
        using var _ctx = ctx;

        for (var round = 0; round < 10; round++)
        {
            var entities = Enumerable.Range(round * 10 + 1, 5)
                .Select(id => new TrackedNode { Id = id, Label = $"node_{id}" })
                .ToList();

            foreach (var e in entities)
                ctx.Attach(e);

            foreach (var e in entities)
                ctx.Remove(e);
        }

        // After removing everything added, tracker must not have leaked entries.
        Assert.Empty(ctx.ChangeTracker.Entries);
    }
}
