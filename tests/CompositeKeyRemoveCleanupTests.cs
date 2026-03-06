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
/// CT-1: Verifies that ChangeTracker.Remove() correctly cleans up composite-key entries
/// from the identity map (_entriesByKey).
///
/// Root bug: EntityEntry.CaptureKey() stored composite keys as object?[] while
/// _entriesByKey was keyed by ValueTuples (2/3 keys) or CompositeKey (&gt;3 keys) via
/// GetPrimaryKeyValue(). TryRemove(object?[]) on a dictionary keyed by (v0,v1) always
/// failed silently, leaving a zombie entry. Subsequent Attach calls for the same
/// composite PK would find the stale null-entity zombie and return a corrupt entry.
///
/// Fix: ChangeTracker.Remove() now converts OriginalKey through ToLookupKey() to the
/// dictionary-compatible shape before TryRemove.
/// </summary>
public class CompositeKeyRemoveCleanupTests
{
    [Table("CompoundItem")]
    private class CompoundItem
    {
        public int Part1 { get; set; }
        public int Part2 { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE CompoundItem " +
            "(Part1 INTEGER, Part2 INTEGER, Value TEXT NOT NULL, PRIMARY KEY (Part1, Part2));";
        cmd.ExecuteNonQuery();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<CompoundItem>()
                .HasKey(e => new { e.Part1, e.Part2 })
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    /// <summary>
    /// CT-1 primary regression: After deleting a composite-key entity via SaveChanges,
    /// the identity map entry must be fully removed. Re-attaching the same composite key
    /// must produce a fresh tracked entry (non-null Entity, correct state).
    ///
    /// Before the fix: TryRemove(object?[]) against a (v0,v1) ValueTuple key always
    /// failed → zombie entry remained → subsequent Attach found the stale null-entity
    /// zombie → returned entry had Entity == null instead of the live re-attached instance.
    /// </summary>
    [Fact]
    public async Task AfterDelete_CompositeKeyEntry_FullyRemovedFromIdentityMap()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var item = new CompoundItem { Part1 = 1, Part2 = 2, Value = "original" };

            ctx.Add(item);
            await ctx.SaveChangesAsync();
            Assert.Single(ctx.ChangeTracker.Entries);

            // Mark Deleted → SaveChanges issues DELETE and then calls ChangeTracker.Remove
            ctx.Remove(item);
            await ctx.SaveChangesAsync();

            // Tracker must be empty after successful delete
            Assert.Empty(ctx.ChangeTracker.Entries);

            // Re-attach the same entity — identity map must see it as fresh, not zombie
            var reattached = ctx.Attach(item);

            // CT-1 bug: zombie in _entriesByKey caused Track() to return stale null-entity entry.
            Assert.NotNull(reattached.Entity);
            Assert.Same(item, reattached.Entity);
            Assert.Single(ctx.ChangeTracker.Entries);
        }
    }

    /// <summary>
    /// CT-1: After removing ONE of several composite-key entities, only the targeted
    /// entity leaves the tracker; the others remain intact with correct identity.
    /// Before the fix, the failed cleanup could corrupt _entriesByKey in unexpected ways.
    /// </summary>
    [Fact]
    public async Task Remove_OneOfSeveral_CompositeKeyEntities_OthersRemainIntact()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var e1 = new CompoundItem { Part1 = 1, Part2 = 1, Value = "A" };
            var e2 = new CompoundItem { Part1 = 1, Part2 = 2, Value = "B" };
            var e3 = new CompoundItem { Part1 = 2, Part2 = 1, Value = "C" };

            ctx.Add(e1); ctx.Add(e2); ctx.Add(e3);
            await ctx.SaveChangesAsync();
            Assert.Equal(3, ctx.ChangeTracker.Entries.Count());

            // Delete only e2
            ctx.Remove(e2);
            await ctx.SaveChangesAsync();

            var remaining = ctx.ChangeTracker.Entries.ToList();
            Assert.Equal(2, remaining.Count);
            Assert.All(remaining, e => Assert.NotNull(e.Entity));
            Assert.Contains(remaining, e => ReferenceEquals(e.Entity, e1));
            Assert.Contains(remaining, e => ReferenceEquals(e.Entity, e3));
        }
    }

    /// <summary>
    /// CT-1: After Remove + re-insert, a NEW object instance with the same composite PK
    /// must be tracked independently (no confusion with the previously removed instance).
    /// </summary>
    [Fact]
    public async Task AfterRemove_NewInstanceWithSameCompositeKey_TrackedAsFresh()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var original = new CompoundItem { Part1 = 5, Part2 = 6, Value = "old" };
            ctx.Add(original);
            await ctx.SaveChangesAsync();

            ctx.Remove(original);
            await ctx.SaveChangesAsync();
            Assert.Empty(ctx.ChangeTracker.Entries);

            // Insert and track a fresh instance with the same composite PK
            var fresh = new CompoundItem { Part1 = 5, Part2 = 6, Value = "new" };
            ctx.Add(fresh);
            await ctx.SaveChangesAsync();

            var entries = ctx.ChangeTracker.Entries.ToList();
            Assert.Single(entries);
            // Must be the fresh instance, not the removed original
            Assert.Same(fresh, entries[0].Entity);
        }
    }

    /// <summary>
    /// CT-1: After Remove, the identity-map dedup contract must hold for subsequent
    /// attaches: two Attach calls for the same composite PK return the SAME entry.
    /// Before the fix, the zombie left by failed cleanup caused the second Attach
    /// to find the zombie instead of the live entity.
    /// </summary>
    [Fact]
    public async Task AfterRemoveAndReattach_DuplicateAttach_DeduplicatesCorrectly()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var item = new CompoundItem { Part1 = 3, Part2 = 4, Value = "dedup-test" };
            ctx.Add(item);
            await ctx.SaveChangesAsync();

            ctx.Remove(item);
            await ctx.SaveChangesAsync();
            Assert.Empty(ctx.ChangeTracker.Entries);

            // Re-attach same instance
            var entry1 = ctx.Attach(item);
            // Attach again — identity map must deduplicate and return the same entry
            var entry2 = ctx.Attach(item);

            Assert.Same(entry1, entry2);
            Assert.Single(ctx.ChangeTracker.Entries);
            Assert.NotNull(entry1.Entity);
        }
    }
}
