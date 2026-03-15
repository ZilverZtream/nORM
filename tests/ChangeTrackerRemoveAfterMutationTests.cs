using System.ComponentModel.DataAnnotations;
using System.Linq;
using System.Reflection;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>Identity-map must not be corrupted when Remove() is called after PK mutation.</summary>
public class ChangeTrackerRemoveAfterMutationTests
{
    private class MutableKeyEntity
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE MutableKeyEntity (Id INTEGER PRIMARY KEY, Name TEXT);" +
                          "INSERT INTO MutableKeyEntity VALUES (1, 'original');";
        cmd.ExecuteNonQuery();
        var ctx = new DbContext(cn, new SqliteProvider(),
            new DbContextOptions { EagerChangeTracking = true });
        return (cn, ctx);
    }

    private static void CallTrackerRemove(ChangeTracker tracker, object entity)
    {
        var method = typeof(ChangeTracker).GetMethod("Remove",
            BindingFlags.Instance | BindingFlags.NonPublic,
            null,
            new[] { typeof(object), typeof(bool) },
            null)!;
        method.Invoke(tracker, new object[] { entity, false });
    }

    /// <summary>
    /// After attaching an entity with pk=1, mutating its PK to 999, and then
    /// calling Remove(), the tracker must use the original key (1) to clean up the
    /// identity map — not the mutated key (999). A subsequent Track of a fresh entity
    /// with pk=1 must succeed without getting a zombie null-entity entry.
    /// </summary>
    [Fact]
    public void Remove_AfterPkMutation_AllowsReattachByOriginalKey()
    {
        var (cn, ctx) = CreateCtx();
        using (cn) using (ctx)
        {
            var entity = new MutableKeyEntity { Id = 1, Name = "original" };
            ctx.Attach(entity);

            // Mutate the PK after attach — identity map still has key=1
            entity.Id = 999;

            // Remove should use OriginalKey (1), not current key (999)
            CallTrackerRemove(ctx.ChangeTracker, entity);

            // Now re-attach a fresh entity with pk=1; must not collide with zombie entry
            var fresh = new MutableKeyEntity { Id = 1, Name = "fresh" };
            ctx.Attach(fresh);

            var entry = ctx.ChangeTracker.Entries
                .FirstOrDefault(e => e.Entity is MutableKeyEntity m && m.Id == 1);

            Assert.NotNull(entry);
            Assert.NotNull(entry!.Entity);
            Assert.Same(fresh, entry.Entity);
        }
    }

    /// <summary>
    /// After attaching, mutating PK, and removing, the tracker must have zero entries.
    /// A zombie entry (Entity==null) left under the original key would cause this to fail.
    /// </summary>
    [Fact]
    public void Remove_AfterPkMutation_DoesNotLeaveZombieEntry()
    {
        var (cn, ctx) = CreateCtx();
        using (cn) using (ctx)
        {
            var entity = new MutableKeyEntity { Id = 1, Name = "test" };
            ctx.Attach(entity);

            entity.Id = 999;

            CallTrackerRemove(ctx.ChangeTracker, entity);

            Assert.Empty(ctx.ChangeTracker.Entries);
        }
    }

    /// <summary>
    /// Regression: normal Remove without PK mutation must still work correctly.
    /// </summary>
    [Fact]
    public void Remove_WithoutMutation_StillWorks()
    {
        var (cn, ctx) = CreateCtx();
        using (cn) using (ctx)
        {
            var entity = new MutableKeyEntity { Id = 1, Name = "test" };
            ctx.Attach(entity);

            // No PK mutation — Remove should work as before
            CallTrackerRemove(ctx.ChangeTracker, entity);

            Assert.Empty(ctx.ChangeTracker.Entries);

            // Re-attach a fresh entity with the same PK — must succeed
            var fresh = new MutableKeyEntity { Id = 1, Name = "fresh" };
            ctx.Attach(fresh);

            Assert.Single(ctx.ChangeTracker.Entries);
            Assert.Same(fresh, ctx.ChangeTracker.Entries.Single().Entity);
        }
    }
}
