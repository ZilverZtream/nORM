using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests
{
    public class CollidingValue
    {
        public string Value { get; set; } = string.Empty;
        public override int GetHashCode() => 1;
    }

    public class CollisionEntity
    {
        public int Id { get; set; }
        public CollidingValue Data { get; set; } = new();
    }

    public class PreciseChangeTrackingTests
    {
        [Fact]
        public void Hash_collision_detected_by_default()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var ctx = new DbContext(cn, new SqliteProvider());
            var entity = new CollisionEntity { Id = 1, Data = new CollidingValue { Value = "A" } };
            ctx.Attach(entity);
            entity.Data = new CollidingValue { Value = "B" };
            var entry = ctx.ChangeTracker.Entries.Single();
            var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            markDirty!.Invoke(ctx.ChangeTracker, new object[] { entry });
            var detect = typeof(ChangeTracker).GetMethod("DetectChanges", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            detect!.Invoke(ctx.ChangeTracker, null);
            var state = ctx.ChangeTracker.Entries.Single().State;
            Assert.Equal(EntityState.Modified, state);
        }

        [Fact]
        public void Hash_collision_detected_with_precise_change_tracking()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { UsePreciseChangeTracking = true });
            var entity = new CollisionEntity { Id = 1, Data = new CollidingValue { Value = "A" } };
            ctx.Attach(entity);
            entity.Data = new CollidingValue { Value = "B" };
            var entry = ctx.ChangeTracker.Entries.Single();
            var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            markDirty!.Invoke(ctx.ChangeTracker, new object[] { entry });
            var detect = typeof(ChangeTracker).GetMethod("DetectChanges", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            detect!.Invoke(ctx.ChangeTracker, null);
            var state = ctx.ChangeTracker.Entries.Single().State;
            Assert.Equal(EntityState.Modified, state);
        }

        [Fact]
        public void Non_notifying_entity_requires_dirty_flag()
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            cn.Open();
            using var ctx = new DbContext(cn, new SqliteProvider());
            var entity = new CollisionEntity { Id = 1, Data = new CollidingValue { Value = "A" } };
            ctx.Attach(entity);
            entity.Data = new CollidingValue { Value = "B" };
            var detect = typeof(ChangeTracker).GetMethod("DetectChanges", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            detect!.Invoke(ctx.ChangeTracker, null);
            var state = ctx.ChangeTracker.Entries.Single().State;
            Assert.Equal(EntityState.Unchanged, state);

            var entry = ctx.ChangeTracker.Entries.Single();
            var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
            markDirty!.Invoke(ctx.ChangeTracker, new object[] { entry });
            detect.Invoke(ctx.ChangeTracker, null);
            state = ctx.ChangeTracker.Entries.Single().State;
            Assert.Equal(EntityState.Modified, state);
        }
    }
}
