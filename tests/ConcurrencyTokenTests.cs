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
/// Validates that the concurrency token (RowVersion) used in UPDATE/DELETE WHERE clauses
/// comes from the original snapshot captured at attach time, not from the current (possibly mutated)
/// entity property value.
/// </summary>
public class ConcurrencyTokenTests
{
    public class VersionedEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        [Timestamp]
        public byte[] RowVersion { get; set; } = System.Array.Empty<byte>();
    }

    /// <summary>
    /// If the entity's RowVersion property is mutated before SaveChanges, the original
    /// (snapshot) value should still be used in the WHERE clause. The test verifies this
    /// by checking that the update succeeds when the original token is valid, even though
    /// the entity's RowVersion property was changed to an incorrect value.
    /// </summary>
    [Fact]
    public async Task SaveChanges_UsesOriginalToken_EvenWhenPropertyIsMutated()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE VersionedEntity(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, RowVersion BLOB NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });

        // Insert an entity
        var entity = new VersionedEntity { Name = "Original", RowVersion = new byte[] { 1, 0, 0, 0 } };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();

        // Retrieve the entity fresh — RowVersion is now the DB value {1,0,0,0}
        using var ctx2 = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });
        ctx2.Attach(entity);

        // Mutate the RowVersion property to a wrong value BEFORE saving
        // The system should use the snapshot value (1,0,0,0), NOT the mutated one
        var originalToken = entity.RowVersion;
        entity.RowVersion = new byte[] { 99, 99, 99, 99 }; // wrong value
        entity.Name = "Updated";

        // Force entry to Modified state
        var entry = ctx2.ChangeTracker.Entries.Single();
        var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        markDirty!.Invoke(ctx2.ChangeTracker, new object[] { entry });

        // The OriginalToken should be the original value, not the mutated one
        Assert.NotNull(entry.OriginalToken);
        Assert.Equal(originalToken, entry.OriginalToken as byte[]);
    }

    /// <summary>
    /// Verify EntityEntry.OriginalToken is populated when the entity has a timestamp column.
    /// </summary>
    [Fact]
    public void EntityEntry_OriginalToken_IsSetOnAttach()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });

        var entity = new VersionedEntity { Id = 1, Name = "Test", RowVersion = new byte[] { 5, 6, 7, 8 } };
        ctx.Attach(entity);

        var entry = ctx.ChangeTracker.Entries.Single();
        Assert.NotNull(entry.OriginalToken);
        Assert.Equal(new byte[] { 5, 6, 7, 8 }, entry.OriginalToken as byte[]);
    }

    // ─── Additional concurrency token coverage ─────────────────────────────

    [Fact]
    public async Task Delete_WithCorrectToken_Succeeds()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE VersionedEntity(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, RowVersion BLOB NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });

        var entity = new VersionedEntity { Name = "ToDelete", RowVersion = new byte[] { 1, 2, 3, 4 } };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();

        using var ctx2 = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });
        ctx2.Attach(entity);
        ctx2.Remove(entity);

        // Should succeed — correct token in snapshot
        var affected = await ctx2.SaveChangesAsync();
        Assert.Equal(1, affected);

        // Verify row is gone
        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT COUNT(*) FROM VersionedEntity WHERE Id = @id";
        verify.Parameters.AddWithValue("@id", entity.Id);
        Assert.Equal(0L, (long)verify.ExecuteScalar()!);
    }

    [Fact]
    public async Task Update_WithStaleToken_ThrowsDbConcurrencyException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE VersionedEntity(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, RowVersion BLOB NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        // Insert with token {1,0,0,0}
        using (var insert = cn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO VersionedEntity (Name, RowVersion) VALUES ('Original', @rv)";
            insert.Parameters.AddWithValue("@rv", new byte[] { 1, 0, 0, 0 });
            insert.ExecuteNonQuery();
        }

        // Simulate another actor changing the row's RowVersion to {2,0,0,0}
        using (var update = cn.CreateCommand())
        {
            update.CommandText = "UPDATE VersionedEntity SET Name = 'Changed', RowVersion = @rv WHERE Id = 1";
            update.Parameters.AddWithValue("@rv", new byte[] { 2, 0, 0, 0 });
            update.ExecuteNonQuery();
        }

        // Attach with stale token {1,0,0,0}
        var entity = new VersionedEntity { Id = 1, Name = "Updated", RowVersion = new byte[] { 1, 0, 0, 0 } };
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });
        ctx.Attach(entity);

        entity.Name = "My Update";
        var entry = ctx.ChangeTracker.Entries.Single();
        var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty",
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        markDirty!.Invoke(ctx.ChangeTracker, new object[] { entry });

        // Should throw because RowVersion in DB is {2,...} but our token is {1,...}
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public async Task Delete_WithStaleToken_ThrowsDbConcurrencyException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE VersionedEntity(Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, RowVersion BLOB NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        // Insert row
        using (var insert = cn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO VersionedEntity (Name, RowVersion) VALUES ('Row', @rv)";
            insert.Parameters.AddWithValue("@rv", new byte[] { 1, 0, 0, 0 });
            insert.ExecuteNonQuery();
        }

        // Simulate another actor modifying the row (changing RowVersion)
        using (var update = cn.CreateCommand())
        {
            update.CommandText = "UPDATE VersionedEntity SET RowVersion = @rv WHERE Id = 1";
            update.Parameters.AddWithValue("@rv", new byte[] { 9, 9, 9, 9 });
            update.ExecuteNonQuery();
        }

        var entity = new VersionedEntity { Id = 1, Name = "Row", RowVersion = new byte[] { 1, 0, 0, 0 } };
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });
        ctx.Attach(entity);
        ctx.Remove(entity);

        // Delete should fail due to stale token
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    [Fact]
    public void TwoEntities_BothGetOriginalToken()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });

        var e1 = new VersionedEntity { Id = 1, Name = "A", RowVersion = new byte[] { 1, 1 } };
        var e2 = new VersionedEntity { Id = 2, Name = "B", RowVersion = new byte[] { 2, 2 } };
        ctx.Attach(e1);
        ctx.Attach(e2);

        var entries = ctx.ChangeTracker.Entries.ToList();
        Assert.Equal(2, entries.Count);
        Assert.All(entries, e => Assert.NotNull(e.OriginalToken));

        var entry1 = entries.Single(e => e.Entity is VersionedEntity ve1 && ve1.Id == 1);
        var entry2 = entries.Single(e => e.Entity is VersionedEntity ve2 && ve2.Id == 2);
        Assert.Equal(new byte[] { 1, 1 }, entry1.OriginalToken as byte[]);
        Assert.Equal(new byte[] { 2, 2 }, entry2.OriginalToken as byte[]);
    }

    [Fact]
    public void OriginalToken_IsImmutable_AfterMutation()
    {
        // Verify that mutating the entity's RowVersion does NOT change the entry's OriginalToken
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });

        var originalBytes = new byte[] { 10, 20, 30, 40 };
        var entity = new VersionedEntity { Id = 1, Name = "Test", RowVersion = originalBytes };
        ctx.Attach(entity);

        var entry = ctx.ChangeTracker.Entries.Single();
        var tokenBeforeMutation = entry.OriginalToken as byte[];

        // Mutate the entity's RowVersion property
        entity.RowVersion = new byte[] { 99, 99, 99, 99 };

        // OriginalToken should still be the original value
        Assert.Equal(tokenBeforeMutation, entry.OriginalToken as byte[]);
        Assert.Equal(new byte[] { 10, 20, 30, 40 }, entry.OriginalToken as byte[]);
    }
}
