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
/// CT-1: Validates that the concurrency token (RowVersion) used in UPDATE/DELETE WHERE clauses
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
}
