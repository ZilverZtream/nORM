using System;
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

namespace nORM.Tests;

/// <summary>
/// SQL-1: Verifies that the concurrency-token WHERE predicate is null-safe.
///
/// Old SQL: col = @param              (always FALSE when both are NULL in SQL)
/// New SQL: (col = @param OR (col IS NULL AND @param IS NULL))
///
/// Without the fix, an entity whose timestamp column is NULL in both the entity
/// (OriginalToken == null) and the DB gets 0 rows-affected → spurious DbConcurrencyException.
/// </summary>
public class NullableConcurrencyTokenTests
{
    [Table("NctEntity")]
    private class NctEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.None)]
        public int Id { get; set; }

        public string Name { get; set; } = string.Empty;

        [Timestamp]
        public byte[]? RowVersion { get; set; }  // nullable: can be NULL in DB
    }

    private static SqliteConnection CreateSchema()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        // RowVersion is nullable — no NOT NULL constraint
        cmd.CommandText = "CREATE TABLE NctEntity (Id INTEGER PRIMARY KEY, Name TEXT, RowVersion BLOB);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext CreateCtx(SqliteConnection cn)
        => new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true });

    private static async Task InsertRawAsync(SqliteConnection cn, int id, string name, byte[]? rowVersion)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO NctEntity (Id, Name, RowVersion) VALUES (@id, @name, @rv)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@rv", rowVersion as object ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> ReadNameAsync(SqliteConnection cn, int id)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM NctEntity WHERE Id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return await cmd.ExecuteScalarAsync() as string;
    }

    private static async Task<long> CountAsync(SqliteConnection cn, int id)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM NctEntity WHERE Id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    // Helper: mark entity as Modified so SaveChanges will emit UPDATE
    private static void MarkModified(DbContext ctx, object entity)
    {
        var entry = ctx.ChangeTracker.Entries.Single(e => ReferenceEquals(e.Entity, entity));
        var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty",
            BindingFlags.Instance | BindingFlags.NonPublic);
        markDirty!.Invoke(ctx.ChangeTracker, new object[] { entry });
    }

    // ── UPDATE tests ──────────────────────────────────────────────────────────

    /// <summary>
    /// Both entity OriginalToken and DB value are NULL.
    /// Without the fix: WHERE RowVersion = NULL → FALSE → 0 rows → DbConcurrencyException.
    /// With the fix:    WHERE (RowVersion = NULL OR (RowVersion IS NULL AND NULL IS NULL)) → TRUE.
    /// </summary>
    [Fact]
    public async Task Update_NullToken_NullInDb_Succeeds()
    {
        using var cn = CreateSchema();
        await InsertRawAsync(cn, 1, "Original", null);

        using var ctx = CreateCtx(cn);
        var entity = new NctEntity { Id = 1, Name = "Original", RowVersion = null };
        ctx.Attach(entity);

        entity.Name = "Updated";
        MarkModified(ctx, entity);

        // Must not throw — null token matches null DB value
        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);
        Assert.Equal("Updated", await ReadNameAsync(cn, 1));
    }

    /// <summary>
    /// Entity OriginalToken is NULL but DB has a non-null RowVersion.
    /// The WHERE predicate should NOT match → DbConcurrencyException.
    /// </summary>
    [Fact]
    public async Task Update_NullToken_NonNullInDb_ThrowsConcurrencyException()
    {
        using var cn = CreateSchema();
        await InsertRawAsync(cn, 1, "Original", new byte[] { 1, 2, 3, 4 });

        using var ctx = CreateCtx(cn);
        // Attach with null RowVersion (token) even though DB has {1,2,3,4}
        var entity = new NctEntity { Id = 1, Name = "Original", RowVersion = null };
        ctx.Attach(entity);

        entity.Name = "Updated";
        MarkModified(ctx, entity);

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }

    /// <summary>
    /// Regression guard: non-null matching token should still succeed.
    /// </summary>
    [Fact]
    public async Task Update_NonNullToken_Match_Succeeds()
    {
        using var cn = CreateSchema();
        await InsertRawAsync(cn, 1, "Original", new byte[] { 5, 6, 7, 8 });

        using var ctx = CreateCtx(cn);
        var entity = new NctEntity { Id = 1, Name = "Original", RowVersion = new byte[] { 5, 6, 7, 8 } };
        ctx.Attach(entity);

        entity.Name = "Updated";
        MarkModified(ctx, entity);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);
        Assert.Equal("Updated", await ReadNameAsync(cn, 1));
    }

    // ── DELETE tests ──────────────────────────────────────────────────────────

    /// <summary>
    /// Both entity OriginalToken and DB RowVersion are NULL.
    /// DELETE WHERE predicate should be null-safe and match the row.
    /// </summary>
    [Fact]
    public async Task Delete_NullToken_NullInDb_Succeeds()
    {
        using var cn = CreateSchema();
        await InsertRawAsync(cn, 2, "ToDelete", null);

        using var ctx = CreateCtx(cn);
        var entity = new NctEntity { Id = 2, Name = "ToDelete", RowVersion = null };
        ctx.Attach(entity);
        ctx.Remove(entity);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);
        Assert.Equal(0L, await CountAsync(cn, 2));
    }

    /// <summary>
    /// Entity OriginalToken is NULL but DB RowVersion is non-null → DELETE should fail with DbConcurrencyException.
    /// </summary>
    [Fact]
    public async Task Delete_NullToken_NonNullInDb_ThrowsConcurrencyException()
    {
        using var cn = CreateSchema();
        await InsertRawAsync(cn, 2, "ToDelete", new byte[] { 9, 8, 7, 6 });

        using var ctx = CreateCtx(cn);
        var entity = new NctEntity { Id = 2, Name = "ToDelete", RowVersion = null };
        ctx.Attach(entity);
        ctx.Remove(entity);

        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }
}
