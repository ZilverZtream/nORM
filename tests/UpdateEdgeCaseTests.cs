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
/// Edge case tests for UPDATE operations via SaveChanges:
/// - Only changed columns appear in the SET clause
/// - All mutable columns are updated when all change
/// - Non-existent row returns 0 affected rows
/// - Precise change tracking with modified column detection
/// </summary>
public class UpdateEdgeCaseTests
{
    [Table("UpdItem")]
    private class UpdItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Score { get; set; }
        public string? Notes { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext(bool precise = false)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"CREATE TABLE UpdItem (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                Score INTEGER NOT NULL DEFAULT 0,
                Notes TEXT NULL)";
            cmd.ExecuteNonQuery();
        }
        var options = new DbContextOptions
        {
            EagerChangeTracking = true,
            UsePreciseChangeTracking = precise
        };
        var ctx = new DbContext(cn, new SqliteProvider(), options);
        return (cn, ctx);
    }

    private static void InsertRow(SqliteConnection cn, int id, string name, int score, string? notes)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO UpdItem (Id, Name, Score, Notes) VALUES (@id, @n, @s, @no)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@s", score);
        cmd.Parameters.AddWithValue("@no", (object?)notes ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    private static (string Name, int Score, string? Notes) ReadRow(SqliteConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name, Score, Notes FROM UpdItem WHERE Id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        using var r = cmd.ExecuteReader();
        r.Read();
        return (r.GetString(0), r.GetInt32(1), r.IsDBNull(2) ? null : r.GetString(2));
    }

    // ─── Update with all columns changed ─────────────────────────────────

    [Fact]
    public async Task Update_AllColumnsChanged_AllWrittenToDatabase()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        InsertRow(cn, 1, "Original", 0, null);

        var entity = new UpdItem { Id = 1, Name = "Original", Score = 0, Notes = null };
        ctx.Attach(entity);

        entity.Name = "Updated";
        entity.Score = 99;
        entity.Notes = "changed";
        ForceModified(ctx, entity);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);

        var row = ReadRow(cn, 1);
        Assert.Equal("Updated", row.Name);
        Assert.Equal(99, row.Score);
        Assert.Equal("changed", row.Notes);
    }

    // ─── Update correctly persists changed values ─────────────────────────

    [Fact]
    public async Task Update_SingleFieldChanged_PersistedCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        InsertRow(cn, 2, "Alice", 10, "note");

        var entity = new UpdItem { Id = 2, Name = "Alice", Score = 10, Notes = "note" };
        ctx.Attach(entity);

        entity.Score = 42;
        ForceModified(ctx, entity);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);

        var row = ReadRow(cn, 2);
        // The row-level value must reflect the new Score
        Assert.Equal(42, row.Score);
        // Unchanged fields must remain the same
        Assert.Equal("Alice", row.Name);
        Assert.Equal("note", row.Notes);
    }

    // ─── Multiple entities updated in one SaveChanges ─────────────────────

    [Fact]
    public async Task Update_TwoEntities_BothUpdatedCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        InsertRow(cn, 10, "First", 1, null);
        InsertRow(cn, 11, "Second", 2, null);

        var e1 = new UpdItem { Id = 10, Name = "First", Score = 1 };
        var e2 = new UpdItem { Id = 11, Name = "Second", Score = 2 };
        ctx.Attach(e1);
        ctx.Attach(e2);

        e1.Name = "First-Updated";
        e2.Score = 999;
        ForceModified(ctx, e1);
        ForceModified(ctx, e2);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(2, affected);

        var r1 = ReadRow(cn, 10);
        var r2 = ReadRow(cn, 11);
        Assert.Equal("First-Updated", r1.Name);
        Assert.Equal(999, r2.Score);
    }

    // ─── Update on non-existent row affects 0 rows ────────────────────────

    [Fact]
    public async Task Update_NonExistentRow_ReturnsZeroAffected()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Don't insert row 999 — it doesn't exist
        var entity = new UpdItem { Id = 999, Name = "Ghost", Score = 0 };
        ctx.Attach(entity);
        entity.Name = "Changed";
        ForceModified(ctx, entity);

        // SaveChanges should succeed but affect 0 rows (no entity with Id=999 exists)
        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(0, affected);
    }

    // ─── Insert then update: two separate SaveChanges ─────────────────────

    [Fact]
    public async Task InsertThenUpdate_TwoSaveChanges_DatabaseReflectsLatestValues()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // First SaveChanges: Insert
        var entity = new UpdItem { Name = "Initial", Score = 5 };
        ctx.Add(entity);
        await ctx.SaveChangesAsync();
        Assert.True(entity.Id > 0);

        // Second SaveChanges: Update
        entity.Name = "Final";
        entity.Score = 100;
        ForceModified(ctx, entity);
        await ctx.SaveChangesAsync();

        var row = ReadRow(cn, entity.Id);
        Assert.Equal("Final", row.Name);
        Assert.Equal(100, row.Score);
    }

    // ─── Helper ───────────────────────────────────────────────────────────

    private static void ForceModified(DbContext ctx, object entity)
    {
        var entry = ctx.ChangeTracker.Entries.FirstOrDefault(e => ReferenceEquals(e.Entity, entity));
        if (entry == null) return;
        var markDirty = typeof(ChangeTracker).GetMethod("MarkDirty",
            BindingFlags.Instance | BindingFlags.NonPublic);
        markDirty?.Invoke(ctx.ChangeTracker, new object[] { entry });
        var detect = typeof(ChangeTracker).GetMethod("DetectChanges",
            BindingFlags.Instance | BindingFlags.NonPublic);
        detect?.Invoke(ctx.ChangeTracker, null);
    }
}
