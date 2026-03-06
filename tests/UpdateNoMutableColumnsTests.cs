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

namespace nORM.Tests;

/// <summary>
/// CT-1: Verifies that UpdateAsync (single-entity) and SaveChanges (batch) both throw
/// NormConfigurationException for entities that have no mutable columns (key-only, or
/// key + concurrency-token-only). Also ensures a normal entity with one mutable column works.
/// </summary>
public class UpdateNoMutableColumnsTests
{
    // PK-only entity — no mutable columns at all.
    [Table("PkOnlyEntity")]
    private class PkOnlyEntity
    {
        [Key] public int Id { get; set; }
    }

    // PK + timestamp-only entity — timestamp is excluded from SET, so still no mutable columns.
    [Table("PkTokenEntity")]
    private class PkTokenEntity
    {
        [Key] public int Id { get; set; }
        [Timestamp] public byte[]? RowVersion { get; set; }
    }

    // Normal entity with one mutable column — should work fine.
    [Table("NormalEntity")]
    private class NormalEntity
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContextWithTable()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NormalEntity (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
                              "INSERT INTO NormalEntity (Name) VALUES ('initial');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions { EagerChangeTracking = true }));
    }

    // ─── UpdateAsync (single-entity path) ────────────────────────────────────

    [Fact]
    public async Task UpdateAsync_PkOnlyEntity_ThrowsNormConfigurationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var entity = new PkOnlyEntity { Id = 1 };
        await Assert.ThrowsAsync<NormConfigurationException>(() => ctx.UpdateAsync(entity));
    }

    [Fact]
    public async Task UpdateAsync_PkTokenOnlyEntity_ThrowsNormConfigurationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var entity = new PkTokenEntity { Id = 1 };
        await Assert.ThrowsAsync<NormConfigurationException>(() => ctx.UpdateAsync(entity));
    }

    // ─── SaveChanges (batch path) ─────────────────────────────────────────────

    [Fact]
    public async Task SaveChanges_PkOnlyEntity_Modified_ThrowsNormConfigurationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var entity = new PkOnlyEntity { Id = 1 };
        ctx.Update(entity);
        await Assert.ThrowsAsync<NormConfigurationException>(() => ctx.SaveChangesAsync(detectChanges: false));
    }

    [Fact]
    public async Task SaveChanges_PkTokenOnlyEntity_Modified_ThrowsNormConfigurationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var entity = new PkTokenEntity { Id = 1 };
        ctx.Update(entity);
        await Assert.ThrowsAsync<NormConfigurationException>(() => ctx.SaveChangesAsync(detectChanges: false));
    }

    // ─── Regression: normal entity with mutable column succeeds ──────────────

    [Fact]
    public async Task UpdateAsync_NormalEntity_Succeeds()
    {
        var (cn, ctx) = CreateContextWithTable();
        using var _cn = cn; using var _ctx = ctx;
        var entity = new NormalEntity { Id = 1, Name = "updated" };
        // Should not throw — has one mutable column (Name)
        var affected = await ctx.UpdateAsync(entity);
        Assert.Equal(1, affected);
    }
}
