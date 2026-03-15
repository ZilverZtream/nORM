using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Verifies that UpdateAsync/DeleteAsync use the original snapshot concurrency token
/// (not the current possibly-mutated property value) for the optimistic-concurrency WHERE predicate,
/// matching the parity of the batched SaveChangesAsync path.
/// </summary>
public class ConcurrencyTokenSnapshotTests
{
    [Table("CtsItem")]
    private class CtsItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;

        [Timestamp]
        public string? ConcToken { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CtsItem (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                ConcToken TEXT NOT NULL DEFAULT 'v1'
            )";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ── snapshot token vs current token ────────────────────────────────

    /// <summary>
    /// When the entity's ConcToken property is mutated after being tracked,
    /// UpdateAsync must use the original snapshot value in the WHERE predicate —
    /// not the mutated current value — to correctly detect a concurrent modification.
    /// </summary>
    [Fact]
    public async Task UpdateAsync_MutatedConcToken_UsesSnapshotTokenInPredicate()
    {
        var (cn, ctx) = Create();
        using var _ = cn;

        // Seed the row manually with a known token
        using var seed = cn.CreateCommand();
        seed.CommandText = "INSERT INTO CtsItem (Name, ConcToken) VALUES ('original', 'v1'); SELECT last_insert_rowid()";
        var id = Convert.ToInt32(seed.ExecuteScalar());

        // Attach the entity so it's tracked (snapshot token = 'v1')
        var entity = new CtsItem { Id = id, Name = "original", ConcToken = "v1" };
        ctx.Attach(entity);

        // Now mutate BOTH the Name AND the ConcToken (simulating what EF would do on read)
        entity.Name = "updated";
        entity.ConcToken = "v2-mutated"; // token changed on the entity object

        // UpdateAsync must build WHERE Id=? AND ConcToken='v1' (snapshot),
        // NOT WHERE Id=? AND ConcToken='v2-mutated' (current). Since the DB still has
        // 'v1', the WHERE must match → rowsAffected = 1.
        int affected = await ctx.UpdateAsync(entity);

        Assert.Equal(1, affected);

        using var check = cn.CreateCommand();
        check.CommandText = $"SELECT Name FROM CtsItem WHERE Id = {id}";
        Assert.Equal("updated", check.ExecuteScalar()?.ToString());
    }

    /// <summary>
    /// When the DB row has been modified externally (token mismatch), UpdateAsync
    /// must detect the conflict and throw DbConcurrencyException — using the original
    /// snapshot token (not any mutated value).
    /// </summary>
    [Fact]
    public async Task UpdateAsync_ExternalModification_ThrowsDbConcurrencyException()
    {
        var (cn, ctx) = Create();
        using var _ = cn;

        using var seed = cn.CreateCommand();
        seed.CommandText = "INSERT INTO CtsItem (Name, ConcToken) VALUES ('original', 'v1'); SELECT last_insert_rowid()";
        var id = Convert.ToInt32(seed.ExecuteScalar());

        var entity = new CtsItem { Id = id, Name = "original", ConcToken = "v1" };
        ctx.Attach(entity);

        // External modification changes the token in the DB
        using var external = cn.CreateCommand();
        external.CommandText = $"UPDATE CtsItem SET ConcToken = 'v2-external' WHERE Id = {id}";
        external.ExecuteNonQuery();

        entity.Name = "should-fail";

        // WHERE predicate uses snapshot 'v1', but DB now has 'v2-external' → 0 rows affected → conflict
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.UpdateAsync(entity));
    }

    /// <summary>
    /// DeleteAsync with a mutated ConcToken must use the snapshot token in the
    /// WHERE predicate, not the mutated current value.
    /// </summary>
    [Fact]
    public async Task DeleteAsync_MutatedConcToken_UsesSnapshotToken()
    {
        var (cn, ctx) = Create();
        using var _ = cn;

        using var seed = cn.CreateCommand();
        seed.CommandText = "INSERT INTO CtsItem (Name, ConcToken) VALUES ('to-delete', 'v1'); SELECT last_insert_rowid()";
        var id = Convert.ToInt32(seed.ExecuteScalar());

        var entity = new CtsItem { Id = id, Name = "to-delete", ConcToken = "v1" };
        ctx.Attach(entity);

        // Mutate the token on the entity
        entity.ConcToken = "v9-wrong";

        // DELETE WHERE Id=? AND ConcToken='v1' (snapshot) — should match DB row → 1 deleted
        int affected = await ctx.DeleteAsync(entity);

        Assert.Equal(1, affected);

        using var check = cn.CreateCommand();
        check.CommandText = $"SELECT COUNT(*) FROM CtsItem WHERE Id = {id}";
        Assert.Equal(0L, (long)check.ExecuteScalar()!);
    }

    /// <summary>
    /// Entity not in the change tracker falls back to current entity property value.
    /// This preserves existing behavior for untracked entities passed directly to UpdateAsync.
    /// </summary>
    [Fact]
    public async Task UpdateAsync_UntrackedEntity_UsesCurrentPropertyValue()
    {
        var (cn, ctx) = Create();
        using var _ = cn;

        using var seed = cn.CreateCommand();
        seed.CommandText = "INSERT INTO CtsItem (Name, ConcToken) VALUES ('untracked', 'v1'); SELECT last_insert_rowid()";
        var id = Convert.ToInt32(seed.ExecuteScalar());

        // Untracked — no snapshot in ChangeTracker
        var entity = new CtsItem { Id = id, Name = "modified-untracked", ConcToken = "v1" };

        // Falls back to current property ('v1') which matches DB → update succeeds
        int affected = await ctx.UpdateAsync(entity);

        Assert.Equal(1, affected);
    }
}
