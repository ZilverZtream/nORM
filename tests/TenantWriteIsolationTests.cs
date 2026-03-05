using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SEC-MT: Verify that UPDATE/DELETE WHERE clauses include the tenant column predicate
/// so that a batch operation cannot affect rows belonging to a different tenant.
/// </summary>
public class TenantWriteIsolationTests
{
    [Table("TwItem")]
    private class TwItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string TenantId { get; set; } = string.Empty;
    }

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _tenantId;
        public FixedTenantProvider(string tenantId) => _tenantId = tenantId;
        public object GetCurrentTenantId() => _tenantId;
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> CreateContextAsync(string tenantId)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"CREATE TABLE TwItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);";
        cmd.ExecuteNonQuery();

        var opts = new DbContextOptions { TenantProvider = new FixedTenantProvider(tenantId) };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        await Task.CompletedTask;
        return (cn, ctx);
    }

    private static async Task InsertRawAsync(SqliteConnection cn, int id, string name, string tenantId)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO TwItem (Id, Name, TenantId) VALUES (@id, @name, @tid)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<long> CountAsync(SqliteConnection cn, int id, string tenantId)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM TwItem WHERE Id=@id AND TenantId=@tid";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static async Task<string?> ReadNameAsync(SqliteConnection cn, int id)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM TwItem WHERE Id=@id";
        cmd.Parameters.AddWithValue("@id", id);
        var result = await cmd.ExecuteScalarAsync();
        return result as string;
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task CrossTenantDelete_DoesNotDeleteRow()
    {
        var (cn, ctx) = await CreateContextAsync("A");
        using var _cn = cn;
        using var _ctx = ctx;

        // Insert a row owned by tenant "B"
        await InsertRawAsync(cn, 1, "OtherTenantRow", "B");

        // Attach entity claiming TenantId="A" (same PK, different tenant column value)
        // This is the "cross-tenant confusion" scenario: the entity in memory has the
        // wrong TenantId compared to what's actually stored.
        var entity = new TwItem { Id = 1, Name = "OtherTenantRow", TenantId = "A" };
        ctx.Attach(entity);
        ctx.Remove(entity);

        // SaveChanges emits: DELETE FROM TwItem WHERE "Id"=@p0 AND "TenantId"=@p1
        // @p1 is bound to "A" (from TenantProvider), so the row with TenantId="B" is NOT affected.
        await ctx.SaveChangesAsync();

        // Row with TenantId="B" must still exist
        Assert.Equal(1L, await CountAsync(cn, 1, "B"));
    }

    [Fact]
    public async Task CrossTenantUpdate_DoesNotModifyRow()
    {
        var (cn, ctx) = await CreateContextAsync("A");
        using var _cn = cn;
        using var _ctx = ctx;

        // Insert a row owned by tenant "B"
        await InsertRawAsync(cn, 2, "OriginalName", "B");

        var entity = new TwItem { Id = 2, Name = "OriginalName", TenantId = "A" };
        ctx.Attach(entity);

        // Modify Name and mark as Modified
        entity.Name = "ModifiedName";
        ctx.Update(entity);

        // SaveChanges emits: UPDATE TwItem SET ... WHERE "Id"=@p1 AND "TenantId"=@p2
        // @p2 = "A" from TenantProvider → row with TenantId="B" not touched
        await ctx.SaveChangesAsync();

        // Name in DB must be unchanged
        Assert.Equal("OriginalName", await ReadNameAsync(cn, 2));
    }

    [Fact]
    public async Task SameTenantDelete_DeletesRow()
    {
        var (cn, ctx) = await CreateContextAsync("A");
        using var _cn = cn;
        using var _ctx = ctx;

        // Insert row belonging to tenant "A"
        await InsertRawAsync(cn, 3, "MyRow", "A");

        var entity = new TwItem { Id = 3, Name = "MyRow", TenantId = "A" };
        ctx.Attach(entity);
        ctx.Remove(entity);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);
        Assert.Equal(0L, await CountAsync(cn, 3, "A"));
    }

    [Fact]
    public async Task SameTenantUpdate_UpdatesRow()
    {
        var (cn, ctx) = await CreateContextAsync("A");
        using var _cn = cn;
        using var _ctx = ctx;

        // Insert row belonging to tenant "A"
        await InsertRawAsync(cn, 4, "Original", "A");

        var entity = new TwItem { Id = 4, Name = "Original", TenantId = "A" };
        ctx.Attach(entity);
        entity.Name = "Updated";
        ctx.Update(entity);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);
        Assert.Equal("Updated", await ReadNameAsync(cn, 4));
    }
}
