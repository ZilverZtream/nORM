using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Mapping;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// X1: Verifies that provider-native BulkUpdateAsync and BulkDeleteAsync include
/// tenant predicates in the database-side WHERE/JOIN so cross-tenant rows cannot
/// be modified even if the caller supplies a foreign-tenant primary key.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkTenantIsolationTests
{
    [Table("BtiItem")]
    private class BtiItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string TenantId { get; set; } = string.Empty;
    }

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    private sealed class FallbackSqliteProvider : DatabaseProvider
    {
        public override int MaxSqlLength => 1_000_000;
        public override int MaxParameters => 999;
        public override string Escape(string id) => $"\"{id}\"";
        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            if (limitParameterName != null) sb.Append(" LIMIT ").Append(limitParameterName);
            if (offsetParameterName != null) sb.Append(" OFFSET ").Append(offsetParameterName);
        }

        public override string GetIdentityRetrievalString(TableMapping m) => "; SELECT last_insert_rowid();";
        public override DbParameter CreateParameter(string name, object? value) => new SqliteParameter(name, value ?? DBNull.Value);
        public override string? TranslateFunction(string name, Type declaringType, params string[] args) => null;
        public override string TranslateJsonPathAccess(string columnName, string jsonPath) => $"json_extract({columnName}, '{jsonPath}')";
        public override string GenerateCreateHistoryTableSql(TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null) => throw new NotImplementedException();
        public override string GenerateTemporalTriggersSql(TableMapping mapping, System.Collections.Generic.IReadOnlyList<LiveColumnInfo>? liveColumns = null) => throw new NotImplementedException();
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> MakeCtx(string tenantId, bool useBatchedBulkOps = false, bool useFallbackProvider = false)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE BtiItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)";
        await cmd.ExecuteNonQueryAsync();
        var opts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenantId),
            UseBatchedBulkOps = useBatchedBulkOps
        };
        DatabaseProvider provider = useFallbackProvider ? new FallbackSqliteProvider() : new SqliteProvider();
        return (cn, new DbContext(cn, provider, opts));
    }

    private static async Task InsertRawAsync(SqliteConnection cn, int id, string name, string tenantId)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO BtiItem (Id, Name, TenantId) VALUES (@id,@n,@tid)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> ReadNameAsync(SqliteConnection cn, int id)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM BtiItem WHERE Id=@id";
        cmd.Parameters.AddWithValue("@id", id);
        return (await cmd.ExecuteScalarAsync()) as string;
    }

    private static async Task<long> CountAsync(SqliteConnection cn, int id, string tenantId)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM BtiItem WHERE Id=@id AND TenantId=@tid";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    // ── BulkUpdateAsync — tenant isolation ──────────────────────────────────

    [Fact]
    public async Task BulkUpdate_CrossTenant_DoesNotModifyForeignTenantRow_SQLite()
    {
        var (cn, ctx) = await MakeCtx("A");
        await InsertRawAsync(cn, 10, "original", "B"); // row belongs to tenant B

        // Build entity with tenant-B's key but tenant-A context
        var entity = new BtiItem { Id = 10, Name = "tampered", TenantId = "A" };
        await ctx.BulkUpdateAsync(new[] { entity });

        // Row should be unchanged — tenant A's WHERE predicate excludes it
        var name = await ReadNameAsync(cn, 10);
        Assert.Equal("original", name);
    }

    [Fact]
    public async Task BulkUpdate_SameTenant_DoesModifyOwnRow_SQLite()
    {
        var (cn, ctx) = await MakeCtx("A");
        await InsertRawAsync(cn, 20, "before", "A");

        var entity = new BtiItem { Id = 20, Name = "after", TenantId = "A" };
        await ctx.BulkUpdateAsync(new[] { entity });

        var name = await ReadNameAsync(cn, 20);
        Assert.Equal("after", name);
    }

    [Fact]
    public async Task BulkUpdate_CrossTenant_MultipleEntities_OnlyOwnRowsAffected_SQLite()
    {
        var (cn, ctx) = await MakeCtx("A");
        await InsertRawAsync(cn, 30, "own-before", "A");
        await InsertRawAsync(cn, 31, "foreign-before", "B");

        var entities = new[]
        {
            new BtiItem { Id = 30, Name = "own-after", TenantId = "A" },
            new BtiItem { Id = 31, Name = "foreign-tampered", TenantId = "A" }
        };
        await ctx.BulkUpdateAsync(entities);

        Assert.Equal("own-after",      await ReadNameAsync(cn, 30));
        Assert.Equal("foreign-before", await ReadNameAsync(cn, 31)); // untouched
    }

    [Fact]
    public async Task BulkUpdate_NoTenantProvider_UpdatesAllRows_SQLite()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE BtiItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)";
        await setup.ExecuteNonQueryAsync();
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions());
        await InsertRawAsync(cn, 40, "before", "X");

        var entity = new BtiItem { Id = 40, Name = "after", TenantId = "X" };
        await ctx.BulkUpdateAsync(new[] { entity });

        Assert.Equal("after", await ReadNameAsync(cn, 40));
    }

    // ── BulkDeleteAsync — tenant isolation ──────────────────────────────────

    [Fact]
    public async Task BulkDelete_CrossTenant_DoesNotDeleteForeignTenantRow_SQLite()
    {
        var (cn, ctx) = await MakeCtx("A");
        await InsertRawAsync(cn, 50, "exists", "B"); // belongs to B

        var entity = new BtiItem { Id = 50, TenantId = "A" }; // wrong tenant context
        await ctx.BulkDeleteAsync(new[] { entity });

        Assert.Equal(1, await CountAsync(cn, 50, "B")); // still there
    }

    [Fact]
    public async Task BulkDelete_Batched_CrossTenant_DoesNotDeleteForeignTenantRow_SQLite()
    {
        var (cn, ctx) = await MakeCtx("A", useBatchedBulkOps: true, useFallbackProvider: true);
        await InsertRawAsync(cn, 51, "exists", "B");

        var entity = new BtiItem { Id = 51, TenantId = "A" };
        await ctx.BulkDeleteAsync(new[] { entity });

        Assert.Equal(1, await CountAsync(cn, 51, "B"));
    }

    [Fact]
    public async Task BulkDelete_SameTenant_DeletesOwnRow_SQLite()
    {
        var (cn, ctx) = await MakeCtx("A");
        await InsertRawAsync(cn, 60, "exists", "A");

        var entity = new BtiItem { Id = 60, TenantId = "A" };
        await ctx.BulkDeleteAsync(new[] { entity });

        Assert.Equal(0, await CountAsync(cn, 60, "A")); // deleted
    }

    [Fact]
    public async Task BulkDelete_CrossTenant_MultipleEntities_OnlyOwnRowsDeleted_SQLite()
    {
        var (cn, ctx) = await MakeCtx("A");
        await InsertRawAsync(cn, 70, "own", "A");
        await InsertRawAsync(cn, 71, "foreign", "B");

        var entities = new[]
        {
            new BtiItem { Id = 70, TenantId = "A" }, // own
            new BtiItem { Id = 71, TenantId = "A" }  // cross-tenant attempt
        };
        await ctx.BulkDeleteAsync(entities);

        Assert.Equal(0, await CountAsync(cn, 70, "A")); // deleted
        Assert.Equal(1, await CountAsync(cn, 71, "B")); // intact
    }

    [Fact]
    public async Task BulkDelete_NoTenantProvider_DeletesAllRows_SQLite()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE BtiItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)";
        await setup.ExecuteNonQueryAsync();
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions());
        await InsertRawAsync(cn, 80, "row", "X");

        var entity = new BtiItem { Id = 80, TenantId = "X" };
        await ctx.BulkDeleteAsync(new[] { entity });

        Assert.Equal(0, await CountAsync(cn, 80, "X"));
    }

    // ── Verify SQL shape for MySQL provider ──────────────────────────────────

    [Fact]
    public void MySqlProvider_BulkUpdate_JoinClause_IncludesTenantParam()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());

        var sql = provider.BuildBulkUpdateSql(
            "`BtiItem`",
            "`BulkUpdate_test`",
            "T1.`Name` = T2.`Name`, T1.`TenantId` = T2.`TenantId`",
            "T1.`Id` = T2.`Id`",
            "`TenantId`");

        Assert.Equal("UPDATE `BtiItem` T1 JOIN `BulkUpdate_test` T2 ON T1.`Id` = T2.`Id` SET T1.`Name` = T2.`Name`, T1.`TenantId` = T2.`TenantId` WHERE T1.`TenantId` = @__tenant_bulk", sql);
    }

    [Fact]
    public void PostgresProvider_BulkUpdate_JoinConditions_IncludesTenant()
    {
        var where = PostgresProvider.BuildBulkUpdateWhereClause(
            new[] { "\"Id\"" },
            "\"RowVersion\"",
            "\"TenantId\"",
            "@__tenant_bulk");

        Assert.Equal("t.\"Id\" = v.\"Id\" AND t.\"RowVersion\" = v.\"RowVersion\" AND t.\"TenantId\" = @__tenant_bulk", where);
    }

    [Fact]
    public void SqlServerProvider_BulkUpdate_WhereClause_IncludesTenant()
    {
        var sql = SqlServerProvider.BuildBulkUpdateSql(
            "[BtiItem]",
            "#BulkUpdate_test",
            "T1.[Name] = T2.[Name], T1.[TenantId] = T2.[TenantId]",
            "T1.[Id] = T2.[Id]",
            "[TenantId]");

        Assert.Equal("UPDATE T1 SET T1.[Name] = T2.[Name], T1.[TenantId] = T2.[TenantId] FROM [BtiItem] T1 JOIN #BulkUpdate_test T2 ON T1.[Id] = T2.[Id] WHERE T1.[TenantId] = @__tenant_bulk", sql);
    }

    [Fact]
    public void SqlServerProvider_BulkDelete_WhereClause_IncludesTenant()
    {
        var sql = SqlServerProvider.BuildBulkDeleteSql(
            "[BtiItem]",
            "#BulkDelete_test",
            "T1.[Id] = T2.[Id]",
            "[TenantId]");

        Assert.Equal("DELETE T1 FROM [BtiItem] T1 JOIN #BulkDelete_test T2 ON T1.[Id] = T2.[Id] WHERE T1.[TenantId] = @__tenant_bulk", sql);
    }

    // ── Composite-key entity for cross-tenant tests ──────────────────────────

    [Table("BtiComposite")]
    private class BtiComposite
    {
        [Key, Column(Order = 0)]
        public int RegionId { get; set; }

        [Key, Column(Order = 1)]
        public int ItemSeq { get; set; }

        public string Name { get; set; } = string.Empty;
        public string TenantId { get; set; } = string.Empty;
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> MakeCompositeCtx(string tenantId, bool useBatchedBulkOps = false, bool useFallbackProvider = false)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE BtiComposite (RegionId INTEGER NOT NULL, ItemSeq INTEGER NOT NULL, Name TEXT NOT NULL, TenantId TEXT NOT NULL, PRIMARY KEY (RegionId, ItemSeq))";
        await cmd.ExecuteNonQueryAsync();
        var opts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenantId),
            UseBatchedBulkOps = useBatchedBulkOps,
            OnModelCreating = mb => mb.Entity<BtiComposite>().HasKey(e => new { e.RegionId, e.ItemSeq })
        };
        DatabaseProvider provider = useFallbackProvider ? new FallbackSqliteProvider() : new SqliteProvider();
        return (cn, new DbContext(cn, provider, opts));
    }

    private static async Task InsertCompositeRawAsync(SqliteConnection cn, int regionId, int itemSeq, string name, string tenantId)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO BtiComposite (RegionId, ItemSeq, Name, TenantId) VALUES (@r, @s, @n, @tid)";
        cmd.Parameters.AddWithValue("@r", regionId);
        cmd.Parameters.AddWithValue("@s", itemSeq);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> ReadCompositeNameAsync(SqliteConnection cn, int regionId, int itemSeq)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM BtiComposite WHERE RegionId=@r AND ItemSeq=@s";
        cmd.Parameters.AddWithValue("@r", regionId);
        cmd.Parameters.AddWithValue("@s", itemSeq);
        return (await cmd.ExecuteScalarAsync()) as string;
    }

    private static async Task<long> CountCompositeAsync(SqliteConnection cn, int regionId, int itemSeq, string tenantId)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM BtiComposite WHERE RegionId=@r AND ItemSeq=@s AND TenantId=@tid";
        cmd.Parameters.AddWithValue("@r", regionId);
        cmd.Parameters.AddWithValue("@s", itemSeq);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    // ── Section 9 Req 1: Composite-key BulkUpdateAsync cross-tenant ─────────

    [Fact]
    public async Task CompositeKey_BulkUpdate_CrossTenant_DoesNotModifyForeignRow()
    {
        var (cn, ctx) = await MakeCompositeCtx("A");
        // Row belongs to tenant B
        await InsertCompositeRawAsync(cn, 1, 100, "original", "B");

        // Tenant-A context tries to update tenant-B's composite-key row
        var entity = new BtiComposite { RegionId = 1, ItemSeq = 100, Name = "tampered", TenantId = "A" };
        await ctx.BulkUpdateAsync(new[] { entity });

        // Row must remain unchanged — tenant predicate excludes it
        var name = await ReadCompositeNameAsync(cn, 1, 100);
        Assert.Equal("original", name);
    }

    [Fact]
    public async Task CompositeKey_BulkUpdate_SameTenant_UpdatesOwnRow()
    {
        var (cn, ctx) = await MakeCompositeCtx("A");
        await InsertCompositeRawAsync(cn, 2, 200, "before", "A");

        var entity = new BtiComposite { RegionId = 2, ItemSeq = 200, Name = "after", TenantId = "A" };
        await ctx.BulkUpdateAsync(new[] { entity });

        var name = await ReadCompositeNameAsync(cn, 2, 200);
        Assert.Equal("after", name);
    }

    // ── Section 9 Req 1: Composite-key BulkDeleteAsync cross-tenant ─────────

    [Fact]
    public async Task CompositeKey_BulkDelete_CrossTenant_DoesNotDeleteForeignRow()
    {
        var (cn, ctx) = await MakeCompositeCtx("A");
        // Row belongs to tenant B
        await InsertCompositeRawAsync(cn, 3, 300, "exists", "B");

        // Tenant-A context tries to delete tenant-B's composite-key row
        var entity = new BtiComposite { RegionId = 3, ItemSeq = 300, TenantId = "A" };
        await ctx.BulkDeleteAsync(new[] { entity });

        // Row must still exist — tenant predicate excludes it
        Assert.Equal(1, await CountCompositeAsync(cn, 3, 300, "B"));
    }

    [Fact]
    public async Task CompositeKey_BulkDelete_Batched_CrossTenant_DoesNotDeleteForeignRow()
    {
        var (cn, ctx) = await MakeCompositeCtx("A", useBatchedBulkOps: true, useFallbackProvider: true);
        await InsertCompositeRawAsync(cn, 5, 500, "exists", "B");

        var entity = new BtiComposite { RegionId = 5, ItemSeq = 500, TenantId = "A" };
        await ctx.BulkDeleteAsync(new[] { entity });

        Assert.Equal(1, await CountCompositeAsync(cn, 5, 500, "B"));
    }

    [Fact]
    public async Task CompositeKey_BulkDelete_SameTenant_DeletesOwnRow()
    {
        var (cn, ctx) = await MakeCompositeCtx("A");
        await InsertCompositeRawAsync(cn, 4, 400, "exists", "A");

        var entity = new BtiComposite { RegionId = 4, ItemSeq = 400, TenantId = "A" };
        await ctx.BulkDeleteAsync(new[] { entity });

        Assert.Equal(0, await CountCompositeAsync(cn, 4, 400, "A"));
    }

    // ── Section 9 Req 1: Cached-query before/after BulkUpdate validation ────

    [Fact]
    public async Task CachedQuery_BulkUpdate_ForgedPK_CacheStillReturnsCorrectTenantData()
    {
        // Setup: tenant-A context with a cache provider
        using var cache = new NormMemoryCacheProvider();
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using (var setup = cn.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE BtiItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)";
            await setup.ExecuteNonQueryAsync();
        }
        var optsA = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider("A"),
            CacheProvider = cache
        };
        var ctxA = new DbContext(cn, new SqliteProvider(), optsA);

        // Seed rows: one for tenant A, one for tenant B
        await InsertRawAsync(cn, 100, "tenantA-data", "A");
        await InsertRawAsync(cn, 101, "tenantB-data", "B");

        // Prime cache with tenant-A query
        var primed = await ctxA.Query<BtiItem>()
            .Where(x => x.Id == 100)
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Single(primed);
        Assert.Equal("tenantA-data", primed[0].Name);

        // Attempt cross-tenant BulkUpdate: tenant-A context tries to update tenant-B's row
        var forgedEntity = new BtiItem { Id = 101, Name = "forged-by-A", TenantId = "A" };
        await ctxA.BulkUpdateAsync(new[] { forgedEntity });

        // Verify tenant-B's row is unchanged at DB level
        var bName = await ReadNameAsync(cn, 101);
        Assert.Equal("tenantB-data", bName);

        // Use a FRESH context to verify cache returns correct tenant-A data
        // (avoids change tracker identity map returning tracked entities)
        var freshOpts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider("A"),
            CacheProvider = cache
        };
        var freshCtx = new DbContext(cn, new SqliteProvider(), freshOpts);
        var afterCache = await freshCtx.Query<BtiItem>()
            .Where(x => x.Id == 100)
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Single(afterCache);
        Assert.Equal("tenantA-data", afterCache[0].Name);
    }

    // ── Section 9 Req 1: Mixed batch — own + foreign PKs in single BulkUpdate ─

    [Fact]
    public async Task BulkUpdate_MixedBatch_OwnAndForeignPKs_OnlyOwnRowUpdated()
    {
        var (cn, ctx) = await MakeCtx("A");

        // Seed: 2 rows for tenant A, 2 rows for tenant B
        await InsertRawAsync(cn, 200, "A-row1-before", "A");
        await InsertRawAsync(cn, 201, "A-row2-before", "A");
        await InsertRawAsync(cn, 202, "B-row1-original", "B");
        await InsertRawAsync(cn, 203, "B-row2-original", "B");

        // Batch of 4: 2 own rows + 2 foreign rows with forged PKs
        var batch = new[]
        {
            new BtiItem { Id = 200, Name = "A-row1-after", TenantId = "A" },
            new BtiItem { Id = 202, Name = "B-forged1", TenantId = "A" },    // foreign PK
            new BtiItem { Id = 201, Name = "A-row2-after", TenantId = "A" },
            new BtiItem { Id = 203, Name = "B-forged2", TenantId = "A" },    // foreign PK
        };
        await ctx.BulkUpdateAsync(batch);

        // Tenant-A rows should be updated
        Assert.Equal("A-row1-after", await ReadNameAsync(cn, 200));
        Assert.Equal("A-row2-after", await ReadNameAsync(cn, 201));

        // Tenant-B rows must be untouched — tenant predicate blocks cross-tenant writes
        Assert.Equal("B-row1-original", await ReadNameAsync(cn, 202));
        Assert.Equal("B-row2-original", await ReadNameAsync(cn, 203));
    }
}
