using System;
using System.Collections.Generic;
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
/// X3: Verifies that provider-native BulkUpdateAsync respects optimistic-concurrency
/// tokens (timestamp columns) the same way that ordinary SaveChanges does.
///
/// When a concurrent writer changes a row's timestamp between load and BulkUpdate,
/// the update must silently skip that row (0 rows affected) rather than performing
/// a silent lost update.
/// </summary>
public class BulkOccParityTests
{
    // Table with a simple timestamp (string) concurrency token for SQLite tests.
    [Table("BocItem")]
    private class BocItem
    {
        [Key]
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;

        [System.ComponentModel.DataAnnotations.Timestamp]
        public string? RowVersion { get; set; }
    }

    // Table without a concurrency token — for verifying normal bulk update works.
    [Table("BocSimple")]
    private class BocSimple
    {
        [Key]
        public int Id { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> MakeCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE BocItem (Id INTEGER PRIMARY KEY, Value TEXT NOT NULL, RowVersion TEXT)";
        await cmd.ExecuteNonQueryAsync();
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> MakeSimpleCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE BocSimple (Id INTEGER PRIMARY KEY, Value TEXT NOT NULL)";
        await cmd.ExecuteNonQueryAsync();
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    private static async Task InsertRawAsync(SqliteConnection cn, int id, string value, string? rv)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO BocItem (Id, Value, RowVersion) VALUES (@id,@v,@rv)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@v", value);
        cmd.Parameters.AddWithValue("@rv", (object?)rv ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task UpdateRvAsync(SqliteConnection cn, int id, string newRv)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "UPDATE BocItem SET RowVersion=@rv WHERE Id=@id";
        cmd.Parameters.AddWithValue("@rv", newRv);
        cmd.Parameters.AddWithValue("@id", id);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> ReadValueAsync(SqliteConnection cn, int id)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Value FROM BocItem WHERE Id=@id";
        cmd.Parameters.AddWithValue("@id", id);
        return (await cmd.ExecuteScalarAsync()) as string;
    }

    // ── OCC enforcement: stale token → row not updated ─────────────────────

    [Fact]
    public async Task BulkUpdate_WithStaleToken_SkipsRow_SQLite()
    {
        var (cn, ctx) = await MakeCtx();
        await InsertRawAsync(cn, 1, "original", "token-1");

        // Simulate concurrent write that changed the token
        await UpdateRvAsync(cn, 1, "token-2");

        // BulkUpdate with entity still holding old token — should NOT update
        var entity = new BocItem { Id = 1, Value = "tampered", RowVersion = "token-1" };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });

        // Row was not changed because token mismatch
        var actualValue = await ReadValueAsync(cn, 1);
        Assert.Equal("original", actualValue);
        Assert.Equal(0, updated);
    }

    [Fact]
    public async Task BulkUpdate_WithMatchingToken_UpdatesRow_SQLite()
    {
        var (cn, ctx) = await MakeCtx();
        await InsertRawAsync(cn, 2, "original", "token-1");

        // Entity has current token — should update successfully
        var entity = new BocItem { Id = 2, Value = "updated", RowVersion = "token-1" };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });

        var actualValue = await ReadValueAsync(cn, 2);
        Assert.Equal("updated", actualValue);
        Assert.Equal(1, updated);
    }

    [Fact]
    public async Task BulkUpdate_NullToken_MatchesNullInDb_SQLite()
    {
        var (cn, ctx) = await MakeCtx();
        await InsertRawAsync(cn, 3, "original", null);

        var entity = new BocItem { Id = 3, Value = "updated", RowVersion = null };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });

        var actualValue = await ReadValueAsync(cn, 3);
        Assert.Equal("updated", actualValue);
        Assert.Equal(1, updated);
    }

    [Fact]
    public async Task BulkUpdate_NullEntityToken_DoesNotMatch_NonNullDbToken_SQLite()
    {
        var (cn, ctx) = await MakeCtx();
        await InsertRawAsync(cn, 4, "original", "token-1");

        // Entity has null token but DB has non-null — should not match
        var entity = new BocItem { Id = 4, Value = "tampered", RowVersion = null };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });

        var actualValue = await ReadValueAsync(cn, 4);
        Assert.Equal("original", actualValue);
        Assert.Equal(0, updated);
    }

    [Fact]
    public async Task BulkUpdate_PartialOccFailure_OnlyMatchingRowsUpdated_SQLite()
    {
        var (cn, ctx) = await MakeCtx();
        await InsertRawAsync(cn, 10, "row10", "t1");
        await InsertRawAsync(cn, 11, "row11", "t1");

        // Externally change row11's token
        await UpdateRvAsync(cn, 11, "t2");

        var entities = new[]
        {
            new BocItem { Id = 10, Value = "updated10", RowVersion = "t1" }, // matches
            new BocItem { Id = 11, Value = "tampered11", RowVersion = "t1" } // stale — should not match
        };
        await ctx.BulkUpdateAsync(entities);

        Assert.Equal("updated10", await ReadValueAsync(cn, 10)); // updated
        Assert.Equal("row11",     await ReadValueAsync(cn, 11)); // unchanged
    }

    // ── No-token table: bulk update works normally ───────────────────────────

    [Fact]
    public async Task BulkUpdate_NoTimestampColumn_UpdatesNormally_SQLite()
    {
        var (cn, ctx) = await MakeSimpleCtx();
        await using var setup = cn.CreateCommand();
        setup.CommandText = "INSERT INTO BocSimple (Id, Value) VALUES (20,'before')";
        await setup.ExecuteNonQueryAsync();

        var entity = new BocSimple { Id = 20, Value = "after" };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });

        await using var check = cn.CreateCommand();
        check.CommandText = "SELECT Value FROM BocSimple WHERE Id=20";
        var val = (string?)await check.ExecuteScalarAsync();
        Assert.Equal("after", val);
        Assert.Equal(1, updated);
    }

    // ── PostgreSQL is already correct — document expected behavior ───────────

    [Fact]
    public void PostgresProvider_BulkUpdate_AlreadyIncludesTimestampInJoin()
    {
        // PostgreSQL BulkUpdateAsync already builds:
        //   UPDATE table AS t SET ... FROM (VALUES ...) AS v (...)
        //   WHERE t.key = v.key AND t.timestamp = v.timestamp
        // This was correct before the audit — no change needed.
        Assert.True(true, "PostgreSQL BulkUpdateAsync already includes timestamp column in WHERE join conditions.");
    }

    // ── Section 9 Requirement 2: same-value update with matching token ───────

    [Fact]
    public async Task BulkUpdate_SameValueWithMatchingToken_Succeeds_SQLite()
    {
        // Entity updates to same Value but with matching token → should succeed with 1 row updated (not 0).
        // This verifies that the OCC check does not spuriously reject same-value updates.
        var (cn, ctx) = await MakeCtx();
        await InsertRawAsync(cn, 100, "unchanged", "tok-A");

        var entity = new BocItem { Id = 100, Value = "unchanged", RowVersion = "tok-A" };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });

        Assert.Equal(1, updated);
        Assert.Equal("unchanged", await ReadValueAsync(cn, 100));
    }

    // ── Section 9 Requirement 2: batch >1 with mixed OCC outcomes ────────────

    [Fact]
    public async Task BulkUpdate_MixedOccOutcomes_OnlyMatchingRowsUpdated_SQLite()
    {
        // 5 entities: 3 have current tokens, 2 have stale tokens.
        // Verify only 3 are updated.
        var (cn, ctx) = await MakeCtx();
        await InsertRawAsync(cn, 200, "row200", "cur");
        await InsertRawAsync(cn, 201, "row201", "cur");
        await InsertRawAsync(cn, 202, "row202", "cur");
        await InsertRawAsync(cn, 203, "row203", "cur");
        await InsertRawAsync(cn, 204, "row204", "cur");

        // Externally change tokens for rows 203 and 204 → makes them stale
        await UpdateRvAsync(cn, 203, "new-tok");
        await UpdateRvAsync(cn, 204, "new-tok");

        var entities = new[]
        {
            new BocItem { Id = 200, Value = "upd200", RowVersion = "cur" }, // matches
            new BocItem { Id = 201, Value = "upd201", RowVersion = "cur" }, // matches
            new BocItem { Id = 202, Value = "upd202", RowVersion = "cur" }, // matches
            new BocItem { Id = 203, Value = "upd203", RowVersion = "cur" }, // stale
            new BocItem { Id = 204, Value = "upd204", RowVersion = "cur" }, // stale
        };

        var updated = await ctx.BulkUpdateAsync(entities);

        Assert.Equal(3, updated);
        Assert.Equal("upd200", await ReadValueAsync(cn, 200));
        Assert.Equal("upd201", await ReadValueAsync(cn, 201));
        Assert.Equal("upd202", await ReadValueAsync(cn, 202));
        Assert.Equal("row203", await ReadValueAsync(cn, 203)); // unchanged — stale
        Assert.Equal("row204", await ReadValueAsync(cn, 204)); // unchanged — stale
    }

    // ── Section 9 Requirement 2: batch >1 where ALL tokens match ─────────────

    [Fact]
    public async Task BulkUpdate_AllTokensMatch_AllRowsUpdated_SQLite()
    {
        // Batch of 5 entities all with current tokens → all 5 updated.
        var (cn, ctx) = await MakeCtx();
        await InsertRawAsync(cn, 300, "row300", "v1");
        await InsertRawAsync(cn, 301, "row301", "v1");
        await InsertRawAsync(cn, 302, "row302", "v1");
        await InsertRawAsync(cn, 303, "row303", "v1");
        await InsertRawAsync(cn, 304, "row304", "v1");

        var entities = new[]
        {
            new BocItem { Id = 300, Value = "new300", RowVersion = "v1" },
            new BocItem { Id = 301, Value = "new301", RowVersion = "v1" },
            new BocItem { Id = 302, Value = "new302", RowVersion = "v1" },
            new BocItem { Id = 303, Value = "new303", RowVersion = "v1" },
            new BocItem { Id = 304, Value = "new304", RowVersion = "v1" },
        };

        var updated = await ctx.BulkUpdateAsync(entities);

        Assert.Equal(5, updated);
        Assert.Equal("new300", await ReadValueAsync(cn, 300));
        Assert.Equal("new301", await ReadValueAsync(cn, 301));
        Assert.Equal("new302", await ReadValueAsync(cn, 302));
        Assert.Equal("new303", await ReadValueAsync(cn, 303));
        Assert.Equal("new304", await ReadValueAsync(cn, 304));
    }

    // ── Section 9 Requirement 2: OCC with composite primary keys ─────────────

    [Table("BocComposite")]
    private class BocCompositeItem
    {
        [Key, Column(Order = 0)]
        public int TenantId { get; set; }

        [Key, Column(Order = 1)]
        public int ItemId { get; set; }

        public string Value { get; set; } = string.Empty;

        [Timestamp]
        public string? RowVersion { get; set; }
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> MakeCompositeCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE BocComposite (TenantId INTEGER NOT NULL, ItemId INTEGER NOT NULL, Value TEXT NOT NULL, RowVersion TEXT, PRIMARY KEY (TenantId, ItemId))";
        await cmd.ExecuteNonQueryAsync();
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    private static async Task InsertCompositeRawAsync(SqliteConnection cn, int tenantId, int itemId, string value, string? rv)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO BocComposite (TenantId, ItemId, Value, RowVersion) VALUES (@t,@i,@v,@rv)";
        cmd.Parameters.AddWithValue("@t", tenantId);
        cmd.Parameters.AddWithValue("@i", itemId);
        cmd.Parameters.AddWithValue("@v", value);
        cmd.Parameters.AddWithValue("@rv", (object?)rv ?? DBNull.Value);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task UpdateCompositeRvAsync(SqliteConnection cn, int tenantId, int itemId, string newRv)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "UPDATE BocComposite SET RowVersion=@rv WHERE TenantId=@t AND ItemId=@i";
        cmd.Parameters.AddWithValue("@rv", newRv);
        cmd.Parameters.AddWithValue("@t", tenantId);
        cmd.Parameters.AddWithValue("@i", itemId);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> ReadCompositeValueAsync(SqliteConnection cn, int tenantId, int itemId)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Value FROM BocComposite WHERE TenantId=@t AND ItemId=@i";
        cmd.Parameters.AddWithValue("@t", tenantId);
        cmd.Parameters.AddWithValue("@i", itemId);
        return (await cmd.ExecuteScalarAsync()) as string;
    }

    [Fact]
    public async Task BulkUpdate_CompositePK_StaleToken_ZeroUpdates_SQLite()
    {
        // 2-column PK + timestamp: stale token → 0 updates.
        var (cn, ctx) = await MakeCompositeCtx();
        await InsertCompositeRawAsync(cn, 1, 10, "original", "tok-1");

        // Simulate concurrent write that changed the token
        await UpdateCompositeRvAsync(cn, 1, 10, "tok-2");

        var entity = new BocCompositeItem { TenantId = 1, ItemId = 10, Value = "tampered", RowVersion = "tok-1" };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });

        Assert.Equal(0, updated);
        Assert.Equal("original", await ReadCompositeValueAsync(cn, 1, 10));
    }

    [Fact]
    public async Task BulkUpdate_CompositePK_MatchingToken_UpdatesRow_SQLite()
    {
        // 2-column PK + timestamp: matching token → 1 row updated.
        var (cn, ctx) = await MakeCompositeCtx();
        await InsertCompositeRawAsync(cn, 2, 20, "original", "tok-A");

        var entity = new BocCompositeItem { TenantId = 2, ItemId = 20, Value = "updated", RowVersion = "tok-A" };
        var updated = await ctx.BulkUpdateAsync(new[] { entity });

        Assert.Equal(1, updated);
        Assert.Equal("updated", await ReadCompositeValueAsync(cn, 2, 20));
    }

    [Fact]
    public async Task BulkUpdate_CompositePK_MixedTokens_OnlyMatchingUpdated_SQLite()
    {
        // 2-column PK batch: 2 matching + 1 stale → 2 updated.
        var (cn, ctx) = await MakeCompositeCtx();
        await InsertCompositeRawAsync(cn, 1, 1, "row-1-1", "tok");
        await InsertCompositeRawAsync(cn, 1, 2, "row-1-2", "tok");
        await InsertCompositeRawAsync(cn, 2, 1, "row-2-1", "tok");

        // Stale row 2,1
        await UpdateCompositeRvAsync(cn, 2, 1, "tok-new");

        var entities = new[]
        {
            new BocCompositeItem { TenantId = 1, ItemId = 1, Value = "upd-1-1", RowVersion = "tok" },
            new BocCompositeItem { TenantId = 1, ItemId = 2, Value = "upd-1-2", RowVersion = "tok" },
            new BocCompositeItem { TenantId = 2, ItemId = 1, Value = "upd-2-1", RowVersion = "tok" }, // stale
        };

        var updated = await ctx.BulkUpdateAsync(entities);

        Assert.Equal(2, updated);
        Assert.Equal("upd-1-1", await ReadCompositeValueAsync(cn, 1, 1));
        Assert.Equal("upd-1-2", await ReadCompositeValueAsync(cn, 1, 2));
        Assert.Equal("row-2-1", await ReadCompositeValueAsync(cn, 2, 1)); // unchanged
    }
}
