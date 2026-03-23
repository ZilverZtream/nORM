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

#nullable enable

namespace nORM.Tests;

// SP1: VerifySingleUpdateOccAsync omitted tenant predicate, causing a tenant-blocked direct UPDATE
// (0 rows because the tenant WHERE clause filtered it out) to be misclassified as a same-value
// update (because the verifier found a matching foreign-tenant row).

public class DirectUpdateOccTenantVerifierTests
{
    [Table("Sp1Entity")]
    private sealed class Sp1Entity
    {
        [Key]
        public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public string Name { get; set; } = "";

        [Timestamp]
        public byte[] Token { get; set; } = Array.Empty<byte>();
    }

    // Simulate MySQL-style affected-row semantics using SQLite
    private sealed class AffectedRowsSqliteProvider : SqliteProvider
    {
        internal override bool UseAffectedRowsSemantics => true;
    }

    private sealed class FixedTenant : ITenantProvider
    {
        public FixedTenant(string id) => Id = id;
        public string Id { get; }
        public object GetCurrentTenantId() => Id;
    }

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE Sp1Entity (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, " +
            "Name TEXT NOT NULL, Token BLOB NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static void SeedRaw(SqliteConnection cn, int id, string tenantId, string name, byte[] token)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO Sp1Entity (Id, TenantId, Name, Token) VALUES (@id, @tid, @name, @tok)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@tid", tenantId);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@tok", token);
        cmd.ExecuteNonQuery();
    }

    // ── SP1-1: Cross-tenant verifier throws DbConcurrencyException ────────

    /// <summary>
    /// SP1: Without the fix, VerifySingleUpdateOccAsync had no tenant predicate.
    /// When TenantA's UPDATE returned 0 rows (tenant-blocked because TenantB holds PK+token),
    /// the verifier SELECT COUNT(*) without tenant would find TenantB's row (count=1)
    /// and return without throwing — treating it as a same-value update.
    /// With the fix, the verifier scopes its SELECT to TenantA → count=0 → throws.
    /// </summary>
    [Fact]
    public async Task TenantBlockedUpdate_OccVerifier_ThrowsConcurrencyException()
    {
        using var cn = OpenDb();

        // Seed TenantB row with PK=1, token=[0x42]
        SeedRaw(cn, 1, "TenantB", "bob", new byte[] { 0x42 });

        // TenantA context — UseAffectedRowsSemantics=true (like MySQL)
        await using var ctx = new DbContext(cn, new AffectedRowsSqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenant("TenantA"),
            TenantColumnName = "TenantId"
        });

        // TenantA entity with PK=1 and same token — but TenantA has no such row in DB.
        // The UPDATE WHERE (PK=1 AND Token=[0x42] AND TenantId='TenantA') finds nothing → 0 rows.
        // VerifySingleUpdateOccAsync is triggered.
        // Without fix: SELECT COUNT(*) WHERE PK=1 AND Token=[0x42] → TenantB row found (count=1)
        //              → misclassified as same-value update → no exception. BUG.
        // With fix: SELECT COUNT(*) WHERE PK=1 AND Token=[0x42] AND TenantId='TenantA'
        //           → nothing found (count=0) → correctly throws.
        var entity = new Sp1Entity { Id = 1, TenantId = "TenantA", Name = "alice", Token = new byte[] { 0x42 } };
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.UpdateAsync(entity));
    }

    // ── SP1-2: Same-tenant same-value update must not throw ───────────────

    /// <summary>
    /// Regression: with the tenant predicate in the verifier, a genuine same-value update
    /// (entity exists for TenantA but no bytes changed) must still NOT throw.
    /// </summary>
    [Fact]
    public async Task SameTenantSameValueUpdate_OccVerifier_DoesNotThrow()
    {
        using var cn = OpenDb();
        SeedRaw(cn, 2, "TenantA", "alice", new byte[] { 0x55 });

        await using var ctx = new DbContext(cn, new AffectedRowsSqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenant("TenantA"),
            TenantColumnName = "TenantId"
        });

        // Update with the SAME values as in DB — affected-rows provider returns 0.
        // VerifySingleUpdateOccAsync is triggered.
        // With tenant predicate: SELECT WHERE PK=2 AND Token=[0x55] AND TenantId='TenantA' → count=1
        // → correctly identified as same-value update → no exception.
        var entity = new Sp1Entity { Id = 2, TenantId = "TenantA", Name = "alice", Token = new byte[] { 0x55 } };
        var rows = await ctx.UpdateAsync(entity); // must not throw
        // SQLite reports 1 affected row even for same-value updates (no MySQL 0-for-unchanged).
        // The key assertion is that no DbConcurrencyException is thrown.
        Assert.True(rows >= 0);
    }

    // ── SP1-3: Same-tenant stale token must throw DbConcurrencyException ──

    [Fact]
    public async Task SameTenantStaleToken_OccVerifier_ThrowsConcurrencyException()
    {
        using var cn = OpenDb();
        SeedRaw(cn, 3, "TenantA", "alice", new byte[] { 0xAA });

        await using var ctx = new DbContext(cn, new AffectedRowsSqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenant("TenantA"),
            TenantColumnName = "TenantId"
        });

        // Entity with stale token [0xBB] — token in DB is [0xAA].
        // UPDATE WHERE PK=3 AND Token=[0xBB] AND TenantId='TenantA' → no match → 0 rows.
        // VerifySingleUpdateOccAsync: SELECT WHERE PK=3 AND Token=[0xBB] AND TenantId='TenantA' → count=0
        // → correctly throws (genuine OCC conflict).
        var entity = new Sp1Entity { Id = 3, TenantId = "TenantA", Name = "alice_new", Token = new byte[] { 0xBB } };
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.UpdateAsync(entity));
    }

    // ── SP1-4: No tenant config — verifier works as before ────────────────

    [Fact]
    public async Task NoTenantConfig_StaleToken_OccVerifier_ThrowsConcurrencyException()
    {
        using var cn = OpenDb();
        SeedRaw(cn, 4, "none", "data", new byte[] { 0x10 });

        await using var ctx = new DbContext(cn, new AffectedRowsSqliteProvider());

        var entity = new Sp1Entity { Id = 4, TenantId = "none", Name = "data_new", Token = new byte[] { 0xFF } };
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.UpdateAsync(entity));
    }
}
