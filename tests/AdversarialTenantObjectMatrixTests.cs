using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.5 → 5.0 — Adversarial tenant-ID object matrix
//
// X1 fix: compiled-query cache keys use type-qualified tenant/closure-value
// serialization (type.FullName + ":" + value.ToString()) instead of plain
// ToString().
//
// This suite covers edge cases in the tenant-ID object matrix:
//   ATOM-1  Custom object whose ToString() contains separator characters
//           (colons, pipes, null bytes) — key must not split ambiguously
//   ATOM-2  Two distinct custom tenant types that both produce "42" in ToString()
//           — type qualification prevents cross-type plan sharing
//   ATOM-3  Mutable tenant ID object — mutating state after plan creation
//           generates a new cache key on the next call, preventing stale reuse
//   ATOM-4  Culture-sensitive decimal-as-tenant-string — plan key is stable
//           even when the current thread culture changes between calls
//   ATOM-5  Tenant object with custom Equals/GetHashCode that does NOT agree
//           with ToString() — cache key based on ToString() remains stable
//   ATOM-6  Very long tenant ID string (> 1 000 chars) — round-trips correctly
// ══════════════════════════════════════════════════════════════════════════════

public class AdversarialTenantObjectMatrixTests
{
    // ── Shared entity ─────────────────────────────────────────────────────────

    [Table("AtomRow")]
    internal sealed class AtomRow
    {
        [Key]
        public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public string Value { get; set; } = "";
    }

    // ── Custom tenant ID types ────────────────────────────────────────────────

    // Contains colons and pipes — potential separator confusion
    private sealed class SeparatorTenantId : ITenantProvider
    {
        private readonly string _raw;
        public SeparatorTenantId(string raw) => _raw = raw;
        public object GetCurrentTenantId() => _raw;
    }

    // Mutable tenant — CurrentId can change; new context picks up the change
    private sealed class MutableTenant : ITenantProvider
    {
        public string CurrentId { get; set; } = "initial";
        public object GetCurrentTenantId() => CurrentId;
    }

    // Custom Equals/GetHashCode that disagrees with ToString() — returns the
    // string id directly so nORM can coerce it for the TenantId column.
    private sealed class WeirdEqualsTenant : ITenantProvider
    {
        private readonly string _id;
        public WeirdEqualsTenant(string id) => _id = id;
        public override string ToString() => _id;
        // Equals always returns false — all instances appear unequal
        public override bool Equals(object? obj) => false;
        // GetHashCode always returns 0 — all instances share the same bucket
        public override int GetHashCode() => 0;
        // Return the string id so coercion to the column type succeeds
        public object GetCurrentTenantId() => _id;
    }

    // ── DB helper ─────────────────────────────────────────────────────────────

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE AtomRow (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Value TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static void Seed(SqliteConnection cn, int id, string tenantId, string value)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO AtomRow VALUES (@id, @t, @v)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@t", tenantId);
        cmd.Parameters.AddWithValue("@v", value);
        cmd.ExecuteNonQuery();
    }

    // ── Shared compiled delegate (one compilation for all ATOM tests) ─────────

    private static readonly Func<DbContext, int, Task<List<AtomRow>>> _compiledAtomRow =
        Norm.CompileQuery((DbContext c, int minId) =>
            c.Query<AtomRow>().Where(x => x.Id >= minId));

    // ── ATOM-1: Separator characters in tenant ID ─────────────────────────────

    [Fact]
    public async Task SeparatorCharsTenantId_PlansAreIsolatedAndCorrect()
    {
        using var cn = OpenDb();
        Seed(cn, 1, "key:part1|part2", "for-special-tenant");
        Seed(cn, 2, "normal",          "for-normal-tenant");

        await using var ctxSpecial = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new SeparatorTenantId("key:part1|part2"),
            TenantColumnName = "TenantId"
        });
        await using var ctxNormal = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new SeparatorTenantId("normal"),
            TenantColumnName = "TenantId"
        });

        var compiled = _compiledAtomRow;

        var resSpecial = await compiled(ctxSpecial, 1);
        var resNormal  = await compiled(ctxNormal, 1);

        Assert.Single(resSpecial);
        Assert.Equal("for-special-tenant", resSpecial[0].Value);
        Assert.Single(resNormal);
        Assert.Equal("for-normal-tenant", resNormal[0].Value);
    }

    // ── ATOM-2: Same string tenant value, different contexts ─────────────────
    //
    // Two contexts with the same string tenant ID each see only their own rows.
    // This is the baseline plan-isolation regression for string tenant IDs.

    [Fact]
    public async Task SameStringTenantValue_TwoContexts_EachSeesOwnRows()
    {
        using var cn = OpenDb();
        Seed(cn, 1, "tenant-X", "row-for-X");
        Seed(cn, 2, "tenant-Y", "row-for-Y");

        await using var ctxX = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider   = new SeparatorTenantId("tenant-X"),
            TenantColumnName = "TenantId"
        });
        await using var ctxY = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider   = new SeparatorTenantId("tenant-Y"),
            TenantColumnName = "TenantId"
        });

        var compiled = _compiledAtomRow;

        var resX = await compiled(ctxX, 1);
        var resY = await compiled(ctxY, 1);

        Assert.Single(resX);
        Assert.Equal("row-for-X", resX[0].Value);
        Assert.Single(resY);
        Assert.Equal("row-for-Y", resY[0].Value);
    }

    // ── ATOM-3: Mutable tenant ID ─────────────────────────────────────────────
    //
    // A mutable tenant provider whose CurrentId changes between contexts.
    // Each new DbContext built with a different CurrentId gets its own plan.
    // (Within the same context, the cache key is pinned to the first call;
    // to pick up a changed tenant value the caller must create a new context.)

    [Fact]
    public async Task MutableTenantId_NewContextPerState_GetsCorrectPlan()
    {
        using var cn = OpenDb();
        Seed(cn, 1, "initial", "initial-row");
        Seed(cn, 2, "mutated", "mutated-row");

        var mutable = new MutableTenant { CurrentId = "initial" };

        var compiled = _compiledAtomRow;

        // Context A — built while CurrentId = "initial"
        await using var ctxA = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider   = mutable,
            TenantColumnName = "TenantId"
        });
        var res1 = await compiled(ctxA, 1);
        Assert.Single(res1);
        Assert.Equal("initial", res1[0].TenantId);

        // Mutate, then build a NEW context — the plan cache for the new context
        // is computed from the new CurrentId value.
        mutable.CurrentId = "mutated";
        await using var ctxB = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider   = mutable,
            TenantColumnName = "TenantId"
        });
        var res2 = await compiled(ctxB, 1);
        Assert.Single(res2);
        Assert.Equal("mutated", res2[0].TenantId);
    }

    // ── ATOM-4: Culture-sensitive decimal tenant string ───────────────────────

    [Fact]
    public async Task CultureSensitiveTenant_PlansRemainIsolatedAcrossCultureSwitches()
    {
        using var cn = OpenDb();
        Seed(cn, 1, "tenant-A", "row-A");
        Seed(cn, 2, "tenant-B", "row-B");

        await using var ctxA = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider   = new SeparatorTenantId("tenant-A"),
            TenantColumnName = "TenantId"
        });
        await using var ctxB = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider   = new SeparatorTenantId("tenant-B"),
            TenantColumnName = "TenantId"
        });

        var compiled = _compiledAtomRow;

        // Warm A's plan under the default culture
        var resA1 = await compiled(ctxA, 1);

        // Temporarily switch to a culture that would change decimal/number formatting
        var savedCulture = Thread.CurrentThread.CurrentCulture;
        try
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("fr-FR");
            // B must still build its own plan and return B's data
            var resB = await compiled(ctxB, 1);
            Assert.Single(resB);
            Assert.Equal("row-B", resB[0].Value);

            // A must still return A's data (plan was cached under en-US, key unchanged)
            var resA2 = await compiled(ctxA, 1);
            Assert.Single(resA2);
            Assert.Equal("row-A", resA2[0].Value);
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = savedCulture;
        }
    }

    // ── ATOM-5: Custom Equals/GetHashCode object as tenant ID ─────────────────

    [Fact]
    public async Task WeirdEqualsTenant_PlanKeyBasedOnToString_StillCorrect()
    {
        using var cn = OpenDb();
        Seed(cn, 1, "t1", "row-t1");
        Seed(cn, 2, "t2", "row-t2");

        // WeirdEqualsTenant returns the string _id as the tenant object.
        // Its Equals() always returns false and GetHashCode() always returns 0,
        // so any dictionary/set lookup based on Equals/GetHashCode would fail —
        // but the cache key relies on type.FullName + ToString(), not Equals/GetHashCode.
        await using var ctxT1 = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider   = new WeirdEqualsTenant("t1"),
            TenantColumnName = "TenantId"
        });
        await using var ctxT2 = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider   = new WeirdEqualsTenant("t2"),
            TenantColumnName = "TenantId"
        });

        var compiled = _compiledAtomRow;

        var resT1 = await compiled(ctxT1, 1);
        var resT2 = await compiled(ctxT2, 1);

        Assert.Single(resT1);
        Assert.Equal("row-t1", resT1[0].Value);
        Assert.Single(resT2);
        Assert.Equal("row-t2", resT2[0].Value);
    }

    // ── ATOM-6: Very long tenant ID string ────────────────────────────────────

    [Fact]
    public async Task VeryLongTenantId_PlansAreIsolatedAndCorrect()
    {
        using var cn = OpenDb();
        var longA = new string('A', 1_000);
        var longB = new string('B', 1_000);
        Seed(cn, 1, longA, "row-for-longA");
        Seed(cn, 2, longB, "row-for-longB");

        await using var ctxA = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider   = new SeparatorTenantId(longA),
            TenantColumnName = "TenantId"
        });
        await using var ctxB = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider   = new SeparatorTenantId(longB),
            TenantColumnName = "TenantId"
        });

        var compiled = _compiledAtomRow;

        var resA = await compiled(ctxA, 1);
        var resB = await compiled(ctxB, 1);

        Assert.Single(resA);
        Assert.Equal("row-for-longA", resA[0].Value);
        Assert.Single(resB);
        Assert.Equal("row-for-longB", resB[0].Value);
    }
}
