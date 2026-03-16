using System;
using System.Collections.Concurrent;
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
using nORM.Internal;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.5 → 5.0 — Adversarial multi-tenant fuzz + cache poisoning
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Adversarial test suite exercising tenant isolation under hostile conditions:
///   — random tenant IDs (fuzzing with differential oracle)
///   — SQL-injection-style strings as tenant IDs
///   — concurrent queries across multiple tenants (cache poisoning probe)
///   — compiled query plan sharing across tenant contexts
///
/// Each test uses a "differential oracle": the nORM result is compared against
/// a ground-truth direct-ADO.NET query to detect data leakage.
/// </summary>
public class AdversarialTenantFuzzTests
{
    [Table("FuzzRow")]
    private class FuzzRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string TenantId { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    private sealed class StringTenantProvider : ITenantProvider
    {
        private readonly string _tenantId;
        public StringTenantProvider(string tenantId) => _tenantId = tenantId;
        public object GetCurrentTenantId() => _tenantId;
    }

    // ── Database setup ────────────────────────────────────────────────────────

    private static SqliteConnection BuildDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE FuzzRow (
                Id INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT NOT NULL,
                TenantId TEXT NOT NULL,
                Value INTEGER NOT NULL DEFAULT 0
            );";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext BuildTenantContext(SqliteConnection cn, string tenantId)
    {
        // TenantColumnName defaults to "TenantId" which matches FuzzRow.TenantId.
        // ownsConnection:false — the test holds the shared connection; the context must not close it.
        var opts = new DbContextOptions { TenantProvider = new StringTenantProvider(tenantId) };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static async Task<List<string>> DirectQueryNamesAsync(SqliteConnection cn, string tenantId)
    {
        await using DbCommand cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM FuzzRow WHERE TenantId = @tid ORDER BY Id";
        cmd.AddParam("@tid", tenantId);
        await using var reader = await cmd.ExecuteReaderAsync();
        var names = new List<string>();
        while (await reader.ReadAsync()) names.Add(reader.GetString(0));
        return names;
    }

    // ── Differential oracle: random tenant fuzzing ────────────────────────────

    [Fact]
    public async Task TenantFuzz_RandomTenants_DifferentialOracle_NoLeakage()
    {
        using var cn = BuildDb();

        // Seed 5 tenants with 10 rows each.
        var tenantIds = Enumerable.Range(1, 5).Select(i => $"tenant-{i:D3}").ToList();
        foreach (var tenantId in tenantIds)
        {
            using var ctx = BuildTenantContext(cn, tenantId);
            for (int i = 0; i < 10; i++)
                await ctx.InsertAsync(new FuzzRow { Name = $"{tenantId}-row{i}", TenantId = tenantId, Value = i });
        }

        var rng = new Random(42);

        // Run 50 random queries; compare nORM result to ground-truth direct ADO.NET query.
        for (int trial = 0; trial < 50; trial++)
        {
            var targetTenant = tenantIds[rng.Next(tenantIds.Count)];
            using var ctx = BuildTenantContext(cn, targetTenant);

            var normResult = (await ctx.Query<FuzzRow>().ToListAsync())
                .OrderBy(r => r.Name)
                .Select(r => r.Name)
                .ToList();

            var oracleResult = (await DirectQueryNamesAsync(cn, targetTenant))
                .OrderBy(n => n)
                .ToList();

            Assert.True(normResult.SequenceEqual(oracleResult),
                $"Trial {trial}: nORM returned different data than direct SQL for tenant '{targetTenant}'");

            // Verify no data from other tenants leaked.
            foreach (var otherTenant in tenantIds.Where(t => t != targetTenant))
            {
                Assert.DoesNotContain(normResult,
                    name => name.StartsWith(otherTenant));
            }
        }
    }

    // ── Adversarial tenant IDs (SQL-injection-style) ──────────────────────────

    [Theory]
    [InlineData("' OR '1'='1")]
    [InlineData("tenant'; DROP TABLE FuzzRow; --")]
    [InlineData("tenant\0null")]
    [InlineData("ten%ant")]
    [InlineData("ten_ant")]
    [InlineData("UNION SELECT * FROM FuzzRow")]
    public async Task TenantFuzz_AdversarialTenantId_UsedAsParameter_NoInjection(string adversarialTenantId)
    {
        // Adversarial tenant IDs must be treated as data (parameterized), not SQL.
        using var cn = BuildDb();

        // Seed one row for a legitimate tenant.
        using var seedCtx = BuildTenantContext(cn, "safe-tenant");
        await seedCtx.InsertAsync(new FuzzRow { Name = "safe-row", TenantId = "safe-tenant", Value = 1 });

        // Also insert a row for the adversarial tenant ID directly (bypassing nORM) to verify isolation.
        await using DbCommand directCmd = cn.CreateCommand();
        directCmd.CommandText = "INSERT INTO FuzzRow (Name, TenantId, Value) VALUES (@n, @t, @v)";
        directCmd.AddParam("@n", "adversarial-row");
        directCmd.AddParam("@t", adversarialTenantId);
        directCmd.AddParam("@v", 99);
        await directCmd.ExecuteNonQueryAsync();

        // Query using the adversarial tenant ID — must NOT return the safe tenant's rows.
        using var advCtx = BuildTenantContext(cn, adversarialTenantId);
        var result = (await advCtx.Query<FuzzRow>().ToListAsync()).ToList();

        Assert.All(result, r => Assert.Equal(adversarialTenantId, r.TenantId));
        Assert.DoesNotContain(result, r => r.TenantId == "safe-tenant");

        // Table must still exist (injection attempt must have been neutralized).
        await using var probeCmd = cn.CreateCommand();
        probeCmd.CommandText = "SELECT COUNT(*) FROM FuzzRow";
        var count = Convert.ToInt64(await probeCmd.ExecuteScalarAsync());
        Assert.True(count >= 1, "FuzzRow table must still exist — injection must not have dropped it");
    }

    // ── Concurrent multi-tenant cache-poisoning probe ─────────────────────────

    [Fact]
    public async Task TenantFuzz_ConcurrentTenantQueries_NoCachePoisoning()
    {
        using var cn = BuildDb();

        // Seed two tenants.
        for (int t = 1; t <= 2; t++)
        {
            using var ctx = BuildTenantContext(cn, $"t{t}");
            for (int i = 0; i < 5; i++)
                await ctx.InsertAsync(new FuzzRow { Name = $"t{t}-item{i}", TenantId = $"t{t}", Value = t * 10 + i });
        }

        var violations = new ConcurrentBag<string>();

        // Two batches of concurrent tasks, each querying a different tenant.
        var tasks = Enumerable.Range(0, 20).Select(idx => Task.Run(async () =>
        {
            var tenantId = idx % 2 == 0 ? "t1" : "t2";
            using var ctx = BuildTenantContext(cn, tenantId);
            var rows = (await ctx.Query<FuzzRow>().ToListAsync()).ToList();

            // Differential oracle: every returned row must belong to the querying tenant.
            foreach (var row in rows)
            {
                if (row.TenantId != tenantId)
                    violations.Add($"task{idx}: query for '{tenantId}' returned row belonging to '{row.TenantId}'");
            }

            // nORM result must match direct SQL.
            var oracle = await DirectQueryNamesAsync(cn, tenantId);
            var normNames = rows.OrderBy(r => r.Name).Select(r => r.Name).ToList();
            var oracleNames = oracle.OrderBy(n => n).ToList();

            if (!normNames.SequenceEqual(oracleNames))
                violations.Add($"task{idx}: nORM vs oracle divergence for tenant '{tenantId}'");
        })).ToArray();

        await Task.WhenAll(tasks);
        Assert.Empty(violations);
    }

    // ── Compiled query tenant isolation under adversarial reuse ───────────────

    [Fact]
    public async Task CompiledQuery_TwoAdversarialTenants_EachSeesOnlyOwnRows()
    {
        using var cn = BuildDb();

        // Seed two distinct tenants.
        using var seed1 = BuildTenantContext(cn, "attacker");
        using var seed2 = BuildTenantContext(cn, "victim");

        await seed1.InsertAsync(new FuzzRow { Name = "attacker-data", TenantId = "attacker", Value = 1 });
        await seed2.InsertAsync(new FuzzRow { Name = "victim-secret", TenantId = "victim", Value = 999 });

        var compiled = Norm.CompileQuery((DbContext c, int minValue) =>
            c.Query<FuzzRow>().Where(r => r.Value >= minValue));

        // Attacker's context must not see victim's data.
        using var attackerCtx = BuildTenantContext(cn, "attacker");
        var attackerResult = (await compiled(attackerCtx, 0)).ToList();
        Assert.All(attackerResult, r => Assert.Equal("attacker", r.TenantId));
        Assert.DoesNotContain(attackerResult, r => r.Name == "victim-secret");

        // Victim's context must not see attacker's data.
        using var victimCtx = BuildTenantContext(cn, "victim");
        var victimResult = (await compiled(victimCtx, 0)).ToList();
        Assert.All(victimResult, r => Assert.Equal("victim", r.TenantId));
        Assert.DoesNotContain(victimResult, r => r.Name == "attacker-data");
    }

    [Fact]
    public async Task CompiledQuery_CycleContextsTenants_PlanNeverStaleBetweenTenants()
    {
        using var cn = BuildDb();

        // Seed.
        for (int t = 1; t <= 3; t++)
        {
            using var ctx = BuildTenantContext(cn, $"cycletenant{t}");
            for (int i = 0; i < 3; i++)
                await ctx.InsertAsync(new FuzzRow { Name = $"t{t}r{i}", TenantId = $"cycletenant{t}", Value = t });
        }

        var compiled = Norm.CompileQuery((DbContext c, int v) =>
            c.Query<FuzzRow>().Where(r => r.Value == v));

        // Query tenants in a cycle; each must get exactly its own 3 rows.
        for (int round = 0; round < 3; round++)
        {
            for (int t = 1; t <= 3; t++)
            {
                using var ctx = BuildTenantContext(cn, $"cycletenant{t}");
                var result = (await compiled(ctx, t)).ToList();

                Assert.Equal(3, result.Count);
                Assert.All(result, r => Assert.Equal($"cycletenant{t}", r.TenantId));
                Assert.All(result, r => Assert.Equal(t, r.Value));
            }
        }
    }

    // ── Write-path tenant isolation ───────────────────────────────────────────

    [Fact]
    public async Task Insert_TenantContext_RowHasCorrectTenantId()
    {
        using var cn = BuildDb();
        using var ctx = BuildTenantContext(cn, "write-tenant");

        await ctx.InsertAsync(new FuzzRow { Name = "written", TenantId = "write-tenant", Value = 5 });

        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT TenantId FROM FuzzRow WHERE Name = 'written'";
        var tenantId = (string?)await cmd.ExecuteScalarAsync();

        Assert.Equal("write-tenant", tenantId);
    }

    [Fact]
    public async Task SaveChanges_TenantUpdate_OnlyAffectsSameTenantRows()
    {
        using var cn = BuildDb();

        // Seed two tenants.
        using var seed1 = BuildTenantContext(cn, "owner");
        using var seed2 = BuildTenantContext(cn, "other");

        var ownerRow = new FuzzRow { Name = "owner-row", TenantId = "owner", Value = 1 };
        await seed1.InsertAsync(ownerRow);

        var otherRow = new FuzzRow { Name = "other-row", TenantId = "other", Value = 1 };
        await seed2.InsertAsync(otherRow);

        // Update the owner row's value to 42.
        using var ownerCtx = BuildTenantContext(cn, "owner");
        var trackedRows = (await ownerCtx.Query<FuzzRow>().ToListAsync()).ToList();
        Assert.Single(trackedRows);
        trackedRows[0].Value = 42;
        await ownerCtx.SaveChangesAsync();

        // Verify: owner row updated, other tenant row untouched.
        await using var ownerCmd = cn.CreateCommand();
        ownerCmd.CommandText = "SELECT Value FROM FuzzRow WHERE TenantId = 'owner'";
        Assert.Equal(42L, Convert.ToInt64(await ownerCmd.ExecuteScalarAsync()));

        await using var otherCmd = cn.CreateCommand();
        otherCmd.CommandText = "SELECT Value FROM FuzzRow WHERE TenantId = 'other'";
        Assert.Equal(1L, Convert.ToInt64(await otherCmd.ExecuteScalarAsync()));
    }
}
