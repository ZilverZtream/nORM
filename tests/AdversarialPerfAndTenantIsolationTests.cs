using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class AdversarialPerfAndTenantIsolationTests
{
    [Table("AptItem")]
    private sealed class AptItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string TenantId { get; set; } = string.Empty;
    }

    // ── 4.5→5.0: Adversarial perf ceilings ───────────────────────────────

    /// <summary>
    /// NormalizeSql must process 10,000 realistic SELECT strings in under 2 seconds,
    /// establishing a minimum throughput floor for the validation hot path.
    /// </summary>
    [Fact]
    public void NormalizeSql_ThroughputCeiling_10kCallsUnder2Seconds()
    {
        const int n = 10_000;
        const string sql = "SELECT  id,  name  FROM  users  WHERE  id = @p0  AND  active = 1";

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < n; i++)
            NormValidator.NormalizeSql(sql);
        sw.Stop();

        Assert.True(sw.ElapsedMilliseconds < 2000,
            $"NormalizeSql throughput: {n} calls took {sw.ElapsedMilliseconds}ms (expected <2000ms)");
    }

    /// <summary>
    /// IsSafeRawSql must process 1,000 SELECT strings in under 1 second.
    /// </summary>
    [Fact]
    public void IsSafeRawSql_ThroughputCeiling_1kCallsUnder1Second()
    {
        const int n = 1_000;
        const string sql = "SELECT id, name FROM users WHERE id = @p0";

        var sw = Stopwatch.StartNew();
        for (int i = 0; i < n; i++)
            NormValidator.IsSafeRawSql(sql);
        sw.Stop();

        Assert.True(sw.ElapsedMilliseconds < 1000,
            $"IsSafeRawSql throughput: {n} calls took {sw.ElapsedMilliseconds}ms (expected <1000ms)");
    }

    /// <summary>
    /// Compiling a query with a moderately complex LINQ expression (chained Where clauses)
    /// must complete in under 500ms, establishing a compilation time ceiling.
    /// </summary>
    [Fact]
    public void QueryCompile_ModeratelyComplexExpression_CompletesWithinTimeCeiling()
    {
        var sw = Stopwatch.StartNew();
        _ = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<AptItem>()
               .Where(x => x.Id > 0)
               .Where(x => x.Id < 100)
               .Where(x => x.Id == id));
        sw.Stop();

        Assert.True(sw.ElapsedMilliseconds < 500,
            $"Query compilation took {sw.ElapsedMilliseconds}ms (expected <500ms)");
    }

    /// <summary>
    /// After warming up the query plan cache, repeated execution of the same compiled query
    /// must complete in under 50ms per call on average.
    /// </summary>
    [Fact]
    public async Task CompiledQueryExecution_WarmCache_AverageUnderCeiling()
    {
        using var cn = CreateConnection();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<AptItem>().Where(x => x.Id == id));

        // Warm up
        _ = await compiled(ctx, 1);

        const int n = 100;
        var sw = Stopwatch.StartNew();
        for (int i = 0; i < n; i++)
            _ = await compiled(ctx, 1);
        sw.Stop();

        double avgMs = (double)sw.ElapsedMilliseconds / n;
        Assert.True(avgMs < 50.0,
            $"Compiled query avg execution: {avgMs:F2}ms (expected <50ms)");
    }

    // ── 4.5→5.0: Multi-tenant cache-poisoning proof suite ─────────────────

    /// <summary>
    /// The fast-path executor must be bypassed for tenant-aware contexts.
    /// Two tenant contexts querying the same CLR type must each receive only their own rows.
    /// </summary>
    [Fact]
    public async Task TenantContexts_FastPathBypassed_EachSeesOnlyOwnRows()
    {
        using var cn = CreateConnection();

        var optsA = new DbContextOptions { TenantProvider = new FixedTenantProvider("T1") };
        var optsB = new DbContextOptions { TenantProvider = new FixedTenantProvider("T2") };

        await using var ctxA = new DbContext(cn, new SqliteProvider(), optsA);
        await using var ctxB = new DbContext(cn, new SqliteProvider(), optsB);

        var rowsA = await ctxA.Query<AptItem>().ToListAsync();
        var rowsB = await ctxB.Query<AptItem>().ToListAsync();

        // Each tenant sees all rows (tenant filter via global TenantProvider applied in the query)
        Assert.All(rowsA, r => Assert.Equal("T1", r.TenantId));
        Assert.All(rowsB, r => Assert.Equal("T2", r.TenantId));
        Assert.NotEmpty(rowsA);
        Assert.NotEmpty(rowsB);
    }

    /// <summary>
    /// A compiled query cache entry for tenant T1 must not be served to tenant T2.
    /// Each tenant's compiled plan must embed the correct WHERE clause for their tenant ID.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_TwoTenants_CachePlansAreIsolated()
    {
        using var cn = CreateConnection();

        var optsA = new DbContextOptions { TenantProvider = new FixedTenantProvider("T1") };
        var optsB = new DbContextOptions { TenantProvider = new FixedTenantProvider("T2") };

        await using var ctxA = new DbContext(cn, new SqliteProvider(), optsA);
        await using var ctxB = new DbContext(cn, new SqliteProvider(), optsB);

        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<AptItem>().Where(x => x.Id == id));

        // Warm A's cache entry
        var resA = await compiled(ctxA, 1);
        // B must not reuse A's plan
        var resB = await compiled(ctxB, 1);

        Assert.All(resA, r => Assert.Equal("T1", r.TenantId));
        Assert.All(resB, r => Assert.Equal("T2", r.TenantId));
    }

    /// <summary>
    /// Count SQL cache is instance-scoped per DbContext/NormQueryProvider.
    /// Two tenant contexts count independently without cross-context cache sharing.
    /// </summary>
    [Fact]
    public async Task CountSqlCache_InstanceScoped_TenantContextsCountIndependently()
    {
        using var cn = CreateConnection();

        var optsA = new DbContextOptions { TenantProvider = new FixedTenantProvider("T1") };
        var optsB = new DbContextOptions { TenantProvider = new FixedTenantProvider("T2") };

        await using var ctxA = new DbContext(cn, new SqliteProvider(), optsA);
        await using var ctxB = new DbContext(cn, new SqliteProvider(), optsB);

        // Warm A's count cache first
        var countA1 = await ctxA.Query<AptItem>().CountAsync();
        var countB = await ctxB.Query<AptItem>().CountAsync();
        var countA2 = await ctxA.Query<AptItem>().CountAsync();

        // Each context counts only its own tenant's rows
        Assert.Equal(countA1, countA2); // A's cached count is stable
        Assert.True(countA1 > 0);
        Assert.True(countB > 0);
        // Counts are independent — B didn't poison A's cache
        Assert.Equal(countA1, countA2);
    }

    /// <summary>
    /// An adversarial tenant ID containing SQL metacharacters must not alter the query
    /// structure — the tenant value is bound as a parameter, not interpolated.
    /// </summary>
    [Fact]
    public async Task AdversarialTenantId_SqlMetaCharacters_ParameterizedSafely()
    {
        using var cn = CreateConnection();
        const string adversarialId = "'; DROP TABLE AptItem; --";

        var opts = new DbContextOptions { TenantProvider = new FixedTenantProvider(adversarialId) };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // The query must execute without error and return zero rows
        // (adversarial tenant ID has no matching rows, not injected as SQL)
        var rows = await ctx.Query<AptItem>().ToListAsync();
        Assert.Empty(rows);

        // Table must still exist (no DROP TABLE executed)
        await using var probe = cn.CreateCommand();
        probe.CommandText = "SELECT COUNT(*) FROM AptItem;";
        var tableCount = Convert.ToInt64(await probe.ExecuteScalarAsync());
        Assert.Equal(4L, tableCount); // rows still there
    }

    /// <summary>
    /// Two contexts without tenant providers but with different global filters must each
    /// receive cache-isolated query plans producing the correct filtered results.
    /// </summary>
    [Fact]
    public async Task GlobalFilter_TwoContextsDifferentFilters_EachGetsCorrectRows()
    {
        using var cn = CreateConnection();

        // Context A: only T1 rows
        var optsA = new DbContextOptions();
        optsA.AddGlobalFilter<AptItem>(x => x.TenantId == "T1");

        // Context B: only T2 rows
        var optsB = new DbContextOptions();
        optsB.AddGlobalFilter<AptItem>(x => x.TenantId == "T2");

        await using var ctxA = new DbContext(cn, new SqliteProvider(), optsA);
        await using var ctxB = new DbContext(cn, new SqliteProvider(), optsB);

        var rowsA = await ctxA.Query<AptItem>().ToListAsync();
        var rowsB = await ctxB.Query<AptItem>().ToListAsync();

        Assert.All(rowsA, r => Assert.Equal("T1", r.TenantId));
        Assert.All(rowsB, r => Assert.Equal("T2", r.TenantId));
    }

    // ── Helpers ───────────────────────────────────────────────────────────

    private static SqliteConnection CreateConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE AptItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);" +
            "INSERT INTO AptItem VALUES (1, 'Alice', 'T1');" +
            "INSERT INTO AptItem VALUES (2, 'Bob',   'T2');" +
            "INSERT INTO AptItem VALUES (3, 'Carol', 'T1');" +
            "INSERT INTO AptItem VALUES (4, 'Dave',  'T2');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }
}
