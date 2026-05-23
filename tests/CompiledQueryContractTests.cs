using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contracts for compiled query isolation and shape parity (blocker 13) that are
/// not already covered by CompiledQuerySqlShapeParityTests or CompiledQueryTenantIsolationTests:
///
/// 1. A compiled query returns the same results as a non-compiled (runtime) query.
/// 2. Two compiled queries for different tenants do not share results.
/// 3. A compiled query with a changed predicate produces a new plan (no plan collision).
/// 4. Calling a compiled query after DbContext disposal throws gracefully.
/// </summary>
public class CompiledQueryContractTests
{
    // ── Domain model ──────────────────────────────────────────────────────────

    [Table("CQC_Item")]
    public class CqcItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int TenantId { get; set; }
        public int Score { get; set; }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static void CreateTable(SqliteConnection cn)
    {
        Exec(cn, "CREATE TABLE CQC_Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId INTEGER NOT NULL, Score INTEGER NOT NULL)");
    }

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly int _tenantId;
        public FixedTenantProvider(int tenantId) => _tenantId = tenantId;
        public object GetCurrentTenantId() => _tenantId;
    }

    // ── 1. Compiled query returns same results as runtime query ───────────────

    [Fact]
    public async Task CompiledQuery_ResultsMatchRuntimeQuery()
    {
        using var cn = OpenDb();
        CreateTable(cn);
        Exec(cn, "INSERT INTO CQC_Item VALUES(1,'Alpha',1,100),(2,'Beta',1,50),(3,'Gamma',1,75)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        // Runtime query
        var runtimeResults = await ctx.Query<CqcItem>()
            .Where(x => x.Score >= 75)
            .OrderBy(x => x.Id)
            .ToListAsync();

        // Compiled query
        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<CqcItem>().Where(x => x.Score >= minScore).OrderBy(x => x.Id));

        var compiledResults = await compiled(ctx, 75);

        Assert.Equal(runtimeResults.Count, compiledResults.Count);
        for (int i = 0; i < runtimeResults.Count; i++)
        {
            Assert.Equal(runtimeResults[i].Id, compiledResults[i].Id);
            Assert.Equal(runtimeResults[i].Name, compiledResults[i].Name);
            Assert.Equal(runtimeResults[i].Score, compiledResults[i].Score);
        }
    }

    // ── 2. Compiled queries for different tenants do not share results ─────────

    [Fact]
    public async Task CompiledQuery_DifferentTenants_IsolatedResults()
    {
        using var cn = OpenDb();
        CreateTable(cn);
        Exec(cn, "INSERT INTO CQC_Item VALUES(1,'T1Item',1,10),(2,'T2Item',2,10)");

        // TenantId column is matched by convention (column name matches TenantColumnName default).
        var opts1 = new DbContextOptions { TenantProvider = new FixedTenantProvider(1) };
        var opts2 = new DbContextOptions { TenantProvider = new FixedTenantProvider(2) };

        using var ctx1 = new DbContext(cn, new SqliteProvider(), opts1);
        using var ctx2 = new DbContext(cn, new SqliteProvider(), opts2);

        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<CqcItem>().Where(x => x.Score >= minScore));

        var tenant1Results = await compiled(ctx1, 0);
        var tenant2Results = await compiled(ctx2, 0);

        Assert.Single(tenant1Results);
        Assert.Equal("T1Item", tenant1Results[0].Name);

        Assert.Single(tenant2Results);
        Assert.Equal("T2Item", tenant2Results[0].Name);
    }

    // ── 3. Changed predicate constant produces correct (non-colliding) results ─

    [Fact]
    public async Task CompiledQuery_ChangedPredicateValue_ProducesCorrectResults()
    {
        using var cn = OpenDb();
        CreateTable(cn);
        Exec(cn, "INSERT INTO CQC_Item VALUES(1,'A',1,10),(2,'B',1,20),(3,'C',1,30)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<CqcItem>().Where(x => x.Score >= minScore).OrderBy(x => x.Id));

        // Different parameter values must produce different result sets.
        var low = await compiled(ctx, 10);
        var mid = await compiled(ctx, 20);
        var high = await compiled(ctx, 30);

        Assert.Equal(3, low.Count);
        Assert.Equal(2, mid.Count);
        Assert.Single(high);
        Assert.Equal("C", high[0].Name);
    }

    // ── 4. Compiled query after DbContext disposal throws gracefully ──────────

    [Fact]
    public async Task CompiledQuery_AfterContextDisposal_ThrowsGracefully()
    {
        var cn = OpenDb();
        CreateTable(cn);
        Exec(cn, "INSERT INTO CQC_Item VALUES(1,'X',1,1)");

        var compiled = Norm.CompileQuery((DbContext c, int minScore) =>
            c.Query<CqcItem>().Where(x => x.Score >= minScore));

        DbContext ctx;
        // Dispose the context while keeping a reference to it.
        using (var disposableCtx = new DbContext(cn, new SqliteProvider()))
        {
            ctx = disposableCtx;
        }
        // ctx and cn are now disposed.
        cn.Dispose();

        // Any exception type is acceptable; what must not happen is silent success
        // returning stale or fabricated data. Typically this throws InvalidOperationException
        // or ObjectDisposedException from the underlying connection.
        await Assert.ThrowsAnyAsync<Exception>(() => compiled(ctx, 0));
    }

    // ── 5. Compiled query across multiple calls on same context is stable ─────

    [Fact]
    public async Task CompiledQuery_MultipleCalls_SameContext_StableResults()
    {
        using var cn = OpenDb();
        CreateTable(cn);
        Exec(cn, "INSERT INTO CQC_Item VALUES(1,'One',1,1),(2,'Two',1,2),(3,'Three',1,3)");

        using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int id) =>
            c.Query<CqcItem>().Where(x => x.Id == id));

        for (int i = 1; i <= 3; i++)
        {
            var result = await compiled(ctx, i);
            Assert.Single(result);
            Assert.Equal(i, result[0].Id);
        }
    }
}
