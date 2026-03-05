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

namespace nORM.Tests;

/// <summary>
/// QP-1/PC-1/SEC-1: Compiled query plans must NOT be reused across contexts that have
/// different tenant IDs or global filters, as those produce different SQL WHERE clauses.
/// </summary>
public class CompiledQueryTenantIsolationTests
{
    [Table("CqtiRow")]
    private class CqtiRow
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int TenantKey { get; set; }
    }

    [Table("CqtiSoft")]
    private class CqtiSoft
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsActive { get; set; }
    }

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    /// <summary>
    /// QP-1/SEC-1: Two contexts sharing the same connection but with different global filters
    /// (simulating per-tenant row visibility via integer tenant key) must each receive their
    /// own compiled plan so the WHERE clause is correct for each context.
    ///
    /// Without the ctxKey fix, both contexts would share the same plansByCtx entry (same
    /// provider+mapping hash), and the second context would reuse the first context's plan —
    /// executing with the wrong filter and returning the wrong rows.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_TwoTenants_EachSeesOnlyOwnRows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE CqtiRow (Id INTEGER PRIMARY KEY, Name TEXT, TenantKey INTEGER);" +
            "INSERT INTO CqtiRow VALUES (1,'Alpha',1);" +
            "INSERT INTO CqtiRow VALUES (2,'Beta',2);";
        setup.ExecuteNonQuery();

        // Use global filters to simulate per-tenant visibility (integer equality is used because
        // compiled-query constant parameterization works reliably for integer literals).
        var opts1 = new DbContextOptions();
        opts1.AddGlobalFilter<CqtiRow>(e => e.TenantKey == 1);
        using var ctx1 = new DbContext(cn, new SqliteProvider(), opts1);

        var opts2 = new DbContextOptions();
        opts2.AddGlobalFilter<CqtiRow>(e => e.TenantKey == 2);
        using var ctx2 = new DbContext(cn, new SqliteProvider(), opts2);

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<CqtiRow>().Where(x => x.Id >= minId));

        var r1 = await compiled(ctx1, 1);
        var r2 = await compiled(ctx2, 1);

        // Each context must only see rows belonging to its own tenant scope.
        Assert.All(r1, item => Assert.Equal(1, item.TenantKey));
        Assert.All(r2, item => Assert.Equal(2, item.TenantKey));

        Assert.Single(r1);
        Assert.Single(r2);
        Assert.Equal(1, r1[0].Id);
        Assert.Equal(2, r2[0].Id);
    }

    /// <summary>
    /// QP-1/SEC-1: After the plan for tenant-1 is cached, a subsequent call using tenant-2
    /// must NOT return tenant-1 rows — verifies the ctxKey separates plan entries per filter.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_TenantSwitch_DoesNotLeakRows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE CqtiRow (Id INTEGER PRIMARY KEY, Name TEXT, TenantKey INTEGER);" +
            "INSERT INTO CqtiRow VALUES (1,'Alpha',1);" +
            "INSERT INTO CqtiRow VALUES (2,'Beta',1);" +
            "INSERT INTO CqtiRow VALUES (3,'Gamma',2);";
        setup.ExecuteNonQuery();

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<CqtiRow>().Where(x => x.Id >= minId));

        // First call: ctx1 (tenant-1 filter) — caches plan for tenant-1
        var opts1 = new DbContextOptions();
        opts1.AddGlobalFilter<CqtiRow>(e => e.TenantKey == 1);
        using var ctx1 = new DbContext(cn, new SqliteProvider(), opts1);
        var r1 = await compiled(ctx1, 1);

        // Second call: ctx2 (tenant-2 filter) — must NOT reuse tenant-1's plan
        var opts2 = new DbContextOptions();
        opts2.AddGlobalFilter<CqtiRow>(e => e.TenantKey == 2);
        using var ctx2 = new DbContext(cn, new SqliteProvider(), opts2);
        var r2 = await compiled(ctx2, 1);

        // Tenant-1 has 2 rows; tenant-2 has 1 row.
        Assert.Equal(2, r1.Count);
        Assert.All(r1, item => Assert.Equal(1, item.TenantKey));

        Assert.Single(r2);
        Assert.All(r2, item => Assert.Equal(2, item.TenantKey));
    }

    /// <summary>
    /// QP-1/SEC-1: Two contexts with the same provider+mapping but different global filters
    /// must each get their own plan so the filter WHERE clause is applied correctly.
    /// This verifies the ctxKey includes the global-filter hash dimension.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_GlobalFilter_DifferentContexts_AppliesCorrectFilter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE CqtiSoft (Id INTEGER PRIMARY KEY, Name TEXT, IsActive INTEGER);" +
            "INSERT INTO CqtiSoft VALUES (1,'Active',1);" +
            "INSERT INTO CqtiSoft VALUES (2,'Inactive',0);";
        setup.ExecuteNonQuery();

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<CqtiSoft>().Where(x => x.Id >= minId));

        // Context 1: only active records
        var opts1 = new DbContextOptions();
        opts1.AddGlobalFilter<CqtiSoft>(e => e.IsActive);
        using var ctx1 = new DbContext(cn, new SqliteProvider(), opts1);

        // Context 2: only inactive records
        var opts2 = new DbContextOptions();
        opts2.AddGlobalFilter<CqtiSoft>(e => !e.IsActive);
        using var ctx2 = new DbContext(cn, new SqliteProvider(), opts2);

        var r1 = await compiled(ctx1, 1);
        var r2 = await compiled(ctx2, 1);

        Assert.Single(r1);
        Assert.True(r1[0].IsActive);

        Assert.Single(r2);
        Assert.False(r2[0].IsActive);
    }

    /// <summary>
    /// QP-1/SEC-1: Smoke test — a context with TenantProvider set must not throw when using
    /// a compiled query. Verifies the ctxKey computation handles TenantProvider gracefully.
    /// </summary>
    [Fact]
    public async Task CompiledQuery_WithTenantProvider_DoesNotThrow()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText =
            "CREATE TABLE CqtiRow (Id INTEGER PRIMARY KEY, Name TEXT, TenantKey INTEGER);" +
            "INSERT INTO CqtiRow VALUES (1,'Alpha',1);" +
            "INSERT INTO CqtiRow VALUES (2,'Beta',2);";
        setup.ExecuteNonQuery();

        // TenantKey is an int column; use a string tenant provider to test type coercion path
        var opts1 = new DbContextOptions { TenantProvider = new FixedTenantProvider("T1") };
        using var ctx1 = new DbContext(cn, new SqliteProvider(), opts1);

        var opts2 = new DbContextOptions { TenantProvider = new FixedTenantProvider("T2") };
        using var ctx2 = new DbContext(cn, new SqliteProvider(), opts2);

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<CqtiRow>().Where(x => x.Id >= minId));

        // Both contexts must execute without exceptions.
        // The ctxKey now includes tenantId hash, so each gets its own plan entry.
        var ex1 = await Record.ExceptionAsync(() => compiled(ctx1, 1));
        var ex2 = await Record.ExceptionAsync(() => compiled(ctx2, 1));

        Assert.Null(ex1);
        Assert.Null(ex2);
    }
}
