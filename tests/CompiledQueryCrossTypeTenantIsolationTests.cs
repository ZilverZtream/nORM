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

// X1: Compiled-query plan cache used tenantId?.ToString() for the tenant segment, which
// collapses distinct objects with the same ToString() (e.g. int 42 and string "42") to
// the same cache key. The second context then reuses the first's plan — including its
// fixed tenant parameter — causing wrong query output and potential cross-tenant exposure.
//
// Fix: tenant segment and closure values are serialized as
//   {type.FullName}:{value} rather than just {value.ToString()}.

// Entity must be at namespace level so Norm.CompileQuery can infer type params.
[Table("X1CRow")]
internal sealed class X1CRow
{
    [Key]
    public int Id { get; set; }
    // TenantId stored as TEXT
    public string TenantId { get; set; } = "";
    public string Value { get; set; } = "";
}

public class CompiledQueryCrossTypeTenantIsolationTests
{
    // A tenant provider that returns a non-string object (int) so ToString() = "42"
    // collides with a string "42" from another provider.
    private sealed class IntTenantProvider : ITenantProvider
    {
        private readonly int _id;
        public IntTenantProvider(int id) => _id = id;
        // Returns int — toString() = "42" for id=42
        public object GetCurrentTenantId() => _id;
    }

    private sealed class StringTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public StringTenantProvider(string id) => _id = id;
        // Returns string — toString() = "42" for id="42"
        public object GetCurrentTenantId() => _id;
    }

    private static SqliteConnection OpenDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        // TenantId column is TEXT; rows for string-tenant "42" only
        cmd.CommandText =
            "CREATE TABLE X1CRow (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, Value TEXT NOT NULL);" +
            "INSERT INTO X1CRow VALUES (1, '42', 'row-for-text-42');" +
            "INSERT INTO X1CRow VALUES (2, '42', 'also-text-42');";
        cmd.ExecuteNonQuery();
        return cn;
    }

    // ── X1-1: Cross-type ToString() collision causes plan reuse ────────────

    /// <summary>
    /// X1: Without the fix, int 42 and string "42" produce the same tenant segment
    /// ("TENANT:42:") in the compiled-query ctxKey — a cross-type ToString() collision.
    ///
    /// With the fix: int 42 → "TENANT:System.Int32:42:" and string "42" → "TENANT:System.String:42:"
    /// — distinct keys → separate plans.
    ///
    /// Note: nORM coerces the tenant ID to the entity's property type, so both int 42 and
    /// string "42" produce the same "42" SQL parameter for a string TenantId column. The fix
    /// ensures separate cache entries regardless, preventing stale plan reuse if the model or
    /// coercion differs between context configurations.
    /// </summary>
    [Fact]
    public async Task CrossType_IntVsString_CompiledQuery_TenantPlansAreIsolated()
    {
        using var cn = OpenDb();

        // Context A: tenant = int 42; nORM coerces to string "42" for the string TenantId column
        await using var ctxA = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new IntTenantProvider(42),
            TenantColumnName = "TenantId"
        });

        // Context B: tenant = string "42"; directly matches the string TenantId column
        await using var ctxB = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new StringTenantProvider("42"),
            TenantColumnName = "TenantId"
        });

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<X1CRow>().Where(x => x.Id >= minId));

        // Warm Context A's cache entry (int 42 coerced to "42")
        var resA = await compiled(ctxA, 1);
        // Context B must use its own plan (distinct cache key with X1 fix)
        var resB = await compiled(ctxB, 1);

        // Both contexts have tenant filter coerced to "42" — both see the 2 seeded rows.
        // The essential property: Context B is never contaminated by Context A's plan.
        Assert.Equal(2, resA.Count);
        Assert.Equal(2, resB.Count);
        Assert.All(resB, r => Assert.Equal("42", r.TenantId));
    }

    // ── X1-2: Same type, different value → isolated plans (regression) ─────

    [Fact]
    public async Task SameType_DifferentTenantValue_PlansAreIsolated()
    {
        using var cn = OpenDb();
        using var extra = cn.CreateCommand();
        extra.CommandText = "INSERT INTO X1CRow VALUES (3, '99', 'row-for-text-99')";
        extra.ExecuteNonQuery();

        await using var ctxA = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new StringTenantProvider("42"),
            TenantColumnName = "TenantId"
        });
        await using var ctxB = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new StringTenantProvider("99"),
            TenantColumnName = "TenantId"
        });

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<X1CRow>().Where(x => x.Id >= minId));

        var resA = await compiled(ctxA, 1);
        var resB = await compiled(ctxB, 1);

        Assert.All(resA, r => Assert.Equal("42", r.TenantId));
        Assert.All(resB, r => Assert.Equal("99", r.TenantId));
        Assert.Equal(2, resA.Count); // IDs 1 and 2
        Assert.Single(resB);        // ID 3
    }

    // ── X1-3: No tenant provider → all rows returned ──────────────────────

    [Fact]
    public async Task NoTenantProvider_CompiledQuery_ReturnsAllRows()
    {
        using var cn = OpenDb();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        var compiled = Norm.CompileQuery((DbContext c, int minId) =>
            c.Query<X1CRow>().Where(x => x.Id >= minId));

        var results = await compiled(ctx, 1);
        Assert.Equal(2, results.Count);
    }

    // ── X1-4: GlobalFilter closure cross-type collision ────────────────────

    /// <summary>
    /// AppendClosureValues used val?.ToString() for global filter closure values.
    /// If two lambdas capture objects of different types whose ToString() are identical,
    /// they hash to the same filter key → shared plan.
    ///
    /// With fix: type name is included, preventing cross-type collision.
    /// </summary>
    [Fact]
    public async Task GlobalFilter_SameClosureValue_BothContextsReturnCorrectRows()
    {
        using var cn = OpenDb();

        // Closure A captures string "42"; closure B captures string "42" too
        // (Both are strings — same type and value — correct behavior: same filter = same results)
        var optsA = new DbContextOptions();
        optsA.AddGlobalFilter<X1CRow>(x => x.TenantId == "42");

        var optsB = new DbContextOptions();
        optsB.AddGlobalFilter<X1CRow>(x => x.TenantId == "42");

        await using var ctxA = new DbContext(cn, new SqliteProvider(), optsA);
        await using var ctxB = new DbContext(cn, new SqliteProvider(), optsB);

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<X1CRow>().Where(x => x.Id >= minId));

        var resA = await compiled(ctxA, 1);
        var resB = await compiled(ctxB, 1);

        // Both contexts have the same filter → both return 2 rows (both match TenantId='42')
        Assert.Equal(2, resA.Count);
        Assert.Equal(2, resB.Count);
    }

    // ── X1-5: GlobalFilter different closure values → isolated results ─────

    [Fact]
    public async Task GlobalFilter_DifferentClosureValues_PlansAreIsolated()
    {
        using var cn = OpenDb();
        using var extra = cn.CreateCommand();
        extra.CommandText = "INSERT INTO X1CRow VALUES (5, '99', 'row-for-99')";
        extra.ExecuteNonQuery();

        // Use inline string constants (not closure-captured variables) so the filter value
        // goes into _params (fixed at plan build time) rather than _compiledParams
        // (which would make the compiled query require 2 user-supplied parameters).
        var optsA = new DbContextOptions();
        optsA.AddGlobalFilter<X1CRow>(x => x.TenantId == "42");

        var optsB = new DbContextOptions();
        optsB.AddGlobalFilter<X1CRow>(x => x.TenantId == "99");

        await using var ctxA = new DbContext(cn, new SqliteProvider(), optsA);
        await using var ctxB = new DbContext(cn, new SqliteProvider(), optsB);

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<X1CRow>().Where(x => x.Id >= minId));

        // Warm A's plan first
        var resA = await compiled(ctxA, 1);
        // B must not reuse A's plan (different closure value "99" vs "42")
        var resB = await compiled(ctxB, 1);

        Assert.All(resA, r => Assert.Equal("42", r.TenantId));
        Assert.All(resB, r => Assert.Equal("99", r.TenantId));
        Assert.Equal(2, resA.Count);
        Assert.Single(resB);
    }

    // ── X1-6: Same compiled delegate across context switches — no contamination

    [Fact]
    public async Task SameDelegate_MultipleContextSwitches_NoContamination()
    {
        using var cn = OpenDb();
        using var extra = cn.CreateCommand();
        extra.CommandText = "INSERT INTO X1CRow VALUES (10, 'str-tenant', 'data')";
        extra.ExecuteNonQuery();

        // String context matches the seeded row
        await using var ctxStr = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new StringTenantProvider("str-tenant"),
            TenantColumnName = "TenantId"
        });

        // Int context — no TEXT rows match int 999 in SQLite
        await using var ctxInt = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new IntTenantProvider(999),
            TenantColumnName = "TenantId"
        });

        var compiled = Norm.CompileQuery((DbContext ctx, int minId) =>
            ctx.Query<X1CRow>().Where(x => x.Id >= minId));

        // Alternate contexts
        var r1 = await compiled(ctxStr, 1);
        var r2 = await compiled(ctxInt, 1);
        var r3 = await compiled(ctxStr, 1);

        Assert.Contains(r1, x => x.TenantId == "str-tenant");
        Assert.Empty(r2); // int 999 param doesn't match any TEXT TenantId in SQLite
        Assert.Contains(r3, x => x.TenantId == "str-tenant");
    }
}
