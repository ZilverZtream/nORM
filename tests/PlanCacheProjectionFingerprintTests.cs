using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Guards the plan-cache ExpressionFingerprint against collisions where two SEMANTICALLY-DIFFERENT queries
/// hash equal and one silently reuses the other's cached SQL/materializer. In particular a MemberInit DTO
/// projection with permuted binding TARGETS (new Dto { A=Col1, B=Col2 } vs { B=Col1, A=Col2 }) must produce
/// a distinct fingerprint, alongside bounding probes (constructor-arg swap, anon member-name swap, operator
/// swap) that were already distinct.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class PlanCacheProjectionFingerprintTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("Hunt56Row")]
    private class Row
    {
        [System.ComponentModel.DataAnnotations.Key]
        public int Id { get; set; }
        public int Col1 { get; set; }
        public int Col2 { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    // Named DTO whose members can be assigned in either binding order.
    private class Dto
    {
        public int A { get; set; }
        public int B { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE Hunt56Row (Id INTEGER PRIMARY KEY, Col1 INTEGER NOT NULL, Col2 INTEGER NOT NULL, Name TEXT NOT NULL);" +
            "INSERT INTO Hunt56Row VALUES (1, 100, 200, 'Alice');";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    // ─────────────────────────────────────────────────────────────────────────
    // SURFACE 6 (variant): MemberInit DTO projection where the binding TARGETS are
    // swapped but the VALUE visit-order is preserved.
    //
    //   A: new Dto { A = x.Col1, B = x.Col2 }   → SELECT Col1 AS A, Col2 AS B
    //   B: new Dto { B = x.Col1, A = x.Col2 }   → SELECT Col1 AS B, Col2 AS A
    //
    // The FingerprintVisitor overrides only VisitConstant/VisitMember/VisitMethodCall/
    // VisitParameter/VisitLambda. It does NOT override VisitMemberAssignment, and the base
    // ExpressionVisitor.VisitMemberAssignment visits only node.Expression (the value), never
    // node.Member (the target). Both queries visit values [Col1, Col2] in the same order, so
    // the structural fingerprints are identical → B reuses A's cached plan.
    // ─────────────────────────────────────────────────────────────────────────
    // Run-and-diff. Query A and Query B are semantically different (their binding TARGETS are
    // swapped) so they MUST return different results against the seeded row (Col1=100, Col2=200):
    //   A correct: { A=100, B=200 }      B correct: { A=200, B=100 }
    // Because the plan cache is static/process-wide and keyed by the (colliding) fingerprint, the
    // second query silently reuses the first's cached SQL + materializer, so both queries return
    // the SAME DTO — the collision collapses two distinct projections into one. This invariant is
    // robust to which plan happened to be cached first by any earlier test in the process.
    [Fact]
    public async Task MemberInit_BindingOrderSwap_MustNotCollapseToSameResult()
    {
        var (cn, ctx) = CreateContext();
        await using (cn) await using (ctx)
        {
            var a = await ctx.Query<Row>()
                .Select(x => new Dto { A = x.Col1, B = x.Col2 })
                .ToListAsync();
            var b = await ctx.Query<Row>()
                .Select(x => new Dto { B = x.Col1, A = x.Col2 })
                .ToListAsync();

            Assert.Single(a);
            Assert.Single(b);

            // The two projections are different; at least one field must differ between them.
            // A collision makes (a.A,a.B) == (b.A,b.B), and at least one of the two queries is
            // therefore returning the WRONG projection's values with no exception raised.
            Assert.False(a[0].A == b[0].A && a[0].B == b[0].B,
                $"Plan-cache collision: both projections returned identical DTO A={a[0].A},B={b[0].A}. " +
                "'new Dto {{ A=Col1, B=Col2 }}' and 'new Dto {{ B=Col1, A=Col2 }}' share a cached plan.");
        }
    }

    // Direct fingerprint-level proof, independent of execution. Two MemberInit lambdas that
    // differ ONLY in binding-target order must NOT share a plan-cache fingerprint.
    [Fact]
    public void MemberInit_BindingOrderSwap_FingerprintsDiffer()
    {
        var fpType = typeof(DbContext).Assembly
            .GetType("nORM.Query.ExpressionFingerprint", throwOnError: true)!;
        var compute = fpType.GetMethod("ComputeForPlanCache", BindingFlags.Public | BindingFlags.Static)!;

        Expression<Func<Row, Dto>> exprA = x => new Dto { A = x.Col1, B = x.Col2 };
        Expression<Func<Row, Dto>> exprB = x => new Dto { B = x.Col1, A = x.Col2 };

        var fpA = compute.Invoke(null, new object[] { exprA })!;
        var fpB = compute.Invoke(null, new object[] { exprB })!;

        Assert.NotEqual(fpA.ToString(), fpB.ToString());
    }

    // ── Bounding probes: characterize exactly where the bug lives ───────────────

    private static string Fp(Expression expr)
    {
        var fpType = typeof(DbContext).Assembly
            .GetType("nORM.Query.ExpressionFingerprint", throwOnError: true)!;
        var compute = fpType.GetMethod("ComputeForPlanCache", BindingFlags.Public | BindingFlags.Static)!;
        return compute.Invoke(null, new object[] { expr })!.ToString()!;
    }

    private class Dto3
    {
        public int A { get; set; }
        public int B { get; set; }
        public Dto3() { }
        public Dto3(int a, int b) { A = a; B = b; }
    }

    // SAFE: constructor-argument projection distinguishes by argument visit-order.
    [Fact]
    public void ConstructorArgSwap_FingerprintsDiffer_Clean()
    {
        Expression<Func<Row, Dto3>> exprA = x => new Dto3(x.Col1, x.Col2);
        Expression<Func<Row, Dto3>> exprB = x => new Dto3(x.Col2, x.Col1);
        Assert.NotEqual(Fp(exprA), Fp(exprB));
    }

    // SAFE: anonymous-type member swap yields DIFFERENT anon types (member names differ) → distinct type handle.
    [Fact]
    public void AnonymousTypeMemberNameSwap_FingerprintsDiffer_Clean()
    {
        Expression<Func<Row, object>> exprA = x => new { A = x.Col1, B = x.Col2 };
        Expression<Func<Row, object>> exprB = x => new { B = x.Col1, A = x.Col2 };
        Assert.NotEqual(Fp(exprA), Fp(exprB));
    }

    // SAFE: operator difference is captured by NodeType in Visit().
    [Fact]
    public void OperatorSwap_FingerprintsDiffer_Clean()
    {
        Expression<Func<Row, bool>> exprA = x => x.Col1 > 5;
        Expression<Func<Row, bool>> exprB = x => x.Col1 < 5;
        Assert.NotEqual(Fp(exprA), Fp(exprB));
    }

    // BUG (same family): nested member-member binding target swap also collides, because
    // VisitMemberMemberBinding does not hash the binding target member either.
    private class Outer { public Inner Inner { get; set; } = new(); }
    private class Inner { public int A { get; set; } public int B { get; set; } }

    [Fact]
    public void NestedMemberBindingSwap_FingerprintsDiffer()
    {
        Expression<Func<Row, Outer>> exprA = x => new Outer { Inner = { A = x.Col1, B = x.Col2 } };
        Expression<Func<Row, Outer>> exprB = x => new Outer { Inner = { B = x.Col1, A = x.Col2 } };
        Assert.NotEqual(Fp(exprA), Fp(exprB));
    }
}
