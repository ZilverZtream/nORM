using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Security regression: a user/closure-controlled STRING value in a computed ExecuteUpdate SetProperty must be
/// PARAMETERIZED, never inlined as a SQL string literal. Inlining with single-quote doubling alone
/// (<c>'…'.Replace("'","''")</c>) is unsafe on MySQL, which treats backslash as a string-literal escape under
/// its default sql_mode — so a value such as <c>\'</c> survives '-doubling and breaks out of the literal,
/// yielding a write-path SQL injection. The audit reached this via
/// <c>ExecuteUpdate(s =&gt; s.SetProperty(x =&gt; x.Bio, x =&gt; x.Bio + userInput))</c>, and the same
/// <c>RenderLiteral</c> path also renders the multi-tenant scope id.
///
/// Asserted PROVIDER-AGNOSTICALLY against the SET-clause generator (<see cref="BulkCudBuilder.BuildSetClause"/>):
/// the adversarial string appears only as a bound parameter value, never inline in the emitted SQL. A functional
/// SQLite round-trip can't prove it — SQLite doesn't treat backslash as an escape, so the breakout never fires
/// there; parameterization is what closes it on every provider.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ExecuteUpdateStringLiteralParameterizationTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("SecBioRow")]
    public sealed class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Bio { get; set; } = "";
    }

    // Backslash-quote breakout payload: inert under correct parameterization; a literal-inlining bug would let
    // the trailing fragment escape the string on MySQL.
    private const string Adversarial = @"\'; DROP TABLE SecBioRow; --";

    private static (BulkCudBuilder builder, TableMapping mapping, IDisposable ctx) NewBuilder()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        return (new BulkCudBuilder(ctx), ctx.GetMapping(typeof(Row)), ctx);
    }

    private static void AssertParameterized(string sql, IReadOnlyDictionary<string, object> prms, params string[] expectedBoundValues)
    {
        foreach (var v in expectedBoundValues)
        {
            Assert.DoesNotContain(v, sql);                                   // never inline
            Assert.Contains(prms, kv => Equals(kv.Value, v));               // always bound
        }
        Assert.DoesNotContain("DROP TABLE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Computed_concat_string_operand_is_bound_not_inlined()
    {
        var (builder, mapping, ctx) = NewBuilder(); using var _ = ctx;
        var suffix = Adversarial; // closure-captured, as a caller-supplied value would be
        var (sql, prms) = builder.BuildSetClause<Row>(mapping, s => s.SetProperty(p => p.Bio, p => p.Bio + suffix));
        AssertParameterized(sql, prms, Adversarial);
    }

    [Fact]
    public void Direct_closure_string_assignment_is_bound_not_inlined()
    {
        var (builder, mapping, ctx) = NewBuilder(); using var _ = ctx;
        var value = Adversarial;
        var (sql, prms) = builder.BuildSetClause<Row>(mapping, s => s.SetProperty(p => p.Bio, p => value));
        AssertParameterized(sql, prms, Adversarial);
    }

    [Fact]
    public void Ternary_branch_string_operands_are_bound_not_inlined()
    {
        var (builder, mapping, ctx) = NewBuilder(); using var _ = ctx;
        var hot = Adversarial;
        const string cold = "o'brien\\"; // second branch, also an escaping hazard on MySQL
        var (sql, prms) = builder.BuildSetClause<Row>(mapping, s => s.SetProperty(p => p.Bio, p => p.Id > 0 ? hot : cold));
        AssertParameterized(sql, prms, Adversarial, cold);
    }
}
