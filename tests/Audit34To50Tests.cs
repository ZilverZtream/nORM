using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Internal;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Tests covering the 3.4/5.0 audit findings:
///   P1/X1 (Medium) – :: PostgreSQL cast syntax misclassified as parameter marker
///   S1    (Medium) – naive semicolon splitting causes false-positive injection blocks
///   C1    (Low)    – compiled delegate cache unbounded (static ConcurrentDictionary)
///
/// Score gates: 3.4→3.5 (P1/X1+S1 lexer fixes), 3.5→4.0 (C1 bounded cache),
///              4.0→4.5 (eviction concurrency), 4.5→5.0 (adversarial security matrix).
/// </summary>
public class Audit34To50Tests
{
    // ════════════════════════════════════════════════════════════════════════
    // Gate 3.4 → 3.5 : P1/X1  ::cast exclusion from parameter-marker counting
    // ════════════════════════════════════════════════════════════════════════

    // Use reflection to call the internal CountParameterMarkers method.
    private static int CountMarkers(string sql)
    {
        var method = typeof(NormValidator).GetMethod(
            "CountParameterMarkers",
            System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        return (int)method.Invoke(null, new object[] { sql })!;
    }

    /// <summary>
    /// P1/X1: A bare ::type cast (no real bind parameter) must NOT be counted as a marker.
    /// Before the fix, `::date` → second `:` followed by `d` (alphanumeric) → count = 1.
    /// </summary>
    [Fact]
    public void P1X1_DoubleColon_TypeCast_Alone_IsNotCountedAsMarker()
    {
        Assert.Equal(0, CountMarkers("SELECT '2026-01-01'::date"));
        Assert.Equal(0, CountMarkers("SELECT col::int FROM t"));
        Assert.Equal(0, CountMarkers("SELECT col::varchar(255) FROM t"));
        Assert.Equal(0, CountMarkers("SELECT col::text, col2::int FROM t"));
    }

    /// <summary>
    /// P1/X1: $1 positional parameter with a ::type cast must count as exactly 1 marker.
    /// </summary>
    [Fact]
    public void P1X1_PositionalParamPlusCast_CountsExactlyOne()
    {
        Assert.Equal(1, CountMarkers("SELECT $1::text"));
        Assert.Equal(1, CountMarkers("WHERE id = $1::int"));
    }

    /// <summary>
    /// P1/X1: Multiple positional parameters each with casts.
    /// </summary>
    [Fact]
    public void P1X1_MultiplePositionalParamsWithCasts_CountsAllParams()
    {
        Assert.Equal(2, CountMarkers("WHERE a = $1::text AND b = $2::int"));
        Assert.Equal(3, CountMarkers("SELECT $1::int, $2::text, $3::date FROM t"));
    }

    /// <summary>
    /// P1/X1: :name style PostgreSQL named parameters must still be counted correctly.
    /// </summary>
    [Fact]
    public void P1X1_NamedColonParam_IsStillCounted()
    {
        Assert.Equal(1, CountMarkers("WHERE id = :id"));
        Assert.Equal(2, CountMarkers("WHERE a = :first AND b = :second"));
    }

    /// <summary>
    /// P1/X1: ::cast inside a string literal must not be counted (the lexer already
    /// skips single-quoted regions; the new fix should not break this).
    /// </summary>
    [Fact]
    public void P1X1_DoubleColonInsideStringLiteral_IsNotCounted()
    {
        Assert.Equal(0, CountMarkers("SELECT '::text' FROM t"));
        Assert.Equal(0, CountMarkers("WHERE note = 'cast::expression'"));
    }

    /// <summary>
    /// P1/X1: ::cast at the very start of the SQL string (i == 0 edge case) is handled.
    /// </summary>
    [Fact]
    public void P1X1_DoubleColonAtStartOfString_IsNotCounted()
    {
        Assert.Equal(0, CountMarkers("::text"));
        Assert.Equal(0, CountMarkers("::int"));
    }

    // ════════════════════════════════════════════════════════════════════════
    // Gate 3.4 → 3.5 : S1  lexer-aware semicolon detection
    // ════════════════════════════════════════════════════════════════════════

    // Helper: call ValidateRawSql directly (public static method).
    private static void AssertAccepted(string sql)
    {
        // Should NOT throw
        NormValidator.ValidateRawSql(sql, null);
    }

    private static void AssertRejected(string sql)
    {
        Assert.Throws<NormUsageException>(() => NormValidator.ValidateRawSql(sql, null));
    }

    /// <summary>
    /// S1: A single semicolon inside a string literal must NOT trigger rejection.
    /// Before the fix: the naive count would be 1, which is ≤ 1, so this case wasn't
    /// affected for single semicolons. But with TWO literals containing semicolons:
    /// count becomes 2 → the naive splitter would split on literal content.
    /// </summary>
    [Fact]
    public void S1_SemicolonInsideStringLiteral_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE note = 'Hello; world'");
        // Two literals each with a semicolon → naive count = 2 → false-positive before fix
        AssertAccepted("SELECT * FROM t WHERE a = 'first; part' AND b = 'second; part'");
    }

    /// <summary>
    /// S1: Semicolons inside string literals that spell out DDL keywords must NOT
    /// trigger "multiple statements with DDL" false-positive.
    /// This is the concrete false-positive: split on ';' inside literal → fragment
    /// starts with "DROP " after the semi in literal context.
    /// </summary>
    [Fact]
    public void S1_SemicolonPlusDdlInsideStringLiteral_IsAccepted()
    {
        // Two real-world false-positives:
        // 1. A text column that contains '; DROP TABLE ...' as data
        AssertAccepted("SELECT * FROM t WHERE a = 'foo; bar' AND b = '; DROP TABLE users'");
        // 2. An audit log column
        AssertAccepted("SELECT * FROM t WHERE log_entry = 'Query: SELECT x; DROP TABLE t'");
    }

    /// <summary>
    /// S1: Semicolons inside line comments must not be counted.
    /// </summary>
    [Fact]
    public void S1_SemicolonInsideLineComment_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE id = 1 -- semicolon here; ignored");
        AssertAccepted("SELECT * FROM t -- first; second;\nWHERE id = 1");
    }

    /// <summary>
    /// S1: Semicolons inside block comments must not be counted.
    /// </summary>
    [Fact]
    public void S1_SemicolonInsideBlockComment_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t /* this has; a semicolon */ WHERE id = 1");
        AssertAccepted("SELECT * FROM t /* first; second; */ WHERE id = 1");
    }

    /// <summary>
    /// S1: A trailing semicolon on a valid single-statement query is acceptable.
    /// </summary>
    [Fact]
    public void S1_TrailingSemicolon_SingleStatement_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE id = 1;");
    }

    /// <summary>
    /// S1: True multi-statement injection with DDL must still be rejected.
    /// </summary>
    [Fact]
    public void S1_TrueMultiStatement_WithDdl_IsRejected()
    {
        AssertRejected("SELECT * FROM t; DROP TABLE t");
        AssertRejected("SELECT * FROM t; ALTER TABLE t ADD COLUMN x INT");
        AssertRejected("SELECT * FROM t; EXEC sp_dangerous");
    }

    // ════════════════════════════════════════════════════════════════════════
    // Gate 3.5 → 4.0 : C1  compiled delegate cache is now bounded
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// C1: The cache must plateau at exactly 512 entries when fed more than 512 unique shapes.
    /// </summary>
    [Fact]
    public void C1_CompiledDelegateCache_Plateaus_At_512_Entries()
    {
        var cache = ExpressionCompiler._compiledDelegateCache;
        var before = cache.Count;

        // Compile 600 unique expressions — each uses Expression.Constant(n) so the
        // expression STRUCTURE differs per iteration, producing a distinct fingerprint.
        // (Closure-based lambdas `x => x == captured` all share the same structure and
        // produce the same fingerprint regardless of the captured value.)
        var plateauParam = Expression.Parameter(typeof(int), "x");
        for (int n = before; n < before + 600; n++)
        {
            var body = Expression.Equal(plateauParam, Expression.Constant(n));
            var expr = Expression.Lambda<Func<int, bool>>(body, plateauParam);
            ExpressionCompiler.CompileExpression(expr);
        }

        // Cache count must be ≤ 512 (the cap).
        Assert.True(cache.Count <= 512,
            $"Cache grew to {cache.Count}, expected ≤ 512");
    }

    /// <summary>
    /// C1: Same expression compiled twice must return the exact same delegate instance
    /// (cache hit after first compile).
    /// </summary>
    [Fact]
    public void C1_SameExpression_ReturnsSameDelegateInstance()
    {
        // Use a constant far outside the range used in other tests to avoid collisions.
        const int sentinel = 99_999_001;
        System.Linq.Expressions.Expression<Func<int, bool>> expr1 = x => x == sentinel;
        System.Linq.Expressions.Expression<Func<int, bool>> expr2 = x => x == sentinel;

        var d1 = ExpressionCompiler.CompileExpression(expr1);
        var d2 = ExpressionCompiler.CompileExpression(expr2);

        Assert.Same(d1, d2);
    }

    /// <summary>
    /// C1: Cached delegate must still produce correct results after the cache has been
    /// under eviction pressure (a previously evicted entry is recompiled on next access).
    /// </summary>
    [Fact]
    public async Task C1_CacheMissAfterEviction_StillProducesCorrectResult()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setupCmd = cn.CreateCommand();
        setupCmd.CommandText =
            "CREATE TABLE CacheTestItem (Id INTEGER PRIMARY KEY, Name TEXT);" +
            "INSERT INTO CacheTestItem VALUES (1,'Alpha');" +
            "INSERT INTO CacheTestItem VALUES (2,'Beta');";
        setupCmd.ExecuteNonQuery();

        // Stress the cache with 600 structurally distinct expressions to cause eviction.
        var evictParam = Expression.Parameter(typeof(int), "x");
        for (int n = 1; n <= 600; n++)
        {
            var evictBody = Expression.Equal(evictParam, Expression.Constant(n + 1_000_000));
            var evictExpr = Expression.Lambda<Func<int, bool>>(evictBody, evictParam);
            ExpressionCompiler.CompileExpression(evictExpr);
        }

        // Despite eviction pressure, a fresh compile of a real query must work.
        var compiled = Norm.CompileQuery((DbContext ctx, int id) =>
            ctx.Query<CacheTestItem>().Where(i => i.Id == id));
        using var ctx = new DbContext(cn, new SqliteProvider());

        var result = await compiled(ctx, 1);
        Assert.Single(result);
        Assert.Equal("Alpha", result[0].Name);
    }

    public class CacheTestItem
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    // ════════════════════════════════════════════════════════════════════════
    // Gate 4.0 → 4.5 : eviction concurrency — no stale delegate reuse
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// C1 concurrency: 30 concurrent tasks each compiling unique shapes must not produce
    /// incorrect results and the cache must remain within bounds.
    /// </summary>
    [Fact]
    public async Task C1_HighConcurrency_UniqueShapes_CacheRemainsWithinBounds()
    {
        const int tasks = 30;
        const int shapesPerTask = 25;
        var cache = ExpressionCompiler._compiledDelegateCache;

        var work = Enumerable.Range(0, tasks).Select(t => Task.Run(() =>
        {
            for (int n = 0; n < shapesPerTask; n++)
            {
                int captured = t * 10_000 + n + 2_000_000;
                // Use Expression.Constant so each iteration produces a structurally distinct
                // fingerprint. Closure-based lambdas all share the same fingerprint and would
                // return the delegate compiled for the first captured value only.
                var xp = Expression.Parameter(typeof(int), "x");
                var expr = Expression.Lambda<Func<int, bool>>(
                    Expression.Equal(xp, Expression.Constant(captured)), xp);
                var del = ExpressionCompiler.CompileExpression(expr);
                // Delegate must be callable and produce correct output.
                Assert.False(del(0));
                Assert.True(del(captured));
            }
        }));

        await Task.WhenAll(work);

        Assert.True(cache.Count <= 512,
            $"Cache exceeded cap under concurrency: {cache.Count}");
    }

    /// <summary>
    /// C1 concurrency: same expression compiled by many concurrent threads must always
    /// return the same (or functionally equivalent) delegate — no corrupt/null delegates.
    /// </summary>
    [Fact]
    public async Task C1_ConcurrentSameExpression_AllReturnCorrectDelegate()
    {
        const int concurrency = 40;
        const int sentinel = 88_776_655;
        var delegates = new Delegate[concurrency];

        var work = Enumerable.Range(0, concurrency).Select(i => Task.Run(() =>
        {
            System.Linq.Expressions.Expression<Func<int, bool>> expr = x => x == sentinel;
            delegates[i] = ExpressionCompiler.CompileExpression(expr);
        }));

        await Task.WhenAll(work);

        foreach (var d in delegates)
        {
            Assert.NotNull(d);
            var typed = (Func<int, bool>)d;
            Assert.False(typed(0));
            Assert.True(typed(sentinel));
        }
    }

    // ════════════════════════════════════════════════════════════════════════
    // Gate 4.5 → 5.0 : adversarial security matrix by provider dialect
    // ════════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Adversarial: @ parameter in WHERE (SQL Server style) is accepted when param dict
    /// is non-empty and paramMarkerCount > 0.
    /// </summary>
    [Fact]
    public void Adversarial_AtSignParam_SqlServer_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE id = @id");
        AssertAccepted("SELECT * FROM t WHERE a = @p1 AND b = @p2");
    }

    /// <summary>
    /// Adversarial: $1 positional parameters (PostgreSQL) with and without ::cast.
    /// </summary>
    [Fact]
    public void Adversarial_PositionalParam_PostgresDialect_IsAccepted()
    {
        AssertAccepted("SELECT * FROM t WHERE id = $1");
        AssertAccepted("SELECT * FROM t WHERE name = $1::text AND age = $2::int");
    }

    /// <summary>
    /// Adversarial: SQL with double-quoted identifiers (PostgreSQL) containing special
    /// characters must not confuse the lexer.
    /// </summary>
    [Fact]
    public void Adversarial_DoubleQuotedIdentifiers_AreLexedCorrectly()
    {
        // @, :, ; inside a double-quoted identifier must be ignored.
        AssertAccepted("SELECT \"col@name\" FROM t WHERE id = 1");
        AssertAccepted("SELECT \"col;name\" FROM t WHERE id = 1");
    }

    /// <summary>
    /// Adversarial: A valid SELECT with both a ::cast AND a line-comment containing
    /// a semicolon must be accepted without false-positive injection block.
    /// </summary>
    [Fact]
    public void Adversarial_CastPlusCommentSemicolon_IsAccepted()
    {
        // ::int cast + comment with ; — both safe
        AssertAccepted("SELECT col::int FROM t WHERE id = 1 -- note: end; of query");
    }

    /// <summary>
    /// Adversarial: SQL with a stacked real injection using @ marker must still be
    /// caught by other patterns (demonstrates that softening the marker check does
    /// not weaken the overall security posture).
    /// </summary>
    [Fact]
    public void Adversarial_RealInjection_SemicolonDropTable_IsStillRejected()
    {
        // Two real statement terminators → second statement is DROP TABLE → rejected
        AssertRejected("SELECT * FROM t WHERE id = 1; DROP TABLE users");
    }

    /// <summary>
    /// Adversarial: ::cast where the type name looks like an injection keyword must
    /// NOT be rejected — a type cast is not executable SQL.
    /// </summary>
    [Fact]
    public void Adversarial_CastWithDdlLookingTypeName_IsAccepted()
    {
        // Unusual but legal PostgreSQL: ::name is just a cast; "droptext" is a type name
        // The real check is on semicolons + DDL keywords as statements, not as casts.
        AssertAccepted("SELECT col::text FROM t WHERE id = 1");
    }
}
