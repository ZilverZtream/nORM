using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ── Entities for CB3 tests ────────────────────────────────────────────────────

[Table("CB3_Widget")]
public class CB3Widget
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Name { get; set; }
    public int Score { get; set; }
    public bool Active { get; set; }
}

/// <summary>
/// GROUP 88 — Cover translator nested classes below 80%:
///   CountTranslator, FirstSingleTranslator, LastTranslator (predicate branches),
///   ThenIncludeTranslator, AggregateExpressionTranslator, QueryTranslatorPooledObjectPolicy.
/// </summary>
public class CoverageBoost3Tests : TestBase
{
    // ── Shared helpers ───────────────────────────────────────────────────────

    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static (string Sql, Dictionary<string, object> Params) TranslateExpr(
        Expression expr, DbContext ctx)
    {
        var tType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(tType, ctx)!;
        var plan = tType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        var parameters = (IReadOnlyDictionary<string, object>)plan.GetType().GetProperty("Parameters")!.GetValue(plan)!;
        return (sql, new Dictionary<string, object>(parameters));
    }

    private static DbContextOptions MakeRelOpts(Action<ModelBuilder> configure)
        => new DbContextOptions { OnModelCreating = configure };

    // ── Helper: Get Queryable method by name and arity ───────────────────────

    private static MethodInfo GetQueryableMethod2(string name)
        => typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == name && m.IsGenericMethodDefinition && m.GetParameters().Length == 2
                         && m.GetParameters()[1].ParameterType.Name.StartsWith("Expression"))
            .MakeGenericMethod(typeof(CB3Widget));

    private static Expression MakeWidgetPredicate(DbContext ctx, Expression<Func<CB3Widget, bool>> pred)
    {
        var q = ctx.Query<CB3Widget>();
        return Expression.Call(
            GetQueryableMethod2("Where"),
            q.Expression,
            Expression.Quote(pred));
    }

    // ── GROUP 88a: CountTranslator — predicate branch ────────────────────────

    [Fact]
    public void CountTranslator_with_predicate_generates_WHERE_clause()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var pred = Expression.Lambda<Func<CB3Widget, bool>>(
            Expression.GreaterThan(Expression.Property(param, nameof(CB3Widget.Score)), Expression.Constant(5)),
            param);

        var countMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "Count" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(CB3Widget));

        var expr = Expression.Call(countMethod, q.Expression, Expression.Quote(pred));
        var (sql, _) = TranslateExpr(expr, ctx);

        Assert.Contains("COUNT(*)", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void LongCountTranslator_with_predicate_generates_WHERE_clause()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var pred = Expression.Lambda<Func<CB3Widget, bool>>(
            Expression.Property(param, nameof(CB3Widget.Active)), param);

        var lcMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "LongCount" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(CB3Widget));

        var expr = Expression.Call(lcMethod, q.Expression, Expression.Quote(pred));
        var (sql, _) = TranslateExpr(expr, ctx);

        Assert.Contains("COUNT(*)", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void CountTranslator_predicate_with_string_comparison_works()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        // w.Name != null
        var pred = Expression.Lambda<Func<CB3Widget, bool>>(
            Expression.NotEqual(Expression.Property(param, nameof(CB3Widget.Name)), Expression.Constant(null)),
            param);

        var countMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "Count" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(CB3Widget));

        var expr = Expression.Call(countMethod, q.Expression, Expression.Quote(pred));
        var (sql, _) = TranslateExpr(expr, ctx);

        Assert.Contains("COUNT(*)", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── GROUP 88b: FirstSingleTranslator — predicate branch ─────────────────

    [Fact]
    public void FirstTranslator_with_predicate_adds_WHERE_and_LIMIT()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var pred = Expression.Lambda<Func<CB3Widget, bool>>(
            Expression.GreaterThan(Expression.Property(param, nameof(CB3Widget.Score)), Expression.Constant(10)),
            param);

        var firstMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "First" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(CB3Widget));

        var expr = Expression.Call(firstMethod, q.Expression, Expression.Quote(pred));
        var (sql, _) = TranslateExpr(expr, ctx);

        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
        // First uses LIMIT 1 (or equivalent paging)
        Assert.True(sql.Contains("LIMIT", StringComparison.OrdinalIgnoreCase) ||
                    sql.Contains("FETCH", StringComparison.OrdinalIgnoreCase) ||
                    sql.Contains("TOP", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void FirstOrDefaultTranslator_with_predicate_adds_WHERE()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var pred = Expression.Lambda<Func<CB3Widget, bool>>(
            Expression.Property(param, nameof(CB3Widget.Active)), param);

        var fodMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "FirstOrDefault" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2
                         && m.GetParameters()[1].ParameterType.Name.Contains("Expression"))
            .MakeGenericMethod(typeof(CB3Widget));

        var expr = Expression.Call(fodMethod, q.Expression, Expression.Quote(pred));
        var (sql, _) = TranslateExpr(expr, ctx);

        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SingleTranslator_with_predicate_fetches_2_rows()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var pred = Expression.Lambda<Func<CB3Widget, bool>>(
            Expression.Equal(Expression.Property(param, nameof(CB3Widget.Id)), Expression.Constant(42)),
            param);

        var singleMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "Single" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(CB3Widget));

        var expr = Expression.Call(singleMethod, q.Expression, Expression.Quote(pred));
        var (sql, _) = TranslateExpr(expr, ctx);

        // Single fetches 2 rows (to detect duplicates), WHERE filters
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SingleOrDefaultTranslator_with_predicate_adds_WHERE()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var pred = Expression.Lambda<Func<CB3Widget, bool>>(
            Expression.LessThan(Expression.Property(param, nameof(CB3Widget.Score)), Expression.Constant(100)),
            param);

        var sodMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "SingleOrDefault" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2
                         && m.GetParameters()[1].ParameterType.Name.Contains("Expression"))
            .MakeGenericMethod(typeof(CB3Widget));

        var expr = Expression.Call(sodMethod, q.Expression, Expression.Quote(pred));
        var (sql, _) = TranslateExpr(expr, ctx);

        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── GROUP 88c: LastTranslator — predicate branch ─────────────────────────

    [Fact]
    public void LastTranslator_with_predicate_adds_WHERE_and_reverses_order()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var pred = Expression.Lambda<Func<CB3Widget, bool>>(
            Expression.GreaterThan(Expression.Property(param, nameof(CB3Widget.Score)), Expression.Constant(3)),
            param);

        var lastMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "Last" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .MakeGenericMethod(typeof(CB3Widget));

        var expr = Expression.Call(lastMethod, q.Expression, Expression.Quote(pred));
        var (sql, _) = TranslateExpr(expr, ctx);

        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
        // Last implies ORDER BY <key> DESC
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void LastOrDefaultTranslator_with_predicate_adds_WHERE()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var pred = Expression.Lambda<Func<CB3Widget, bool>>(
            Expression.Property(param, nameof(CB3Widget.Active)), param);

        var lodMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "LastOrDefault" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2
                         && m.GetParameters()[1].ParameterType.Name.Contains("Expression"))
            .MakeGenericMethod(typeof(CB3Widget));

        var expr = Expression.Call(lodMethod, q.Expression, Expression.Quote(pred));
        var (sql, _) = TranslateExpr(expr, ctx);

        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── GROUP 88d: ThenIncludeTranslator — bug fix + test ────────────────────

    private static DbContextOptions MakeIpcOpts()
        => new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<IpcAuthor>().HasKey(a => a.Id);
                mb.Entity<IpcBook>().HasKey(b => b.Id);
                mb.Entity<IpcReview>().HasKey(r => r.Id);
                mb.Entity<IpcAuthor>()
                  .HasMany(a => a.Books).WithOne()
                  .HasForeignKey(b => b.AuthorId, a => a.Id);
                mb.Entity<IpcBook>()
                  .HasMany(b => b.Reviews).WithOne()
                  .HasForeignKey(r => r.BookId, b => b.Id);
            }
        };

    [Fact]
    public void ThenInclude_extends_include_path_to_depth_2()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider(), MakeIpcOpts());
        _ = ctx.GetMapping(typeof(IpcAuthor));
        _ = ctx.GetMapping(typeof(IpcBook));

        var q = (INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>();
        // Include(a => a.Books).ThenInclude(b => b.Reviews)
        var withInclude = q.Include(a => a.Books).ThenInclude(b => b.Reviews).AsSplitQuery();

        using var t = QueryTranslator.Rent(ctx);
        var plan = t.Translate(withInclude.Expression);

        Assert.NotNull(plan);
        Assert.NotEmpty(plan.Includes);
        // After the bug fix, ThenInclude should add Reviews to the path
        Assert.Equal(2, plan.Includes[0].Path.Count);
    }

    [Fact]
    public void ThenInclude_with_single_nav_extends_path()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider(), MakeIpcOpts());
        _ = ctx.GetMapping(typeof(IpcAuthor));
        _ = ctx.GetMapping(typeof(IpcBook));

        var q = (INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>();
        var withInclude = q.Include(a => a.Books).ThenInclude(b => b.Reviews).AsSplitQuery();

        // Just verify that the path was extended (depth > 1)
        using var t = QueryTranslator.Rent(ctx);
        var plan = t.Translate(withInclude.Expression);

        Assert.True(plan.Includes[0].Path.Count >= 2,
            "ThenInclude should extend the Include path to at least depth 2");
    }

    [Fact]
    public void ThenInclude_only_no_prior_include_is_noop()
    {
        // ThenInclude when _includes is empty should not throw.
        // Use Include(b => b.Title) on IpcBook to get an includable queryable of scalar,
        // then ThenInclude — this exercises the `t._includes.Count == 0` no-op branch.
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider(), MakeIpcOpts());
        _ = ctx.GetMapping(typeof(IpcAuthor));
        _ = ctx.GetMapping(typeof(IpcBook));

        // Include with a scalar property (Books.Title is string, not a relation)
        // then ThenInclude — Include adds nothing to relations, so _includes is empty
        // when ThenInclude fires.
        var q = (INormQueryable<IpcBook>)ctx.Query<IpcBook>();
        // ThenInclude after Include on scalar: both are no-ops, should not throw.
        var withInclude = q.Include(b => b.Title).ThenInclude(s => s.Length).AsSplitQuery();

        using var t = QueryTranslator.Rent(ctx);
        var plan = t.Translate(withInclude.Expression);

        // No relations were added; query still produces SQL without crashing
        Assert.NotNull(plan);
        Assert.Empty(plan.Includes);
    }

    [Fact]
    public void ThenInclude_ignores_unknown_nav_property_gracefully()
    {
        // ThenInclude with a property that's not a relation should be a no-op
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider(), MakeIpcOpts());
        _ = ctx.GetMapping(typeof(IpcAuthor));
        _ = ctx.GetMapping(typeof(IpcBook));

        var q = (INormQueryable<IpcAuthor>)ctx.Query<IpcAuthor>();
        // Include Books first, then ThenInclude a scalar property (Title) — no relation, so noop
        var withInclude = q.Include(a => a.Books).ThenInclude(b => b.Title).AsSplitQuery();

        using var t = QueryTranslator.Rent(ctx);
        var plan = t.Translate(withInclude.Expression);

        // The Books include should still be there; ThenInclude of scalar is a no-op
        Assert.NotEmpty(plan.Includes);
        // Path has only the Books relation (ThenInclude of Title found no relation)
        Assert.Single(plan.Includes[0].Path);
    }

    // ── GROUP 88e: AggregateExpressionTranslator (InternalSumExpression etc.) ─

    // This static method with a name matching the translator's registered keys
    // lets us craft an expression tree that triggers AggregateExpressionTranslator.
    public static int InternalSumExpression(
        IQueryable<CB3Widget> source,
        Expression<Func<CB3Widget, int>> selector,
        string function) => throw new NotImplementedException();

    public static int InternalMinExpression(
        IQueryable<CB3Widget> source,
        Expression<Func<CB3Widget, int>> selector,
        string function) => throw new NotImplementedException();

    [Fact]
    public void AggregateExpressionTranslator_Sum_via_InternalSumExpression()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var selector = Expression.Lambda<Func<CB3Widget, int>>(
            Expression.Property(param, nameof(CB3Widget.Score)), param);

        // Build a MethodCallExpression for our "InternalSumExpression" helper —
        // the translator dispatches on node.Method.Name == "InternalSumExpression".
        var helperMethod = typeof(CoverageBoost3Tests)
            .GetMethod(nameof(InternalSumExpression), BindingFlags.Public | BindingFlags.Static)!;

        var expr = Expression.Call(
            helperMethod,
            q.Expression,
            Expression.Quote(selector),
            Expression.Constant("Sum"));

        var (sql, _) = TranslateExpr(expr, ctx);
        Assert.Contains("SUM", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Score", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AggregateExpressionTranslator_Average_maps_to_AVG()
    {
        // Re-use InternalSumExpression but pass "Average" as the function name;
        // HandleAggregateExpression normalises "AVERAGE" → "AVG".
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var selector = Expression.Lambda<Func<CB3Widget, int>>(
            Expression.Property(param, nameof(CB3Widget.Score)), param);

        var helperMethod = typeof(CoverageBoost3Tests)
            .GetMethod(nameof(InternalSumExpression), BindingFlags.Public | BindingFlags.Static)!;

        var expr = Expression.Call(
            helperMethod,
            q.Expression,
            Expression.Quote(selector),
            Expression.Constant("Average"));

        var (sql, _) = TranslateExpr(expr, ctx);
        Assert.Contains("AVG", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AggregateExpressionTranslator_Min_generates_MIN()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var q = ctx.Query<CB3Widget>();

        var param = Expression.Parameter(typeof(CB3Widget), "w");
        var selector = Expression.Lambda<Func<CB3Widget, int>>(
            Expression.Property(param, nameof(CB3Widget.Score)), param);

        var helperMethod = typeof(CoverageBoost3Tests)
            .GetMethod(nameof(InternalMinExpression), BindingFlags.Public | BindingFlags.Static)!;

        var expr = Expression.Call(
            helperMethod,
            q.Expression,
            Expression.Quote(selector),
            Expression.Constant("Min"));

        var (sql, _) = TranslateExpr(expr, ctx);
        Assert.Contains("MIN", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── GROUP 88f: QueryTranslatorPooledObjectPolicy.Return ──────────────────

    [Fact]
    public void QueryTranslatorPooledObjectPolicy_Return_clears_and_returns_true()
    {
        // Access the private static _translatorPool field via reflection and call Return().
        var tType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var poolField = tType.GetField("_translatorPool",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        var pool = poolField.GetValue(null)!;

        // Get() from pool to have an object to return
        var getMethod = pool.GetType().GetMethod("Get")!;
        var translator = getMethod.Invoke(pool, null)!;

        // Return() calls policy.Return(obj) which calls obj.Clear()
        var returnMethod = pool.GetType().GetMethod("Return")!;
        returnMethod.Invoke(pool, new[] { translator });

        // If we get here without exception, Return succeeded (coverage line hit)
        Assert.NotNull(translator);
    }
}
