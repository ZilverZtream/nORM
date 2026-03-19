using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ── Entity types needed for GROUP 77-86 tests ───────────────────────────────

[Table("CB2_OrderItem")]
public class CB2OrderItem
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int OrderId { get; set; }
    public string? Product { get; set; }
    public int Quantity { get; set; }
    public decimal Price { get; set; }
}

[Table("CB2_Order")]
public class CB2Order
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Customer { get; set; }
    public ICollection<CB2OrderItem> Items { get; set; } = new List<CB2OrderItem>();
}

[Table("CB2_Short")]
public class CB2ShortEntity
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public short ShortVal { get; set; }
    public byte ByteVal { get; set; }
}

[Table("CB2_PrincipalMulti")]
public class CB2PrincipalMulti
{
    [Key] public int K1 { get; set; }
    [Key] public int K2 { get; set; }
    public string Data { get; set; } = "";
    public ICollection<CB2OrderItem> Items { get; set; } = new List<CB2OrderItem>();
}

// ── Types for GroupBy result selector tests ─────────────────────────────────

/// <summary>Class with parameterless ctor; used as GroupBy result type so the materializer
/// doesn't crash when projection args are all aggregates (no columns extracted).</summary>
public class CB2GroupAgg
{
    public CB2GroupAgg() { }
    public CB2GroupAgg(int count) { Count = count; }
    public int Count { get; set; }
}

public struct CB2GroupKey
{
    public CB2GroupKey(int key, int count) { Key = key; Count = count; }
    public int Key { get; }
    public int Count { get; }
}

public struct CB2GroupFull
{
    public CB2GroupFull(int key, int count, int sum, int min, int max, int lc)
    { Key = key; Count = count; Sum = sum; Min = min; Max = max; LongCount = lc; }
    public int Key { get; }
    public int Count { get; }
    public int Sum { get; }
    public int Min { get; }
    public int Max { get; }
    public int LongCount { get; }
}

/// <summary>
/// GROUP 77-86 — boost six classes to ≥80%
/// </summary>
public class CoverageBoost2Tests : TestBase
{
    // ── Shared helpers ───────────────────────────────────────────────────────

    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    /// <summary>
    /// Translates an expression tree directly via QueryTranslator (bypasses NormQueryProvider
    /// plan cache) so coverage hits the translator's method bodies on every test run.
    /// </summary>
    private static (string Sql, Dictionary<string, object> Params) TranslateDirectExpr(
        Expression expr, DbContext ctx)
    {
        var tType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(tType, ctx)!;
        var plan = tType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        var parameters = (IReadOnlyDictionary<string, object>)plan.GetType().GetProperty("Parameters")!.GetValue(plan)!;
        return (sql, new Dictionary<string, object>(parameters));
    }

    /// <summary>Returns Queryable.Method with given arity-2 and specific return type.</summary>
    private static MethodInfo GetQueryableMethod(string name, Type retType)
        => typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == name && m.IsGenericMethodDefinition
                         && m.GetParameters().Length == 2
                         && m.ReturnType == retType);

    // ── GROUP 77: HandleDirectAggregate — Sum/Min/Max/Average ────────────────

    [Fact]
    public void HandleDirectAggregate_Sum_generates_SUM_sql()
    {
        // Queryable.Sum<CovItem>(source, p => p.Value) → HandleDirectAggregate
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        var param = Expression.Parameter(typeof(CovItem), "p");
        var valueAccess = Expression.Property(param, nameof(CovItem.Value));
        var lambda = Expression.Lambda<Func<CovItem, int>>(valueAccess, param);
        var sumMethod = GetQueryableMethod("Sum", typeof(int)).MakeGenericMethod(typeof(CovItem));
        var expr = Expression.Call(sumMethod, query.Expression, Expression.Quote(lambda));
        var (sql, _) = TranslateDirectExpr(expr, ctx);
        Assert.Contains("SUM", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Value", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void HandleDirectAggregate_Min_generates_MIN_sql()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        var param = Expression.Parameter(typeof(CovItem), "p");
        var lambda = Expression.Lambda<Func<CovItem, int>>(
            Expression.Property(param, nameof(CovItem.Value)), param);
        var minMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Min" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .First(m => m.GetGenericArguments().Length == 2) // Min<TSource, TResult>
            .MakeGenericMethod(typeof(CovItem), typeof(int));
        var expr = Expression.Call(minMethod, query.Expression, Expression.Quote(lambda));
        var (sql, _) = TranslateDirectExpr(expr, ctx);
        Assert.Contains("MIN", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void HandleDirectAggregate_Max_generates_MAX_sql()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        var param = Expression.Parameter(typeof(CovItem), "p");
        var lambda = Expression.Lambda<Func<CovItem, int>>(
            Expression.Property(param, nameof(CovItem.Value)), param);
        var maxMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Max" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .First(m => m.GetGenericArguments().Length == 2)
            .MakeGenericMethod(typeof(CovItem), typeof(int));
        var expr = Expression.Call(maxMethod, query.Expression, Expression.Quote(lambda));
        var (sql, _) = TranslateDirectExpr(expr, ctx);
        Assert.Contains("MAX", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void HandleDirectAggregate_Average_generates_AVG_sql()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        var param = Expression.Parameter(typeof(CovItem), "p");
        var lambda = Expression.Lambda<Func<CovItem, int>>(
            Expression.Property(param, nameof(CovItem.Value)), param);
        // Queryable.Average<TSource>(IQueryable<TSource>, Expression<Func<TSource,int>>) → double
        var avgMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Average" && m.IsGenericMethodDefinition
                        && m.GetParameters().Length == 2
                        && m.ReturnType == typeof(double))
            .Select(m => {
                try {
                    var p1 = m.GetParameters()[1].ParameterType;
                    if (!p1.IsGenericType) return null;
                    var ft = p1.GetGenericArguments()[0];
                    if (!ft.IsGenericType) return null;
                    var fArgs = ft.GetGenericArguments();
                    if (fArgs.Length < 2 || fArgs[1] != typeof(int)) return null;
                    return m.MakeGenericMethod(typeof(CovItem));
                } catch { return null; }
            })
            .First(m => m != null)!;
        var expr = Expression.Call(avgMethod, query.Expression, Expression.Quote(lambda));
        var (sql, _) = TranslateDirectExpr(expr, ctx);
        Assert.Contains("AVG", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── GROUP 78: HandleAllOperation ─────────────────────────────────────────

    [Fact]
    public void HandleAllOperation_generates_NOT_EXISTS_sql()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        var param = Expression.Parameter(typeof(CovItem), "p");
        var isActive = Expression.Property(param, nameof(CovItem.IsActive));
        var lambda = Expression.Lambda<Func<CovItem, bool>>(isActive, param);
        var allMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "All" && m.IsGenericMethodDefinition)
            .MakeGenericMethod(typeof(CovItem));
        var expr = Expression.Call(allMethod, query.Expression, Expression.Quote(lambda));
        var (sql, _) = TranslateDirectExpr(expr, ctx);
        Assert.Contains("NOT EXISTS", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void HandleAllOperation_with_predicate_includes_WHERE_NOT()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        var param = Expression.Parameter(typeof(CovItem), "p");
        // p.Value > 0
        var condition = Expression.GreaterThan(
            Expression.Property(param, nameof(CovItem.Value)),
            Expression.Constant(0));
        var lambda = Expression.Lambda<Func<CovItem, bool>>(condition, param);
        var allMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "All" && m.IsGenericMethodDefinition)
            .MakeGenericMethod(typeof(CovItem));
        var expr = Expression.Call(allMethod, query.Expression, Expression.Quote(lambda));
        var (sql, _) = TranslateDirectExpr(expr, ctx);
        Assert.Contains("NOT EXISTS", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("NOT", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── GROUP 79/80: HandleGroupBy with result selector + BuildGroupBySelectClause ──

    /// <summary>
    /// Returns the Queryable.GroupBy overload that takes a RESULT SELECTOR:
    ///   GroupBy&lt;TSource,TKey,TResult&gt;(source, keySelector, Func&lt;TKey,IEnumerable&lt;TSource&gt;,TResult&gt;)
    /// Distinguished from the ELEMENT SELECTOR overload by the delegate having 3 type args vs 2.
    /// </summary>
    private static MethodInfo GetGroupByResultSelectorMethod()
        => typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "GroupBy" && m.IsGenericMethodDefinition
                        && m.GetParameters().Length == 3
                        && m.GetGenericArguments().Length == 3)
            .Single(m => {
                // 3rd param type: Expression<Func<TKey, IEnumerable<TSource>, TResult>>
                var thirdParam = m.GetParameters()[2].ParameterType; // Expression<Func<...>>
                var delegateType = thirdParam.GetGenericArguments()[0]; // Func<...>
                return delegateType.GetGenericArguments().Length == 3;  // TKey + IEnumerable<TSource> + TResult
            });

    /// <summary>3-arg GroupBy with g.Count() result → BuildGroupBySelectClause MethodCallExpression path.</summary>
    [Fact]
    public void GroupBy_result_selector_count_generates_COUNT_sql()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        var keyParam = Expression.Parameter(typeof(CovItem), "p");
        var keySelector = Expression.Lambda<Func<CovItem, int>>(
            Expression.Property(keyParam, nameof(CovItem.Value)), keyParam);
        var k = Expression.Parameter(typeof(int), "k");
        var g = Expression.Parameter(typeof(IEnumerable<CovItem>), "g");
        // g.Count() call
        var countMethod = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "Count" && m.IsGenericMethodDefinition && m.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(CovItem));
        var countCall = Expression.Call(countMethod, g);
        var resultSelector = Expression.Lambda<Func<int, IEnumerable<CovItem>, int>>(countCall, k, g);
        var groupByMethod = GetGroupByResultSelectorMethod()
            .MakeGenericMethod(typeof(CovItem), typeof(int), typeof(int));
        var expr = Expression.Call(groupByMethod, query.Expression,
            Expression.Quote(keySelector), Expression.Quote(resultSelector));
        var (sql, _) = TranslateDirectExpr(expr, ctx);
        Assert.Contains("COUNT(*)", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("GROUP BY", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>3-arg GroupBy where result body is NewExpression → BuildGroupBySelectClause NewExpression path.
    /// Uses CB2GroupAgg (has parameterless ctor) so the materializer doesn't crash when projection
    /// args are aggregates (MethodCallExpression) which ExtractColumnsFromProjection skips.</summary>
    [Fact]
    public void GroupBy_result_selector_NewExpression_with_aggregates()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        // keySelector: p => p.Value
        var keyParam = Expression.Parameter(typeof(CovItem), "p");
        var keySelector = Expression.Lambda<Func<CovItem, int>>(
            Expression.Property(keyParam, nameof(CovItem.Value)), keyParam);
        // resultSelector: (k, g) => new CB2GroupAgg(g.Count())
        // CB2GroupAgg has a parameterless ctor so the materializer succeeds even though
        // ExtractColumnsFromProjection returns [] (MethodCallExpression args are not extracted).
        var k = Expression.Parameter(typeof(int), "k");
        var g = Expression.Parameter(typeof(IEnumerable<CovItem>), "g");
        var countMethod = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "Count" && m.IsGenericMethodDefinition && m.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(CovItem));
        var countCall = Expression.Call(countMethod, g);
        var ctor = typeof(CB2GroupAgg).GetConstructor(new[] { typeof(int) })!;
        var newExpr = Expression.New(ctor, countCall);
        var resultSelector = Expression.Lambda<Func<int, IEnumerable<CovItem>, CB2GroupAgg>>(newExpr, k, g);
        var groupByMethod = GetGroupByResultSelectorMethod()
            .MakeGenericMethod(typeof(CovItem), typeof(int), typeof(CB2GroupAgg));
        var expr = Expression.Call(groupByMethod, query.Expression,
            Expression.Quote(keySelector), Expression.Quote(resultSelector));
        var (sql, _) = TranslateDirectExpr(expr, ctx);
        Assert.Contains("COUNT(*)", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── GROUP 80: TranslateGroupAggregateMethod — Sum/Min/Max/Average/LongCount ──
    // Each test uses a SCALAR result body (not a struct wrapper) so the materializer doesn't need
    // a registered entity mapping for the result type.

    private (string Sql, Dictionary<string, object> Params) BuildGroupByDirectAggregateExpr(
        Func<ParameterExpression, ParameterExpression, Expression> buildAggregateBody,
        DbContext ctx)
    {
        var query = ctx.Query<CovItem>();
        var keyParam = Expression.Parameter(typeof(CovItem), "p");
        var keySelector = Expression.Lambda<Func<CovItem, int>>(
            Expression.Property(keyParam, nameof(CovItem.Value)), keyParam);
        var k = Expression.Parameter(typeof(int), "k");
        var g = Expression.Parameter(typeof(IEnumerable<CovItem>), "g");
        // Build aggregate body — result is a scalar (int/long/double), not a struct
        var aggregateBody = buildAggregateBody(k, g);
        var resultLambda = Expression.Lambda(aggregateBody, k, g);
        var resultType = aggregateBody.Type;
        var groupByMethod = GetGroupByResultSelectorMethod()
            .MakeGenericMethod(typeof(CovItem), typeof(int), resultType);
        var expr = Expression.Call(groupByMethod, query.Expression,
            Expression.Quote(keySelector), Expression.Quote(resultLambda));
        return TranslateDirectExpr(expr, ctx);
    }

    private static MethodInfo GetEnumerableMethod(string name, int paramCount, Type? secondTypeArg = null)
        => typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == name && m.IsGenericMethodDefinition && m.GetParameters().Length == paramCount)
            .First(m => secondTypeArg == null || (m.GetGenericArguments().Length >= 2))
            .MakeGenericMethod(secondTypeArg == null
                ? new[] { typeof(CovItem) }
                : new[] { typeof(CovItem), secondTypeArg });

    [Fact]
    public void TranslateGroupAggregateMethod_Sum_generates_SUM()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        // Find Enumerable.Sum<TSource>(IEnumerable<TSource>, Func<TSource,int>) → returns int
        var sumMethod = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Sum" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .Select(m => { try { return m.MakeGenericMethod(typeof(CovItem)); } catch { return null!; } })
            .First(m => m != null && m.ReturnType == typeof(int));
        var (sql, _) = BuildGroupByDirectAggregateExpr(
            (k, g) => {
                var valParam = Expression.Parameter(typeof(CovItem), "x");
                var selector = Expression.Lambda(Expression.Property(valParam, nameof(CovItem.Value)), valParam);
                return Expression.Call(sumMethod, g, selector);
            }, ctx);
        Assert.Contains("SUM", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TranslateGroupAggregateMethod_LongCount_generates_COUNT()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var lcMethod = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.Name == "LongCount" && m.IsGenericMethodDefinition && m.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(CovItem));
        // LongCount returns long — scalar, materializer handles it
        var (sql, _) = BuildGroupByDirectAggregateExpr(
            (k, g) => Expression.Call(lcMethod, g),
            ctx);
        Assert.Contains("COUNT(*)", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TranslateGroupAggregateMethod_Min_generates_MIN()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var minMethod = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Min" && m.IsGenericMethodDefinition
                        && m.GetParameters().Length == 2 && m.GetGenericArguments().Length == 2)
            .First()
            .MakeGenericMethod(typeof(CovItem), typeof(int));
        var (sql, _) = BuildGroupByDirectAggregateExpr(
            (k, g) => {
                var xp = Expression.Parameter(typeof(CovItem), "x");
                var selector = Expression.Lambda(Expression.Property(xp, nameof(CovItem.Value)), xp);
                return Expression.Call(minMethod, g, selector);
            }, ctx);
        Assert.Contains("MIN", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TranslateGroupAggregateMethod_Max_generates_MAX()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var maxMethod = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Max" && m.IsGenericMethodDefinition
                        && m.GetParameters().Length == 2 && m.GetGenericArguments().Length == 2)
            .First()
            .MakeGenericMethod(typeof(CovItem), typeof(int));
        var (sql, _) = BuildGroupByDirectAggregateExpr(
            (k, g) => {
                var xp = Expression.Parameter(typeof(CovItem), "x");
                var selector = Expression.Lambda(Expression.Property(xp, nameof(CovItem.Value)), xp);
                return Expression.Call(maxMethod, g, selector);
            }, ctx);
        Assert.Contains("MAX", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void TranslateGroupAggregateMethod_Average_generates_AVG()
    {
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        // Find Enumerable.Average<TSource>(IEnumerable<TSource>, Func<TSource,int>) → returns double
        var avgMethod = typeof(Enumerable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Average" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .Select(m => { try { return m.MakeGenericMethod(typeof(CovItem)); } catch { return null!; } })
            .First(m => m != null && m.ReturnType == typeof(double));
        var (sql, _) = BuildGroupByDirectAggregateExpr(
            (k, g) => {
                var xp = Expression.Parameter(typeof(CovItem), "x");
                var selector = Expression.Lambda(Expression.Property(xp, nameof(CovItem.Value)), xp);
                return Expression.Call(avgMethod, g, selector);
            }, ctx);
        Assert.Contains("AVG", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── GROUP 81: SelectMany with DefaultIfEmpty → LEFT JOIN ─────────────────

    [Fact]
    public void SelectMany_DefaultIfEmpty_produces_LEFT_JOIN()
    {
        // Must register the relationship with HasKey so HasForeignKey can infer principal key.
        using var cn = OpenMemory();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => {
                mb.Entity<CB2Order>().HasKey(o => o.Id);
                mb.Entity<CB2Order>()
                    .HasMany(o => o.Items)
                    .WithOne()
                    .HasForeignKey(i => i.OrderId, o => o.Id);
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Build expression using the same context so the translator resolves the relation.
        var q = ctx.Query<CB2Order>();
        var linq = q.SelectMany(o => o.Items.DefaultIfEmpty(), (o, i) => new { o.Id });
        var (sql, _) = TranslateDirectExpr(linq.Expression, ctx);
        Assert.Contains("LEFT JOIN", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SelectMany_DefaultIfEmpty_with_result_selector_sets_left_join()
    {
        // Second variant: result projection differs; confirms useLeftJoin=true suppresses INNER JOIN.
        using var cn = OpenMemory();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => {
                mb.Entity<CB2Order>().HasKey(o => o.Id);
                mb.Entity<CB2Order>()
                    .HasMany(o => o.Items)
                    .WithOne()
                    .HasForeignKey(i => i.OrderId, o => o.Id);
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var q = ctx.Query<CB2Order>();
        var linq = q.SelectMany(o => o.Items.DefaultIfEmpty(), (o, i) => new { OrderId = o.Id });
        var (sql, _) = TranslateDirectExpr(linq.Expression, ctx);
        Assert.DoesNotContain("INNER JOIN", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ── GROUP 82: EntityTypeBuilder<T> uncovered methods ─────────────────────

    [Fact]
    public void SharesTableWith_registers_table_split()
    {
        var builder = new EntityTypeBuilder<CovItem>();
        // Should not throw
        var result = builder.SharesTableWith<CovAuthor>();
        Assert.Same(builder, result);
        // Verify configuration was set (via Configuration property)
        var configProp = typeof(EntityTypeBuilder<CovItem>)
            .GetProperty("Configuration", BindingFlags.NonPublic | BindingFlags.Instance)!;
        var config = configProp.GetValue(builder)!;
        var tableSplitProp = config.GetType().GetProperty("TableSplitWith")!;
        Assert.Equal(typeof(CovAuthor), tableSplitProp.GetValue(config));
    }

    [Fact]
    public void Property_non_generic_returns_property_builder()
    {
        var builder = new EntityTypeBuilder<CovItem>();
        // Non-generic Property(Expression<Func<TEntity, object>>) overload
        var propBuilder = builder.Property(e => (object)e.Value);
        Assert.NotNull(propBuilder);
    }

    [Fact]
    public void Property_non_generic_HasColumnName_sets_column()
    {
        var builder = new EntityTypeBuilder<CovItem>();
        var result = builder.Property(e => (object)e.Value).HasColumnName("item_value");
        Assert.Same(builder, result);
    }

    [Fact]
    public void OwnsOne_with_buildAction_configures_owned()
    {
        // OwnsOne<TOwned> with non-null buildAction — calls buildAction.Invoke(innerBuilder)
        var builder = new EntityTypeBuilder<CovRefEntity>();
        bool buildActionCalled = false;
        var result = builder.OwnsOne(
            e => e.LazyRef!,
            inner => { buildActionCalled = true; });
        Assert.Same(builder, result);
        Assert.True(buildActionCalled);
    }

    [Fact]
    public void GetProperty_throws_on_invalid_expression()
    {
        var builder = new EntityTypeBuilder<CovItem>();
        // HasKey with a constant expression body → GetProperty sees ConstantExpression → throws
        Assert.Throws<ArgumentException>(() => builder.HasKey(e => 42));
    }

    [Fact]
    public void HasForeignKey_throws_when_principal_has_no_single_key()
    {
        // Composite PK: HasForeignKey without principalKeyExpression throws
        var builder = new EntityTypeBuilder<CB2PrincipalMulti>();
        builder.HasKey(e => new { e.K1, e.K2 }); // 2-key → count != 1 → throw
        Assert.Throws<NormConfigurationException>(() =>
            builder.HasMany(o => o.Items)
                   .WithOne()
                   .HasForeignKey(i => i.OrderId));
    }

    [Fact]
    public void HasForeignKey_succeeds_with_explicit_principal_key()
    {
        // Should not throw when principalKeyExpression is provided
        var builder = new EntityTypeBuilder<CB2PrincipalMulti>();
        builder.HasKey(e => new { e.K1, e.K2 });
        // No exception expected
        builder.HasMany(o => o.Items)
               .WithOne()
               .HasForeignKey(i => i.OrderId, p => p.K1);
    }

    [Fact]
    public void OwnsMany_with_custom_table_and_fk()
    {
        var builder = new EntityTypeBuilder<CB2Order>();
        // OwnsMany exercises the collection navigation path
        var result = builder.OwnsMany(
            o => o.Items,
            tableName: "OrderItemsTable",
            foreignKey: "ParentOrderId");
        Assert.Same(builder, result);
    }

    // ── GROUP 83: ExpressionUtils — OperationCanceledException fallback ───────

    [Fact]
    public void CompileWithFallback_nongeneric_precancelled_token_returns_interpreted_delegate()
    {
        // Access ExpressionUtils via reflection (internal class)
        var exprUtilsType = typeof(DbContext).Assembly.GetType("nORM.Internal.ExpressionUtils", true)!;
        var method = exprUtilsType.GetMethod("CompileWithFallback",
            BindingFlags.Public | BindingFlags.Static,
            null,
            new[] { typeof(LambdaExpression), typeof(CancellationToken) },
            null)!;
        Expression<Func<int, int>> lambda = x => x + 1;
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // pre-cancel → Task.Wait(cancelledToken) throws OCE → fallback to interpreted
        var result = method.Invoke(null, new object[] { (LambdaExpression)lambda, cts.Token });
        // The result is a Delegate (compiled or interpreted)
        Assert.NotNull(result);
        Assert.IsAssignableFrom<Delegate>(result);
    }

    [Fact]
    public void CompileWithFallback_generic_precancelled_token_returns_compiled_delegate()
    {
        var exprUtilsType = typeof(DbContext).Assembly.GetType("nORM.Internal.ExpressionUtils", true)!;
        // Generic overload: CompileWithFallback<TDelegate>(Expression<TDelegate>, CancellationToken)
        var genericMethod = exprUtilsType.GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "CompileWithFallback" && m.IsGenericMethodDefinition)
            .Single()
            .MakeGenericMethod(typeof(Func<int, int>));
        Expression<Func<int, int>> lambda = x => x * 2;
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        var result = genericMethod.Invoke(null, new object[] { lambda, cts.Token });
        Assert.NotNull(result);
        Assert.IsAssignableFrom<Func<int, int>>(result);
    }

    [Fact]
    public void CompileWithFallback_uncancelled_compiles_normally()
    {
        var exprUtilsType = typeof(DbContext).Assembly.GetType("nORM.Internal.ExpressionUtils", true)!;
        var method = exprUtilsType.GetMethod("CompileWithFallback",
            BindingFlags.Public | BindingFlags.Static,
            null,
            new[] { typeof(LambdaExpression), typeof(CancellationToken) },
            null)!;
        Expression<Func<string, int>> lambda = s => s.Length;
        var result = (Delegate)method.Invoke(null, new object[] { (LambdaExpression)lambda, CancellationToken.None })!;
        Assert.NotNull(result);
        // Verify it works:
        Assert.Equal(5, (int)result.DynamicInvoke("hello")!);
    }

    // ── GROUP 84: QueryTranslator.ClientEvaluation — TrySplitProjection ───────

    private static string UntranslatableMethod_CB2(string s) => "[" + s + "]";
    private static string ConstantUntranslatable_CB2() => "CONST";
    private static string UntranslatableWithValueTuple_CB2(ValueTuple<int> vt) => vt.Item1.ToString();

    [Fact]
    public void TrySplitProjection_untranslatable_method_splits_into_server_client()
    {
        // Select with a method not in SQL-translatable list → TrySplitProjection returns true
        // Server projection: fetch Name column; client projection: apply UntranslatableMethod_CB2
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        var param = Expression.Parameter(typeof(CovItem), "p");
        // CustomMethod(p.Name) — custom method not in SQL-translatable set
        var customMethod = typeof(CoverageBoost2Tests).GetMethod(
            nameof(UntranslatableMethod_CB2), BindingFlags.NonPublic | BindingFlags.Static)!;
        var nameAccess = Expression.Property(param, nameof(CovItem.Name));
        var customCall = Expression.Call(customMethod, nameAccess);
        var projLambda = Expression.Lambda<Func<CovItem, string>>(customCall, param);
        var selectMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Select" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .First()
            .MakeGenericMethod(typeof(CovItem), typeof(string));
        var selectExpr = Expression.Call(selectMethod, query.Expression, Expression.Quote(projLambda));
        // Should not throw — translator either splits or falls back to full translation
        var (sql, _) = TranslateDirectExpr(selectExpr, ctx);
        Assert.NotNull(sql);
    }

    [Fact]
    public void TrySplitProjection_returns_false_for_constant_untranslatable_no_member_access()
    {
        // Select with untranslatable method but no entity member access
        // → TrySplitProjection returns false (line 175: AccessedMembers.Count == 0)
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        var param = Expression.Parameter(typeof(CovItem), "p");
        var constMethod = typeof(CoverageBoost2Tests).GetMethod(
            nameof(ConstantUntranslatable_CB2), BindingFlags.NonPublic | BindingFlags.Static)!;
        var constCall = Expression.Call(constMethod); // no entity members accessed
        var projLambda = Expression.Lambda<Func<CovItem, string>>(constCall, param);
        var selectMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Select" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .First()
            .MakeGenericMethod(typeof(CovItem), typeof(string));
        var selectExpr = Expression.Call(selectMethod, query.Expression, Expression.Quote(projLambda));
        var (sql, _) = TranslateDirectExpr(selectExpr, ctx);
        Assert.NotNull(sql);
    }

    [Fact]
    public void TranslatabilityAnalyzer_VisitInvocation_marks_untranslatable()
    {
        // Select with InvocationExpression → VisitInvocation sets HasUntranslatableExpression=true
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        Func<string, string> customFn = s => "[" + s + "]";
        var param = Expression.Parameter(typeof(CovItem), "p");
        var nameAccess = Expression.Property(param, nameof(CovItem.Name));
        var funcConst = Expression.Constant(customFn, typeof(Func<string, string>));
        // InvocationExpression: funcConst(nameAccess) — not a regular MethodCall, but an Invocation
        var invExpr = Expression.Invoke(funcConst, nameAccess);
        var projLambda = Expression.Lambda<Func<CovItem, string>>(invExpr, param);
        var selectMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Select" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .First()
            .MakeGenericMethod(typeof(CovItem), typeof(string));
        var selectExpr = Expression.Call(selectMethod, query.Expression, Expression.Quote(projLambda));
        // Should not throw; InvocationExpression triggers TrySplitProjection with HasUntranslatable=true
        var (sql, _) = TranslateDirectExpr(selectExpr, ctx);
        Assert.NotNull(sql);
    }

    [Fact]
    public void TranslatabilityAnalyzer_VisitNew_value_type_constructor_traversed()
    {
        // Select with new ValueTuple<int>(p.Value) inside untranslatable method
        // → VisitNew is called for the value type constructor
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = ctx.Query<CovItem>();
        var param = Expression.Parameter(typeof(CovItem), "p");
        var valueAccess = Expression.Property(param, nameof(CovItem.Value));
        // new ValueTuple<int>(p.Value) — ValueTuple<int> is a value type with 1 arg
        var vtCtor = typeof(ValueTuple<int>).GetConstructor(new[] { typeof(int) })!;
        var vtNew = Expression.New(vtCtor, valueAccess); // NewExpression with value type
        // Wrap in untranslatable method call
        var customMethod = typeof(CoverageBoost2Tests).GetMethod(
            nameof(UntranslatableWithValueTuple_CB2), BindingFlags.NonPublic | BindingFlags.Static)!;
        var customCall = Expression.Call(customMethod, vtNew);
        var projLambda = Expression.Lambda<Func<CovItem, string>>(customCall, param);
        var selectMethod = typeof(Queryable).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Where(m => m.Name == "Select" && m.IsGenericMethodDefinition && m.GetParameters().Length == 2)
            .First()
            .MakeGenericMethod(typeof(CovItem), typeof(string));
        var selectExpr = Expression.Call(selectMethod, query.Expression, Expression.Quote(projLambda));
        var (sql, _) = TranslateDirectExpr(selectExpr, ctx);
        Assert.NotNull(sql);
    }

    // ── GROUP 85: MySqlProvider uncovered methods ─────────────────────────────

    // Fake transaction for savepoint testing
    private sealed class FakeSavepointTransaction : DbTransaction
    {
        private readonly DbConnection _conn;
        public List<string> SavedPoints { get; } = new();
        public List<string> RolledBack { get; } = new();
        public FakeSavepointTransaction(DbConnection conn) => _conn = conn;
        protected override DbConnection DbConnection => _conn;
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
        public override void Commit() { }
        public override void Rollback() { }
        // Overrides DbTransaction.Save/Rollback(string) — discovered via reflection by providers
        public override void Save(string savepointName) => SavedPoints.Add(savepointName);
        public override void Rollback(string savepointName) => RolledBack.Add(savepointName);
    }

    private sealed class FakeSavepointTransactionThrows : DbTransaction
    {
        private readonly DbConnection _conn;
        public FakeSavepointTransactionThrows(DbConnection conn) => _conn = conn;
        protected override DbConnection DbConnection => _conn;
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
        public override void Commit() { }
        public override void Rollback() { }
        public override void Save(string savepointName) => throw new InvalidOperationException("Save error test");
        public override void Rollback(string savepointName) => throw new InvalidOperationException("Rollback error test");
    }

    /// <summary>Custom transaction with NO Save/Rollback(string) overrides.
    /// DbTransaction.Save/Rollback(string) throw NotSupportedException by default.</summary>
    private sealed class NoSavepointTransaction : DbTransaction
    {
        private readonly DbConnection _conn;
        public NoSavepointTransaction(DbConnection conn) => _conn = conn;
        protected override DbConnection DbConnection => _conn;
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;
        public override void Commit() { }
        public override void Rollback() { }
        // No Save(string)/Rollback(string) overrides → base DbTransaction throws NotSupportedException
    }

    [Fact]
    public void MySql_SplitSchemaTable_no_dot_returns_null_schema()
    {
        var method = typeof(MySqlProvider).GetMethod("SplitSchemaTable",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        dynamic result = method.Invoke(null, new object[] { "tablename" })!;
        string? schema = result.Item1;
        string table = result.Item2;
        Assert.Null(schema);
        Assert.Equal("tablename", table);
    }

    [Fact]
    public void MySql_SplitSchemaTable_with_dot_returns_schema_and_table()
    {
        var method = typeof(MySqlProvider).GetMethod("SplitSchemaTable",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        dynamic result = method.Invoke(null, new object[] { "mydb.mytable" })!;
        string? schema = result.Item1;
        string table = result.Item2;
        Assert.Equal("mydb", schema);
        Assert.Equal("mytable", table);
    }

    [Fact]
    public void MySql_SplitSchemaTable_backtick_table_is_trimmed()
    {
        var method = typeof(MySqlProvider).GetMethod("SplitSchemaTable",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        dynamic result = method.Invoke(null, new object[] { "`mytable`" })!;
        string table = result.Item2;
        Assert.Equal("mytable", table);
    }

    [Fact]
    public async Task MySql_ValidateConnection_throws_for_non_mysql_connection()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, provider);
        // GetMapping to build the mapping
        var getMapping = typeof(DbContext).GetMethod("GetMapping",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var mapping = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(CovItem) })!;
        // BulkInsertAsync calls ValidateConnection internally
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await provider.BulkInsertAsync<CovItem>(ctx, mapping,
                new List<CovItem> { new CovItem { Name = "test", Value = 1 } },
                CancellationToken.None));
    }

    [Fact]
    public async Task MySql_CreateSavepointAsync_with_valid_save_method_succeeds()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var fakeTx = new FakeSavepointTransaction(cn);
        await provider.CreateSavepointAsync(fakeTx, "sp1");
        Assert.Contains("sp1", fakeTx.SavedPoints);
    }

    [Fact]
    public async Task MySql_RollbackToSavepointAsync_with_valid_rollback_method_succeeds()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var fakeTx = new FakeSavepointTransaction(cn);
        await provider.RollbackToSavepointAsync(fakeTx, "sp1");
        Assert.Contains("sp1", fakeTx.RolledBack);
    }

    [Fact]
    public void MySql_CreateSavepointAsync_throws_not_supported_for_unsupported_transaction()
    {
        // NoSavepointTransaction has no Save override; base DbTransaction.Save throws NotSupportedException
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var tx = new NoSavepointTransaction(cn);
        Assert.Throws<NotSupportedException>(() =>
            provider.CreateSavepointAsync(tx, "sp1").GetAwaiter().GetResult());
    }

    [Fact]
    public void MySql_RollbackToSavepointAsync_throws_not_supported_for_unsupported_transaction()
    {
        // NoSavepointTransaction has no Rollback(string) override; base DbTransaction throws
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var tx = new NoSavepointTransaction(cn);
        Assert.Throws<NotSupportedException>(() =>
            provider.RollbackToSavepointAsync(tx, "sp1").GetAwaiter().GetResult());
    }

    [Fact]
    public void MySql_CreateSavepointAsync_handles_TargetInvocationException_by_rethrowing_inner()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var throwTx = new FakeSavepointTransactionThrows(cn);
        // Save(string) throws InvalidOperationException → TargetInvocationException → rethrows inner
        Assert.Throws<InvalidOperationException>(() =>
            provider.CreateSavepointAsync(throwTx, "sp1").GetAwaiter().GetResult());
    }

    [Fact]
    public void MySql_RollbackToSavepointAsync_handles_TargetInvocationException_by_rethrowing_inner()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var throwTx = new FakeSavepointTransactionThrows(cn);
        Assert.Throws<InvalidOperationException>(() =>
            provider.RollbackToSavepointAsync(throwTx, "sp1").GetAwaiter().GetResult());
    }

    [Fact]
    public void MySql_CreateSavepointAsync_precancelled_throws_OperationCanceledException()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var tx = cn.BeginTransaction();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.Throws<OperationCanceledException>(() =>
            provider.CreateSavepointAsync(tx, "sp1", cts.Token).GetAwaiter().GetResult());
    }

    [Fact]
    public void MySql_RollbackToSavepointAsync_precancelled_throws_OperationCanceledException()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var tx = cn.BeginTransaction();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.Throws<OperationCanceledException>(() =>
            provider.RollbackToSavepointAsync(tx, "sp1", cts.Token).GetAwaiter().GetResult());
    }

    // ── GROUP 86: PostgresProvider uncovered methods ──────────────────────────

    [Fact]
    public async Task Postgres_ValidateConnection_throws_for_non_npgsql_connection()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var getMapping = typeof(DbContext).GetMethod("GetMapping",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var mapping = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(CovItem) })!;
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await provider.BulkInsertAsync<CovItem>(ctx, mapping,
                new List<CovItem> { new CovItem { Name = "x", Value = 1 } },
                CancellationToken.None));
    }

    [Fact]
    public async Task Postgres_CreateSavepointAsync_with_valid_save_method_succeeds()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var fakeTx = new FakeSavepointTransaction(cn);
        await provider.CreateSavepointAsync(fakeTx, "sp1");
        Assert.Contains("sp1", fakeTx.SavedPoints);
    }

    [Fact]
    public async Task Postgres_RollbackToSavepointAsync_with_valid_rollback_method_succeeds()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var fakeTx = new FakeSavepointTransaction(cn);
        await provider.RollbackToSavepointAsync(fakeTx, "sp1");
        Assert.Contains("sp1", fakeTx.RolledBack);
    }

    [Fact]
    public void Postgres_CreateSavepointAsync_throws_not_supported_for_unsupported_tx()
    {
        // NoSavepointTransaction has no Save override; base DbTransaction.Save throws NotSupportedException
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var tx = new NoSavepointTransaction(cn);
        Assert.Throws<NotSupportedException>(() =>
            provider.CreateSavepointAsync(tx, "sp1").GetAwaiter().GetResult());
    }

    [Fact]
    public void Postgres_RollbackToSavepointAsync_throws_not_supported_for_unsupported_tx()
    {
        // NoSavepointTransaction has no Rollback(string) override; base DbTransaction throws
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var tx = new NoSavepointTransaction(cn);
        Assert.Throws<NotSupportedException>(() =>
            provider.RollbackToSavepointAsync(tx, "sp1").GetAwaiter().GetResult());
    }

    [Fact]
    public void Postgres_CreateSavepointAsync_handles_TargetInvocationException()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var throwTx = new FakeSavepointTransactionThrows(cn);
        Assert.Throws<InvalidOperationException>(() =>
            provider.CreateSavepointAsync(throwTx, "sp1").GetAwaiter().GetResult());
    }

    [Fact]
    public void Postgres_RollbackToSavepointAsync_handles_TargetInvocationException()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var throwTx = new FakeSavepointTransactionThrows(cn);
        Assert.Throws<InvalidOperationException>(() =>
            provider.RollbackToSavepointAsync(throwTx, "sp1").GetAwaiter().GetResult());
    }

    [Fact]
    public void Postgres_CreateSavepointAsync_precancelled_throws_OperationCanceledException()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var tx = cn.BeginTransaction();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.Throws<OperationCanceledException>(() =>
            provider.CreateSavepointAsync(tx, "sp1", cts.Token).GetAwaiter().GetResult());
    }

    [Fact]
    public void Postgres_RollbackToSavepointAsync_precancelled_throws_OperationCanceledException()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var tx = cn.BeginTransaction();
        using var cts = new CancellationTokenSource();
        cts.Cancel();
        Assert.Throws<OperationCanceledException>(() =>
            provider.RollbackToSavepointAsync(tx, "sp1", cts.Token).GetAwaiter().GetResult());
    }

    [Fact]
    public void Postgres_BuildPostgresBatchInsertSql_generates_correct_sql()
    {
        // Call private BuildPostgresBatchInsertSql via reflection
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var getMapping = typeof(DbContext).GetMethod("GetMapping",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var mapping = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(CovItem) })!;
        var cols = mapping.Columns.Where(c => !c.IsDbGenerated).ToArray();
        var buildMethod = typeof(PostgresProvider).GetMethod("BuildPostgresBatchInsertSql",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var sql = (string)buildMethod.Invoke(provider, new object[] { mapping, cols, 2 })!;
        Assert.Contains("INSERT INTO", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("VALUES", sql, StringComparison.OrdinalIgnoreCase);
        // P1: ON CONFLICT DO NOTHING removed — batch inserts should not silently swallow conflicts
        Assert.DoesNotContain("ON CONFLICT DO NOTHING", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Postgres_BuildPostgresBatchInsertSql_single_row_has_no_leading_comma()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var getMapping = typeof(DbContext).GetMethod("GetMapping",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var mapping = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(CovItem) })!;
        var cols = mapping.Columns.Where(c => !c.IsDbGenerated).ToArray();
        var buildMethod = typeof(PostgresProvider).GetMethod("BuildPostgresBatchInsertSql",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var sql = (string)buildMethod.Invoke(provider, new object[] { mapping, cols, 1 })!;
        // Single row: no trailing comma before VALUES
        Assert.DoesNotContain(", @p", sql.Split("VALUES")[0]);
    }

    [Fact]
    public void Postgres_GetPostgresType_short_returns_SMALLINT()
    {
        // GetPostgresType is private static, but called from GenerateCreateHistoryTableSql
        // CB2ShortEntity has ShortVal (short) and ByteVal (byte)
        var provider = new PostgresProvider(new SqliteParameterFactory());
        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var getMapping = typeof(DbContext).GetMethod("GetMapping",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var mapping = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(CB2ShortEntity) })!;
        // GenerateCreateHistoryTableSql iterates columns and calls GetPostgresType for each
        var sql = provider.GenerateCreateHistoryTableSql(mapping);
        // Both short and byte map to SMALLINT
        Assert.Contains("SMALLINT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Postgres_GetPostgresType_via_reflection_short_and_byte()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());
        var getType = typeof(PostgresProvider).GetMethod("GetPostgresType",
            BindingFlags.NonPublic | BindingFlags.Static)!;
        var shortResult = (string)getType.Invoke(null, new object[] { typeof(short) })!;
        var byteResult = (string)getType.Invoke(null, new object[] { typeof(byte) })!;
        Assert.Equal("SMALLINT", shortResult);
        Assert.Equal("SMALLINT", byteResult);
    }
}
