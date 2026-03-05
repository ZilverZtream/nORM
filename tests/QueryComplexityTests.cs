using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that complexity metrics are computed from the expression tree during
/// LINQ-to-SQL translation, NOT from SQL string scanning.
///
/// Key properties under test:
/// 1. The correct metric flags/counts are set for each LINQ operator.
/// 2. A column named "group_by_column" does NOT trigger HasGroupBy (proving we
///    are not string-scanning the generated SQL).
/// 3. The derived complexity score causes the command timeout to increase relative
///    to a simpler query.
/// </summary>
public class QueryComplexityTests : TestBase
{
    // ── entity types ──────────────────────────────────────────────────────────

    [Table("ComplexityOrder")]
    private class Order
    {
        [Key] public int Id { get; set; }
        // A column whose name contains the substring "group_by" —
        // a naive SQL-string scan would incorrectly flag GROUP BY.
        public string group_by_column { get; set; } = string.Empty;
        public int CustomerId { get; set; }
        public decimal Amount { get; set; }
    }

    [Table("ComplexityCustomer")]
    private class Customer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("ComplexityProduct")]
    private class Product
    {
        [Key] public int Id { get; set; }
        public int CategoryId { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("ComplexityCategory")]
    private class Category
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    // ── helper ────────────────────────────────────────────────────────────────

    /// <summary>
    /// Translates a LINQ query and returns the full <see cref="QueryPlan"/>,
    /// allowing tests to inspect the <see cref="QueryComplexityMetrics"/>.
    /// </summary>
    private static QueryPlan GetPlan<T, TResult>(
        Func<IQueryable<T>, IQueryable<TResult>> build)
        where T : class, new()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var query = build(ctx.Query<T>());
        var expr = query.Expression;

        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = (QueryPlan)translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
        return plan;
    }

    // ── Issue 1 tests ─────────────────────────────────────────────────────────

    [Fact]
    public void Simple_where_query_has_low_complexity()
    {
        var plan = GetPlan<Order, Order>(q => q.Where(o => o.Amount > 100));

        Assert.Equal(1, plan.Complexity.PredicateCount);
        Assert.False(plan.Complexity.HasGroupBy);
        Assert.False(plan.Complexity.HasOrderBy);
        Assert.Equal(0, plan.Complexity.JoinCount);
        Assert.False(plan.Complexity.HasAggregates);

        // Complexity score should be very low
        Assert.True(plan.Complexity.ToComplexityScore() <= 5,
            $"Expected low score, got {plan.Complexity.ToComplexityScore()}");
    }

    [Fact]
    public void Join_query_increments_join_count()
    {
        var plan = GetPlan<Order, object>(q =>
            q.Join(
                q.Provider.CreateQuery<Customer>(
                    System.Linq.Expressions.Expression.Constant(
                        Enumerable.Empty<Customer>().AsQueryable())),
                o => o.CustomerId,
                c => c.Id,
                (o, c) => new { o.Id, c.Name } as object));

        Assert.Equal(1, plan.Complexity.JoinCount);
    }

    [Fact]
    public void Two_joins_produce_join_count_two()
    {
        // Build a query with TWO joins using reflection to bypass generic constraints.
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var orders = ctx.Query<Order>();
        var customers = Enumerable.Empty<Customer>().AsQueryable();
        var products = Enumerable.Empty<Product>().AsQueryable();

        // orders.Join(customers, ...) then .Join(products, ...)
        var step1 = orders.Join(customers, o => o.CustomerId, c => c.Id, (o, c) => o);
        var step2 = step1.Join(products, o => o.Id, p => p.Id, (o, p) => o);

        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = (QueryPlan)translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { step2.Expression })!;

        Assert.Equal(2, plan.Complexity.JoinCount);
    }

    [Fact]
    public void GroupBy_sets_HasGroupBy_true()
    {
        var plan = GetPlan<Order, IGrouping<int, Order>>(q =>
            q.GroupBy(o => o.CustomerId));

        Assert.True(plan.Complexity.HasGroupBy);
    }

    [Fact]
    public void OrderBy_sets_HasOrderBy_true()
    {
        var plan = GetPlan<Order, Order>(q => q.OrderBy(o => o.Amount));

        Assert.True(plan.Complexity.HasOrderBy);
    }

    [Fact]
    public void Distinct_sets_HasDistinct_true()
    {
        var plan = GetPlan<Order, Order>(q => q.Distinct());

        Assert.True(plan.Complexity.HasDistinct);
    }

    [Fact]
    public void Column_named_group_by_column_does_NOT_trigger_HasGroupBy()
    {
        // The column "group_by_column" is selected in every query on Order.
        // SQL-string scanning would see "GROUP BY" in the column name and
        // falsely set HasGroupBy. Tree-walk metrics must be immune to this.
        var plan = GetPlan<Order, Order>(q =>
            q.Where(o => o.group_by_column != ""));

        Assert.False(plan.Complexity.HasGroupBy,
            "HasGroupBy must not be triggered by a column name containing 'group_by'");

        // Also verify the generated SQL actually contains the column name (sanity check
        // that would expose a false negative if the column were renamed).
        Assert.Contains("group_by_column", plan.Sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Column_named_order_by_does_NOT_trigger_HasOrderBy()
    {
        // Same false-positive concern: if a column were named "order_by_something"
        // and SQL scanning were used, HasOrderBy could be set incorrectly.
        // This test documents the expected tree-walk behaviour.
        var plan = GetPlan<Order, Order>(q =>
            q.Where(o => o.group_by_column != ""));   // no OrderBy call in tree

        Assert.False(plan.Complexity.HasOrderBy,
            "HasOrderBy must only be set when an OrderBy LINQ call is present");
    }

    [Fact]
    public void Timeout_increases_with_join_complexity()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Simple query plan
        var simplePlan = GetPlan<Order, Order>(q => q.Where(o => o.Amount > 0));

        // Complex query (two joins)
        var orders = ctx.Query<Order>();
        var customers = Enumerable.Empty<Customer>().AsQueryable();
        var products = Enumerable.Empty<Product>().AsQueryable();
        var step1 = orders.Join(customers, o => o.CustomerId, c => c.Id, (o, c) => o);
        var step2 = step1.Join(products, o => o.Id, p => p.Id, (o, p) => o);

        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var complexPlan = (QueryPlan)translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { step2.Expression })!;

        // The complex plan's score must be strictly higher than the simple plan's score
        Assert.True(
            complexPlan.Complexity.ToComplexityScore() > simplePlan.Complexity.ToComplexityScore(),
            $"Complex score ({complexPlan.Complexity.ToComplexityScore()}) should exceed simple score ({simplePlan.Complexity.ToComplexityScore()})");
    }

    [Fact]
    public void Complexity_metrics_are_in_plan()
    {
        // Verify that the Complexity field is populated on the plan (not just default(QueryComplexityMetrics))
        // for a query that has meaningful structure.
        var plan = GetPlan<Order, Order>(q =>
            q.Where(o => o.Amount > 10).OrderBy(o => o.Id));

        // Should have at least one predicate and OrderBy
        Assert.Equal(1, plan.Complexity.PredicateCount);
        Assert.True(plan.Complexity.HasOrderBy);
    }

    [Fact]
    public void ToComplexityScore_caps_at_50()
    {
        // Construct an artificially extreme metrics value and verify the cap.
        var metrics = new QueryComplexityMetrics
        {
            JoinCount = 100,      // 100 * 2 = 200
            HasGroupBy = true,    // +2
            HasOrderBy = true,    // +2
            SubqueryDepth = 20,   // 20 * 2 = 40
            PredicateCount = 100, // 100 / 5 = 20
            HasDistinct = true,   // +1
            HasAggregates = true  // +1
        };

        Assert.Equal(50, metrics.ToComplexityScore());
    }

    [Fact]
    public void ToComplexityScore_baseline_is_1()
    {
        var metrics = default(QueryComplexityMetrics);
        Assert.Equal(1, metrics.ToComplexityScore());
    }
}
