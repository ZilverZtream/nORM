using System;
using System.Linq;
using System.Reflection;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

//<summary>
//Tests for QueryTranslator recursion depth limiting and the configurable MaxRecursionDepth.
//</summary>
public class QueryTranslatorRecursionTests : TestBase
{
    private class Item
    {
        public int Id { get; set; }
    }

    private static IQueryable<Item> BuildNestedUnion(IQueryable<Item> source, int depth)
    {
        if (depth <= 0) return source;
        return source.Union(BuildNestedUnion(source, depth - 1));
    }

 // ─── Original regression test ─────────────────────────────────────────

    [Fact]
    public void Deeply_nested_unions_throw()
    {
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var ex = Assert.Throws<TargetInvocationException>(() =>
            TranslateQuery<Item, Item>(q => BuildNestedUnion(q, 110), connection, provider));
        Assert.IsType<NormQueryException>(ex.InnerException);
    }

 // ─── Default limit = 50, error message mentions MaxRecursionDepth ─

    [Fact]
    public void Default_limit_is_50_not_30()
    {
 // Default MaxRecursionDepth is now 50 (raised from 30).
 // Depth 28 should succeed without throwing.
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;

 // Should NOT throw — depth 28 is below both old (30) and new (50) limits
        var (sql, _, _) = TranslateQuery<Item, Item>(q => BuildNestedUnion(q, 28), connection, provider);
        Assert.NotEmpty(sql);
    }

    [Fact]
    public void Depth_31_ExceedsOldLimit_ButSucceedsWithNewDefault50()
    {
 // Depth 31 exceeded the old hardcoded limit of 30, but new default is 50.
 // This should now succeed.
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Item, Item>(q => BuildNestedUnion(q, 31), connection, provider);
        Assert.NotEmpty(sql);
    }

    [Fact]
    public void Default_depth_51_throws_NormQueryException()
    {
 // Depth 51 exceeds the new default limit of 50.
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var ex = Assert.Throws<TargetInvocationException>(() =>
            TranslateQuery<Item, Item>(q => BuildNestedUnion(q, 51), connection, provider));
        var inner = Assert.IsType<NormQueryException>(ex.InnerException);
 // Error message must mention MaxRecursionDepth so users know how to fix it
        Assert.Contains("MaxRecursionDepth", inner.Message);
        Assert.Contains("50", inner.Message);
    }

 // ─── Configurable MaxRecursionDepth via DbContextOptions ────────

    [Fact]
    public void Custom_MaxRecursionDepth_60_Allows_Depth55()
    {
 // Configure MaxRecursionDepth = 60 → depth 55 should succeed.
        var options = new DbContextOptions { MaxRecursionDepth = 60 };
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        using var ctx = new DbContext(connection, setup.Provider, options);

 // Build the query manually using the custom-options context
        var query = BuildNestedUnion(ctx.Query<Item>(), 55);
        var expr = query.Expression;
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        Assert.NotEmpty(sql);
    }

    [Fact]
    public void Custom_MaxRecursionDepth_60_Rejects_Depth65()
    {
 // Configure MaxRecursionDepth = 60 → depth 65 should throw.
        var options = new DbContextOptions { MaxRecursionDepth = 60 };
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        using var ctx = new DbContext(connection, setup.Provider, options);

        var query = BuildNestedUnion(ctx.Query<Item>(), 65);
        var expr = query.Expression;
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;

        var ex = Assert.Throws<TargetInvocationException>(() =>
            translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr }));
        var inner = Assert.IsType<NormQueryException>(ex.InnerException);
        Assert.Contains("60", inner.Message);
        Assert.Contains("MaxRecursionDepth", inner.Message);
    }

    [Fact]
    public void MaxRecursionDepth_OutOfRange_Throws_ArgumentOutOfRangeException()
    {
 // MaxRecursionDepth must be between 1 and 200.
        var options = new DbContextOptions();
        Assert.Throws<ArgumentOutOfRangeException>(() => options.MaxRecursionDepth = 0);
        Assert.Throws<ArgumentOutOfRangeException>(() => options.MaxRecursionDepth = 201);
    }

    [Fact]
    public void MaxRecursionDepth_Valid_Boundary_Values()
    {
 // Values at the valid boundary should be accepted.
        var options = new DbContextOptions();
        options.MaxRecursionDepth = 1;
        Assert.Equal(1, options.MaxRecursionDepth);
        options.MaxRecursionDepth = 200;
        Assert.Equal(200, options.MaxRecursionDepth);
        options.MaxRecursionDepth = 50; // restore default
        Assert.Equal(50, options.MaxRecursionDepth);
    }

 // ─── Recursion via BuildExists (Any() subquery path) ────────────

    [Fact]
    public void Any_inside_Where_routes_through_BuildExists_and_is_translated()
    {
 // BuildExists creates a sub-translator — verifies basic translation succeeds.
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, _, _) = TranslateQuery<Item, Item>(
            q => q.Where(x => q.Any(y => y.Id == x.Id)),
            connection, provider);
        Assert.Contains("EXISTS", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Deeply_nested_Any_inside_Where_exceeds_depth_limit()
    {
 // Each nested Any() creates a sub-translator via BuildExists at depth+1.
 // With MaxRecursionDepth=2, three levels of Any() should exceed the limit.
        var options = new DbContextOptions { MaxRecursionDepth = 2 };
        var setup = CreateProvider(ProviderKind.Sqlite);
        using var connection = setup.Connection;
        using var ctx = new DbContext(connection, setup.Provider, options);

 // Build query: outer.Any(a => inner.Any(b => inner.Any(c => c.Id == a.Id)))
 // Each Any goes through BuildExists, incrementing depth by 1.
        var inner = ctx.Query<Item>();
        var query = inner.Where(x =>
            inner.Any(a =>
                inner.Any(b =>
                    inner.Any(c => c.Id == x.Id))));

        var expr = query.Expression;
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;

 // With MaxRecursionDepth=2, this should throw NormQueryException due to depth exceeded.
        var ex = Assert.Throws<System.Reflection.TargetInvocationException>(() =>
            translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr }));
        Assert.IsType<NormQueryException>(ex.InnerException);
        Assert.Contains("MaxRecursionDepth", ex.InnerException!.Message);
    }
}
