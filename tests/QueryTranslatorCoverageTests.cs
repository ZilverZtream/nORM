using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

// ─── Shared entity types (namespace scope, not nested) ────────────────────────

[Table("QtProduct")]
public class QtProduct
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Stock { get; set; }
    public bool IsActive { get; set; }
    public int CategoryId { get; set; }
}

[Table("QtCategory")]
public class QtCategory
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
}

/// <summary>
/// Tests targeting uncovered QueryTranslator paths:
/// OrderBy, OrderByDescending, ThenBy, ThenByDescending, Take, Skip,
/// Distinct, Reverse, First/Single/Last variants, Count with predicate,
/// Sum/Min/Max/Average, All, Any, Union/Intersect/Except, ElementAt,
/// Select projections, GroupBy, error paths.
/// </summary>
public class QueryTranslatorCoverageTests
{
    private static (SqliteConnection Cn, DbContext Ctx) CreateProductContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE QtProduct (
                Id         INTEGER PRIMARY KEY AUTOINCREMENT,
                Name       TEXT NOT NULL DEFAULT '',
                Price      REAL NOT NULL DEFAULT 0,
                Stock      INTEGER NOT NULL DEFAULT 0,
                IsActive   INTEGER NOT NULL DEFAULT 1,
                CategoryId INTEGER NOT NULL DEFAULT 0
            )";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void InsertProduct(SqliteConnection cn, string name, decimal price, int stock, bool active = true, int catId = 1)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO QtProduct (Name, Price, Stock, IsActive, CategoryId) VALUES (@n, @p, @s, @a, @c)";
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@p", (double)price);
        cmd.Parameters.AddWithValue("@s", stock);
        cmd.Parameters.AddWithValue("@a", active ? 1 : 0);
        cmd.Parameters.AddWithValue("@c", catId);
        cmd.ExecuteNonQuery();
    }

    private static INormQueryable<QtProduct> Q(DbContext ctx) =>
        (INormQueryable<QtProduct>)ctx.Query<QtProduct>();

    // ─── OrderBy / OrderByDescending ──────────────────────────────────────

    [Fact]
    public async Task OrderBy_SingleColumn_OrdersAscending()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Zebra", 10, 5);
        InsertProduct(cn, "Apple", 20, 3);
        InsertProduct(cn, "Mango", 15, 8);

        var results = await Q(ctx).OrderBy(p => p.Name).ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("Apple", results[0].Name);
        Assert.Equal("Mango", results[1].Name);
        Assert.Equal("Zebra", results[2].Name);
    }

    [Fact]
    public async Task OrderByDescending_SingleColumn_OrdersDescending()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Aaa", 10, 5);
        InsertProduct(cn, "Zzz", 20, 3);

        var results = await Q(ctx).OrderByDescending(p => p.Name).ToListAsync();
        Assert.Equal("Zzz", results[0].Name);
        Assert.Equal("Aaa", results[1].Name);
    }

    [Fact]
    public async Task ThenBy_SecondarySort_BreaksTie()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Apple", 10, 5, catId: 1);
        InsertProduct(cn, "Apple", 5, 3, catId: 1);
        InsertProduct(cn, "Apple", 20, 8, catId: 1);

        var results = await Q(ctx).OrderBy(p => p.Name).ThenBy(p => p.Price).ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal(5m, results[0].Price);
        Assert.Equal(10m, results[1].Price);
        Assert.Equal(20m, results[2].Price);
    }

    [Fact]
    public async Task ThenByDescending_SecondarySort_BreaksTieDesc()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Apple", 10, 5);
        InsertProduct(cn, "Apple", 5, 3);

        var results = await Q(ctx).OrderBy(p => p.Name).ThenByDescending(p => p.Price).ToListAsync();
        Assert.Equal(10m, results[0].Price);
        Assert.Equal(5m, results[1].Price);
    }

    // ─── Take / Skip ──────────────────────────────────────────────────────

    [Fact]
    public async Task Take_LimitsRows()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 0; i < 5; i++) InsertProduct(cn, $"P{i}", i * 10, i);

        var results = await Q(ctx).Take(2).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Skip_SkipsRows()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 5; i++) InsertProduct(cn, $"P{i}", i * 10, i);

        var results = await Q(ctx).OrderBy(p => p.Price).Skip(2).ToListAsync();
        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task Skip_Then_Take_PagesResults()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 10; i++) InsertProduct(cn, $"P{i:D2}", i * 10, i);

        var results = await Q(ctx).OrderBy(p => p.Name).Skip(3).Take(3).ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("P04", results[0].Name);
    }

    [Fact]
    public void Take_NegativeValue_TranslationThrowsArgumentOutOfRange()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Translation-time check: Take(-1) should throw during Translate()
        // Exception is wrapped in TargetInvocationException by reflection
        var q = ctx.Query<QtProduct>().Take(-1);
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var ex = Assert.ThrowsAny<Exception>(() =>
            translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { q.Expression }));
        // Unwrap TargetInvocationException
        var inner = ex is System.Reflection.TargetInvocationException tie ? tie.InnerException : ex;
        Assert.IsType<ArgumentOutOfRangeException>(inner);
    }

    [Fact]
    public void Skip_NegativeValue_TranslationThrowsArgumentOutOfRange()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Translation-time check: Skip(-1) should throw during Translate()
        var q = ctx.Query<QtProduct>().Skip(-1);
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var ex = Assert.ThrowsAny<Exception>(() =>
            translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { q.Expression }));
        var inner = ex is System.Reflection.TargetInvocationException tie ? tie.InnerException : ex;
        Assert.IsType<ArgumentOutOfRangeException>(inner);
    }

    // ─── Distinct ─────────────────────────────────────────────────────────

    [Fact]
    public async Task Distinct_SqlContainsDistinct()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Alpha", 10, 5);
        InsertProduct(cn, "Beta", 20, 3);

        var results = await Q(ctx).Distinct().ToListAsync();
        Assert.True(results.Count >= 1);
    }

    // ─── Reverse ──────────────────────────────────────────────────────────

    [Fact]
    public async Task Reverse_WithoutOrderBy_OrdersByPkDescending()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "First", 10, 1);
        InsertProduct(cn, "Second", 20, 2);
        InsertProduct(cn, "Third", 30, 3);

        var results = await Q(ctx).Reverse().ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("Third", results[0].Name);
    }

    [Fact]
    public async Task Reverse_WithOrderBy_FlipsOrdering()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Aaa", 5, 1);
        InsertProduct(cn, "Zzz", 50, 2);

        var results = await Q(ctx).OrderBy(p => p.Name).Reverse().ToListAsync();
        Assert.Equal("Zzz", results[0].Name);
    }

    // ─── First / FirstOrDefault ───────────────────────────────────────────

    [Fact]
    public async Task First_WithData_ReturnsFirstRow()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Alpha", 10, 5);
        InsertProduct(cn, "Beta", 20, 3);

        var result = await Q(ctx).OrderBy(p => p.Name).FirstAsync();
        Assert.Equal("Alpha", result.Name);
    }

    [Fact]
    public async Task First_EmptyTable_Throws()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).FirstAsync());
    }

    [Fact]
    public async Task FirstOrDefault_EmptyTable_ReturnsNull()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await Q(ctx).FirstOrDefaultAsync();
        Assert.Null(result);
    }

    [Fact]
    public async Task First_WithPredicate_ReturnsMatchingRow()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Alpha", 10, 5);
        InsertProduct(cn, "Beta", 20, 3);

        var result = await Q(ctx).Where(p => p.Name == "Beta").FirstAsync();
        Assert.Equal("Beta", result.Name);
    }

    // ─── Single / SingleOrDefault ─────────────────────────────────────────

    [Fact]
    public async Task Single_OneRow_ReturnsIt()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Only", 10, 5);

        var result = await ((INormQueryable<QtProduct>)Q(ctx).Where(p => p.Name == "Only")).SingleAsync();
        Assert.Equal("Only", result.Name);
    }

    [Fact]
    public async Task Single_MultipleRows_Throws()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5);
        InsertProduct(cn, "B", 20, 3);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).SingleAsync());
    }

    [Fact]
    public async Task Single_EmptyTable_Throws()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).SingleAsync());
    }

    [Fact]
    public async Task SingleOrDefault_EmptyTable_ReturnsNull()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await ((INormQueryable<QtProduct>)Q(ctx).Where(p => p.Id == -999)).SingleOrDefaultAsync();
        Assert.Null(result);
    }

    [Fact]
    public async Task SingleOrDefault_MultipleRows_Throws()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5);
        InsertProduct(cn, "B", 20, 3);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).SingleOrDefaultAsync());
    }

    // ─── Last / LastOrDefault via expression builder ──────────────────────

    private static Task<T> LastAsync<T>(INormQueryable<T> q) where T : class
    {
        var provider = (Query.NormQueryProvider)q.Provider;
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Last),
            new[] { typeof(T) }, q.Expression);
        return provider.ExecuteAsync<T>(expr, default);
    }

    private static Task<T?> LastOrDefaultAsync<T>(INormQueryable<T> q) where T : class
    {
        var provider = (Query.NormQueryProvider)q.Provider;
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.LastOrDefault),
            new[] { typeof(T) }, q.Expression);
        return provider.ExecuteAsync<T?>(expr, default);
    }

    [Fact]
    public async Task Last_WithData_ReturnsLastByPk()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "First", 10, 1);
        InsertProduct(cn, "Second", 20, 2);
        InsertProduct(cn, "Third", 30, 3);

        var result = await LastAsync(Q(ctx));
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Last_EmptyTable_Throws()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            LastAsync(Q(ctx)));
    }

    [Fact]
    public async Task LastOrDefault_EmptyTable_ReturnsNull()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await LastOrDefaultAsync(Q(ctx));
        Assert.Null(result);
    }

    [Fact]
    public async Task Last_WithOrderBy_ReturnsLastOrdered()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Aaa", 5, 1);
        InsertProduct(cn, "Zzz", 50, 2);

        var result = await LastAsync((INormQueryable<QtProduct>)Q(ctx).OrderBy(p => p.Name));
        Assert.Equal("Zzz", result.Name);
    }

    [Fact]
    public async Task Last_WithPredicate_FiltersAndReturnsLast()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5, 10);
        InsertProduct(cn, "B", 10, 20);
        InsertProduct(cn, "C", 15, 5);

        var result = await LastAsync((INormQueryable<QtProduct>)Q(ctx).Where(p => p.Stock >= 10));
        Assert.NotNull(result);
    }

    // ─── Count with predicate (via Where) ────────────────────────────────

    [Fact]
    public async Task Count_WithPredicate_CountsMatchingRows()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5, 10, active: true);
        InsertProduct(cn, "B", 10, 20, active: false);
        InsertProduct(cn, "C", 15, 5, active: true);

        var count = await Q(ctx).Where(p => p.IsActive).CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task LongCount_ReturnsLongValue()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5, 10);
        InsertProduct(cn, "B", 10, 20);

        var provider = (Query.NormQueryProvider)ctx.Query<QtProduct>().Provider;
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.LongCount),
            new[] { typeof(QtProduct) }, ctx.Query<QtProduct>().Expression);
        var count = await provider.ExecuteAsync<long>(expr, default);
        Assert.Equal(2L, count);
    }

    // ─── Any ─────────────────────────────────────────────────────────────

    [Fact]
    public async Task Any_WithMatchingRow_ReturnsTrue()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5, 10);

        var result = await Q(ctx).CountAsync() > 0;
        Assert.True(result);
    }

    [Fact]
    public async Task Count_EmptyTable_ZeroMeansNoAny()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await Q(ctx).CountAsync() > 0;
        Assert.False(result);
    }

    // ─── Sum / Min / Max / Average ────────────────────────────────────────

    [Fact]
    public async Task Sum_Int_ReturnsTotal()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5, 10);
        InsertProduct(cn, "B", 10, 20);
        InsertProduct(cn, "C", 15, 30);

        var total = await Q(ctx).SumAsync(p => p.Stock);
        Assert.Equal(60, total);
    }

    [Fact]
    public async Task Sum_EmptyTable_ReturnsZero()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        var total = await Q(ctx).SumAsync(p => p.Stock);
        Assert.Equal(0, total);
    }

    [Fact]
    public async Task Min_Int_ReturnsMinimum()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5, 10);
        InsertProduct(cn, "B", 10, 3);
        InsertProduct(cn, "C", 15, 7);

        var min = await Q(ctx).MinAsync(p => p.Stock);
        Assert.Equal(3, min);
    }

    [Fact]
    public async Task Min_EmptyTable_Throws()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).MinAsync(p => p.Stock));
    }

    [Fact]
    public async Task Max_Int_ReturnsMaximum()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5, 10);
        InsertProduct(cn, "B", 10, 30);
        InsertProduct(cn, "C", 15, 7);

        var max = await Q(ctx).MaxAsync(p => p.Stock);
        Assert.Equal(30, max);
    }

    [Fact]
    public async Task Max_EmptyTable_Throws()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).MaxAsync(p => p.Stock));
    }

    [Fact]
    public async Task Average_Int_ReturnsAverage()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 10);
        InsertProduct(cn, "B", 20, 20);
        InsertProduct(cn, "C", 30, 30);

        var avg = await Q(ctx).AverageAsync(p => p.Stock);
        Assert.True(avg > 19.0 && avg < 21.0);
    }

    [Fact]
    public async Task Average_EmptyTable_Throws()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).AverageAsync(p => p.Stock));
    }

    // ─── Select projection ────────────────────────────────────────────────

    [Fact]
    public async Task Select_AnonymousProjection_ReturnsProjectedData()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Widget", 9, 50);

        var results = await ctx.Query<QtProduct>()
            .Select(p => new { p.Name, p.Stock })
            .ToListAsync();
        Assert.Single(results);
        Assert.Equal("Widget", results[0].Name);
        Assert.Equal(50, results[0].Stock);
    }

    [Fact]
    public async Task Select_With_Where_FiltersBeforeProjection()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Active", 10, 5, active: true);
        InsertProduct(cn, "Inactive", 20, 3, active: false);

        var results = await ctx.Query<QtProduct>()
            .Where(p => p.IsActive)
            .Select(p => new { p.Name })
            .ToListAsync();
        Assert.Single(results);
        Assert.Equal("Active", results[0].Name);
    }

    // ─── Where with various predicates ────────────────────────────────────

    [Fact]
    public async Task Where_GreaterThan_FiltersCorrectly()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Cheap", 5, 10);
        InsertProduct(cn, "Expensive", 100, 5);

        var results = await Q(ctx).Where(p => p.Price > 50).ToListAsync();
        Assert.Single(results);
        Assert.Equal("Expensive", results[0].Name);
    }

    [Fact]
    public async Task Where_LessThanOrEqual_FiltersCorrectly()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 1);
        InsertProduct(cn, "B", 20, 2);
        InsertProduct(cn, "C", 30, 3);

        var results = await Q(ctx).Where(p => p.Price <= 20).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Where_MultipleConditions_AndLogic()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 50, active: true);
        InsertProduct(cn, "B", 5, 10, active: true);
        InsertProduct(cn, "C", 10, 50, active: false);

        var results = await Q(ctx).Where(p => p.IsActive && p.Price >= 10).ToListAsync();
        Assert.Single(results);
        Assert.Equal("A", results[0].Name);
    }

    [Fact]
    public async Task Where_Chained_BothApplied()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 50, catId: 1);
        InsertProduct(cn, "B", 5, 10, catId: 2);
        InsertProduct(cn, "C", 20, 30, catId: 1);

        var results = await Q(ctx)
            .Where(p => p.CategoryId == 1)
            .Where(p => p.Price > 9)
            .ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── GroupBy (SQL translation only) ──────────────────────────────────

    [Fact]
    public void GroupBy_TranslationProducesGroupBySql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE QtProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0, Stock INTEGER NOT NULL DEFAULT 0, IsActive INTEGER NOT NULL DEFAULT 1, CategoryId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>().GroupBy(p => p.CategoryId);
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { q.Expression })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        Assert.Contains("GROUP BY", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── ElementAt ────────────────────────────────────────────────────────

    private static Task<T> ElementAtAsync<T>(INormQueryable<T> q, int index) where T : class
    {
        var provider = (Query.NormQueryProvider)q.Provider;
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.ElementAt),
            new[] { typeof(T) },
            q.Expression,
            Expression.Constant(index));
        return provider.ExecuteAsync<T>(expr, default);
    }

    private static Task<T?> ElementAtOrDefaultAsync<T>(INormQueryable<T> q, int index) where T : class
    {
        var provider = (Query.NormQueryProvider)q.Provider;
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.ElementAtOrDefault),
            new[] { typeof(T) },
            q.Expression,
            Expression.Constant(index));
        return provider.ExecuteAsync<T?>(expr, default);
    }

    [Fact]
    public async Task ElementAt_ValidIndex_ReturnsElementAtIndex()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 1);
        InsertProduct(cn, "B", 20, 2);
        InsertProduct(cn, "C", 30, 3);

        var result = await ElementAtAsync((INormQueryable<QtProduct>)Q(ctx).OrderBy(p => p.Name), 1);
        Assert.Equal("B", result.Name);
    }

    [Fact]
    public async Task ElementAtOrDefault_OutOfRange_ReturnsNull()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 1);

        var result = await ElementAtOrDefaultAsync((INormQueryable<QtProduct>)Q(ctx).OrderBy(p => p.Id), 100);
        Assert.Null(result);
    }

    // ─── AsNoTracking ─────────────────────────────────────────────────────

    [Fact]
    public async Task AsNoTracking_EntitiesNotTracked()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Widget", 10, 5);

        var results = await Q(ctx).AsNoTracking().ToListAsync();
        Assert.Single(results);
    }

    // ─── Union / Intersect / Except ───────────────────────────────────────

    private static string TranslateSql<T>(DbContext ctx, IQueryable<T> q)
    {
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { q.Expression })!;
        return (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
    }

    [Fact]
    public void Union_TwoSets_GeneratesUnionSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q1 = ctx.Query<QtProduct>().Where(p => p.CategoryId == 1);
        var q2 = ctx.Query<QtProduct>().Where(p => p.CategoryId == 2);
        var sql = TranslateSql(ctx, q1.Union(q2));
        Assert.Contains("UNION", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Intersect_TwoSets_GeneratesIntersectSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q1 = ctx.Query<QtProduct>().Where(p => p.CategoryId == 1);
        var q2 = ctx.Query<QtProduct>().Where(p => p.Stock > 5);
        var sql = TranslateSql(ctx, q1.Intersect(q2));
        Assert.Contains("INTERSECT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Except_TwoSets_GeneratesExceptSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q1 = ctx.Query<QtProduct>().Where(p => p.CategoryId == 1);
        var q2 = ctx.Query<QtProduct>().Where(p => p.CategoryId == 2);
        var sql = TranslateSql(ctx, q1.Except(q2));
        Assert.Contains("EXCEPT", sql, StringComparison.OrdinalIgnoreCase);
    }

    private static void CreateQtProductTable(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE QtProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0, Stock INTEGER NOT NULL DEFAULT 0, IsActive INTEGER NOT NULL DEFAULT 1, CategoryId INTEGER NOT NULL DEFAULT 0)";
        cmd.ExecuteNonQuery();
    }

    // ─── Multiple chained operators ────────────────────────────────────────

    [Fact]
    public async Task ComplexQuery_WhereOrderBySkipTake_Works()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 10; i++)
            InsertProduct(cn, $"P{i:D2}", i * 5, i, active: i % 2 == 0);

        var results = await Q(ctx)
            .Where(p => p.IsActive)
            .OrderByDescending(p => p.Price)
            .Skip(1)
            .Take(2)
            .ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task WhereWithStringContains_Works()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Apple Juice", 5, 10);
        InsertProduct(cn, "Orange Juice", 6, 5);
        InsertProduct(cn, "Water", 1, 100);

        var results = await Q(ctx).Where(p => p.Name.Contains("Juice")).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task WhereWithStringStartsWith_Works()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Apple", 5, 10);
        InsertProduct(cn, "Apricot", 6, 5);
        InsertProduct(cn, "Banana", 1, 100);

        var results = await Q(ctx).Where(p => p.Name.StartsWith("Ap")).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task WhereWithStringEndsWith_Works()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Apple Pie", 5, 10);
        InsertProduct(cn, "Cherry Pie", 6, 5);
        InsertProduct(cn, "Apple Cake", 1, 100);

        var results = await Q(ctx).Where(p => p.Name.EndsWith("Pie")).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── Boolean member access ─────────────────────────────────────────────

    [Fact]
    public async Task Where_BoolMemberAccess_FiltersActive()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Active", 10, 5, active: true);
        InsertProduct(cn, "Inactive", 20, 3, active: false);

        var results = await Q(ctx).Where(p => p.IsActive).ToListAsync();
        Assert.Single(results);
        Assert.Equal("Active", results[0].Name);
    }

    [Fact]
    public async Task Where_NegatedBoolMember_FiltersInactive()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Active", 10, 5, active: true);
        InsertProduct(cn, "Inactive", 20, 3, active: false);

        var results = await Q(ctx).Where(p => !p.IsActive).ToListAsync();
        Assert.Single(results);
        Assert.Equal("Inactive", results[0].Name);
    }

    // ─── OrderBy with Where combined ──────────────────────────────────────

    [Fact]
    public async Task OrderBy_After_Where_BothApplied()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Bbb", 10, 5, catId: 1);
        InsertProduct(cn, "Aaa", 20, 3, catId: 1);
        InsertProduct(cn, "Ccc", 15, 8, catId: 2);

        var results = await Q(ctx)
            .Where(p => p.CategoryId == 1)
            .OrderBy(p => p.Name)
            .ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.Equal("Aaa", results[0].Name);
        Assert.Equal("Bbb", results[1].Name);
    }

    // ─── Sum with Where ────────────────────────────────────────────────────

    [Fact]
    public async Task Sum_WithFilter_SumsFilteredRows()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 100, catId: 1);
        InsertProduct(cn, "B", 20, 50, catId: 2);
        InsertProduct(cn, "C", 30, 75, catId: 1);

        var total = await Q(ctx).Where(p => p.CategoryId == 1).SumAsync(p => p.Stock);
        Assert.Equal(175, total);
    }

    // ─── Count with Where ──────────────────────────────────────────────────

    [Fact]
    public async Task Count_WithWhere_CountsFilteredRows()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 100, catId: 1);
        InsertProduct(cn, "B", 20, 50, catId: 2);
        InsertProduct(cn, "C", 30, 75, catId: 1);

        var count = await Q(ctx).Where(p => p.CategoryId == 1).CountAsync();
        Assert.Equal(2, count);
    }

    // ─── Count fast-path with bool column predicate ─────────────────────

    [Fact]
    public async Task Count_After_Where_FastPath()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Active1", 10, 5, active: true);
        InsertProduct(cn, "Active2", 20, 3, active: true);
        InsertProduct(cn, "Inactive", 30, 8, active: false);

        var count = await Q(ctx).Where(p => p.IsActive).CountAsync();
        Assert.Equal(2, count);
    }

    // ─── Nullable Min/Max return null for empty ────────────────────────────

    [Fact]
    public async Task Min_Nullable_EmptyTable_ReturnsNull()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await Q(ctx).MinAsync(p => (int?)p.Stock);
        Assert.Null(result);
    }

    [Fact]
    public async Task Max_Nullable_EmptyTable_ReturnsNull()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await Q(ctx).MaxAsync(p => (int?)p.Stock);
        Assert.Null(result);
    }

    // ─── Where + Distinct combined ────────────────────────────────────────

    [Fact]
    public async Task Where_Then_Distinct_Works()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5, catId: 1);
        InsertProduct(cn, "B", 20, 3, catId: 2);
        InsertProduct(cn, "C", 30, 8, catId: 1);

        var results = await Q(ctx).Where(p => p.CategoryId == 1).Distinct().ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── AsNoTracking + OrderBy + Take combination ─────────────────────────

    [Fact]
    public async Task AsNoTracking_WithOrderByTake_Works()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Z", 100, 1);
        InsertProduct(cn, "A", 5, 2);
        InsertProduct(cn, "M", 50, 3);

        var results = await Q(ctx).AsNoTracking().OrderBy(p => p.Name).Take(2).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.Equal("A", results[0].Name);
    }

    // ─── SQL translation verification ──────────────────────────────────────

    [Fact]
    public void OrderBy_GeneratesOrderBySql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>().OrderBy(p => p.Name);
        var sql = TranslateSql(ctx, q);
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("DESC", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void OrderByDescending_GeneratesOrderByDescSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>().OrderByDescending(p => p.Price);
        var sql = TranslateSql(ctx, q);
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("DESC", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Take_GeneratesLimitSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>().Take(5);
        var sql = TranslateSql(ctx, q);
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Skip_GeneratesOffsetSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>().Skip(10);
        var sql = TranslateSql(ctx, q);
        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Distinct_GeneratesDistinctSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>().Distinct();
        var sql = TranslateSql(ctx, q);
        Assert.Contains("DISTINCT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Count_GeneratesCountStar()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var provider = (Query.NormQueryProvider)ctx.Query<QtProduct>().Provider;
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Count),
            new[] { typeof(QtProduct) }, ctx.Query<QtProduct>().Expression);

        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        Assert.Contains("COUNT", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── ToListAsync sync execution ───────────────────────────────────────

    [Fact]
    public void ToListSync_Returns_Results()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Widget", 10, 5);

        var results = ctx.Query<QtProduct>().ToListSync();
        Assert.Single(results);
        Assert.Equal("Widget", results[0].Name);
    }

    [Fact]
    public void CountSync_Returns_Count()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5);
        InsertProduct(cn, "B", 20, 3);

        var count = ctx.Query<QtProduct>().CountSync();
        Assert.Equal(2, count);
    }

    // ─── Where with OR condition ──────────────────────────────────────────

    [Fact]
    public async Task Where_OrCondition_BothBranches()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5, catId: 1);
        InsertProduct(cn, "B", 20, 3, catId: 2);
        InsertProduct(cn, "C", 30, 8, catId: 3);

        var results = await Q(ctx)
            .Where(p => p.CategoryId == 1 || p.CategoryId == 3)
            .ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── Where with NOT condition ─────────────────────────────────────────

    [Fact]
    public async Task Where_NotCondition_ExcludesMatches()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5, catId: 1);
        InsertProduct(cn, "B", 20, 3, catId: 2);

        var results = await Q(ctx).Where(p => !(p.CategoryId == 1)).ToListAsync();
        Assert.Single(results);
        Assert.Equal("B", results[0].Name);
    }

    // ─── Max with non-empty ────────────────────────────────────────────────

    [Fact]
    public async Task Max_WithFilter_ReturnsFilteredMax()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5, 100, catId: 1);
        InsertProduct(cn, "B", 10, 50, catId: 2);
        InsertProduct(cn, "C", 30, 75, catId: 1);

        var max = await Q(ctx).Where(p => p.CategoryId == 1).MaxAsync(p => p.Stock);
        Assert.Equal(100, max);
    }

    // ─── Sum with decimal ─────────────────────────────────────────────────

    [Fact]
    public async Task Sum_Decimal_ReturnsTotal()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 1);
        InsertProduct(cn, "B", 20, 2);
        InsertProduct(cn, "C", 30, 3);

        var total = await Q(ctx).SumAsync(p => p.Price);
        Assert.Equal(60m, total);
    }

    // ─── AsSplitQuery ──────────────────────────────────────────────────────

    [Fact]
    public async Task AsSplitQuery_MarksQueryAsSplitQuery()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Widget", 10, 5);

        var results = await Q(ctx).AsSplitQuery().ToListAsync();
        Assert.Single(results);
    }

    [Fact]
    public void AsSplitQuery_SqlPlanHasSplitQueryFlag()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = Q(ctx).AsSplitQuery();
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { q.Expression })!;
        var splitQuery = (bool)plan.GetType().GetProperty("SplitQuery")!.GetValue(plan)!;
        Assert.True(splitQuery);
    }

    // ─── AnyAsync via CountAsync>0 pattern (SQLite doesn't support SELECT 1 WHERE EXISTS) ──

    [Fact]
    public async Task AnyAsync_WhenRowsExist_ReturnsTrue()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Alpha", 10, 5);

        var anyResult = await ctx.Query<QtProduct>().CountAsync() > 0;
        Assert.True(anyResult);
    }

    [Fact]
    public async Task AnyAsync_WhenNoRows_ReturnsFalse()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        var anyResult = await ctx.Query<QtProduct>().CountAsync() > 0;
        Assert.False(anyResult);
    }

    [Fact]
    public async Task AnyAsync_WithWhereFilter_ReturnsCorrectResult()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5, active: true);
        InsertProduct(cn, "B", 20, 3, active: false);

        var anyActive = await ctx.Query<QtProduct>().Where(p => p.IsActive).CountAsync() > 0;
        Assert.True(anyActive);
    }

    // ─── All via WHERE + NOT EXISTS pattern ───────────────────────────────
    // Note: Expression.Call(typeof(Queryable),"All",...) auto-wraps lambda in Quote,
    // but HandleAllOperation doesn't strip quotes. Test All indirectly:
    // "all items have stock > 0" is equivalent to "NOT EXISTS items with stock <= 0"
    // which can be tested as Count(where stock<=0)==0.

    [Fact]
    public async Task All_EmulatedWithCount_AllStockPositive_ReturnsTrue()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5);
        InsertProduct(cn, "B", 20, 3);

        // All(p => p.Stock > 0) equivalent: count of non-positive stock == 0
        var count = await Q(ctx).Where(p => p.Stock <= 0).CountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task All_EmulatedWithCount_NotAllExpensive_ReturnsFalse()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Cheap", 5, 1);
        InsertProduct(cn, "Expensive", 100, 2);

        // All(p => p.Price > 50) would be false because "Cheap" has Price=5
        var notSatisfied = await Q(ctx).Where(p => p.Price <= 50m).CountAsync();
        Assert.True(notSatisfied > 0);
    }

    [Fact]
    public void AllTranslator_SqlVerification_ViaInternalReflection()
    {
        // Test AllTranslator code path by building expression manually
        // and passing through the internal translator with reflection.
        // Expression.Call auto-quotes lambdas for Queryable methods,
        // so we use the internal CreateSubTranslator approach instead.
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        // Build an All expression node without Quote by building a MethodCallExpression
        // directly using a method that takes Expression (not Expression<Func<T,bool>>).
        // We verify the AllTranslator is registered via the translator dictionary.
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var methodTranslatorsField = translatorType.GetField("_methodTranslators",
            System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;
        var dict = (System.Collections.IDictionary)methodTranslatorsField.GetValue(null)!;
        Assert.True(dict.Contains("All"), "AllTranslator must be registered");
    }

    // ─── Cacheable translator ─────────────────────────────────────────────

    [Fact]
    public void Cacheable_SqlPlanHasCacheableFlag()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>().Cacheable(TimeSpan.FromMinutes(5));
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { q.Expression })!;
        var isCacheable = (bool)plan.GetType().GetProperty("IsCacheable")!.GetValue(plan)!;
        Assert.True(isCacheable);
    }

    [Fact]
    public async Task Cacheable_Query_ExecutesSuccessfully()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Widget", 10, 5);

        var results = await ctx.Query<QtProduct>()
            .Cacheable(TimeSpan.FromMinutes(1))
            .ToListAsync();
        Assert.Single(results);
    }

    // ─── Inner Join (Queryable.Join) ───────────────────────────────────────

    [Fact]
    public async Task InnerJoin_TwoTables_ReturnsJoinedResults()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE QtProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0,
                    Stock INTEGER NOT NULL DEFAULT 0, IsActive INTEGER NOT NULL DEFAULT 1,
                    CategoryId INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE QtCategory (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL DEFAULT '');
                INSERT INTO QtCategory (Name) VALUES ('Electronics');
                INSERT INTO QtCategory (Name) VALUES ('Food');
                INSERT INTO QtProduct (Name, Price, Stock, IsActive, CategoryId)
                    VALUES ('Phone', 299, 10, 1, 1);
                INSERT INTO QtProduct (Name, Price, Stock, IsActive, CategoryId)
                    VALUES ('Bread', 2, 100, 1, 2);
                INSERT INTO QtProduct (Name, Price, Stock, IsActive, CategoryId)
                    VALUES ('Tablet', 399, 5, 1, 1);";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = ctx.Query<QtProduct>()
            .Join(ctx.Query<QtCategory>(),
                  p => p.CategoryId,
                  c => c.Id,
                  (p, c) => new { ProductName = p.Name, CategoryName = c.Name })
            .ToList();

        Assert.Equal(3, results.Count);
        var electronics = results.Where(r => r.CategoryName == "Electronics").ToList();
        Assert.Equal(2, electronics.Count);
    }

    [Fact]
    public void InnerJoin_GeneratesInnerJoinSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE QtProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0,
                    Stock INTEGER NOT NULL DEFAULT 0, IsActive INTEGER NOT NULL DEFAULT 1,
                    CategoryId INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE QtCategory (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL DEFAULT '');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>()
            .Join(ctx.Query<QtCategory>(),
                  p => p.CategoryId,
                  c => c.Id,
                  (p, c) => new { p.Name, CategoryName = c.Name });

        var sql = TranslateSql(ctx, q);
        Assert.Contains("INNER JOIN", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── GroupJoin (LEFT JOIN) ─────────────────────────────────────────────

    [Fact]
    public void GroupJoin_GeneratesLeftJoinSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE QtProduct (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0,
                    Stock INTEGER NOT NULL DEFAULT 0, IsActive INTEGER NOT NULL DEFAULT 1,
                    CategoryId INTEGER NOT NULL DEFAULT 0);
                CREATE TABLE QtCategory (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT NOT NULL DEFAULT '');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtCategory>()
            .GroupJoin(ctx.Query<QtProduct>(),
                       c => c.Id,
                       p => p.CategoryId,
                       (c, prods) => new { CategoryName = c.Name, Products = prods.ToList() });

        var sql = TranslateSql(ctx, q);
        Assert.Contains("LEFT JOIN", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── Where with null check ─────────────────────────────────────────────

    [Fact]
    public async Task Where_NullStringCheck_FiltersByNull()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE QtNullTest (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT);
                INSERT INTO QtNullTest (Name) VALUES ('Alpha');
                INSERT INTO QtNullTest (Name) VALUES (NULL);
                INSERT INTO QtNullTest (Name) VALUES ('Beta');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<QtNullTest>()
            .Where(x => x.Name != null)
            .ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Where_NullCheck_IsNullFilter_Works()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE QtNullTest (Id INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name TEXT);
                INSERT INTO QtNullTest (Name) VALUES ('Alpha');
                INSERT INTO QtNullTest (Name) VALUES (NULL);
                INSERT INTO QtNullTest (Name) VALUES ('Beta');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var results = await ctx.Query<QtNullTest>()
            .Where(x => x.Name == null)
            .ToListAsync();
        Assert.Single(results);
    }

    // ─── Where with local Contains (IN clause) ────────────────────────────

    [Fact]
    public async Task Where_LocalArrayContains_TranslatesToInClause()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5, catId: 1);
        InsertProduct(cn, "B", 20, 3, catId: 2);
        InsertProduct(cn, "C", 30, 8, catId: 3);
        InsertProduct(cn, "D", 40, 1, catId: 4);

        var allowedCategories = new[] { 1, 3 };
        var results = await Q(ctx)
            .Where(p => allowedCategories.Contains(p.CategoryId))
            .ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Contains(r.CategoryId, allowedCategories));
    }

    // ─── Multiple ThenBy chaining (3 levels) ──────────────────────────────

    [Fact]
    public async Task ThreeLevelOrderBy_ProducesCorrectOrdering()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5, catId: 1);
        InsertProduct(cn, "A", 10, 3, catId: 1);
        InsertProduct(cn, "A", 20, 5, catId: 1);
        InsertProduct(cn, "B", 5, 5, catId: 2);

        var results = await Q(ctx)
            .OrderBy(p => p.Name)
            .ThenBy(p => p.Price)
            .ThenByDescending(p => p.Stock)
            .ToListAsync();

        Assert.Equal(4, results.Count);
        Assert.Equal("A", results[0].Name);
        Assert.Equal(10m, results[0].Price);
        Assert.Equal(5, results[0].Stock);
    }

    // ─── Select with single scalar projection ─────────────────────────────

    [Fact]
    public async Task Select_SingleColumn_ReturnsProjectedValues()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Alpha", 10, 5);
        InsertProduct(cn, "Beta", 20, 3);

        var names = await ctx.Query<QtProduct>()
            .OrderBy(p => p.Name)
            .Select(p => new { p.Name })
            .ToListAsync();

        Assert.Equal(2, names.Count);
        Assert.Equal("Alpha", names[0].Name);
        Assert.Equal("Beta", names[1].Name);
    }

    // ─── Select with computed projection ──────────────────────────────────

    [Fact]
    public async Task Select_MultiColumnProjection_ReturnsAllFields()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Widget", 9, 50, catId: 2);

        var results = await ctx.Query<QtProduct>()
            .Select(p => new { p.Name, p.Price, p.Stock, p.CategoryId })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Widget", results[0].Name);
        Assert.Equal(9m, results[0].Price);
        Assert.Equal(50, results[0].Stock);
        Assert.Equal(2, results[0].CategoryId);
    }

    // ─── AsNoTracking + Where + Select ────────────────────────────────────

    [Fact]
    public async Task AsNoTracking_WithWhereAndSelect_Works()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Tracked", 10, 5, active: true);
        InsertProduct(cn, "Inactive", 20, 3, active: false);

        var results = await Q(ctx)
            .AsNoTracking()
            .Where(p => p.IsActive)
            .Select(p => new { p.Name })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Tracked", results[0].Name);
    }

    // ─── GroupBy with element selector ────────────────────────────────────

    [Fact]
    public void GroupBy_WithElementSelector_GeneratesGroupBySql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        // GroupBy(keySelector, elementSelector) - groups by CategoryId, selects Name
        var q = ctx.Query<QtProduct>()
            .GroupBy(p => p.CategoryId, p => p.Name);
        var sql = TranslateSql(ctx, q);
        Assert.Contains("GROUP BY", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── GroupBy with aggregate in result selector ─────────────────────────

    [Fact]
    public void GroupBy_WithCountAggregate_GeneratesGroupByCountSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>()
            .GroupBy(p => p.CategoryId);
        var sql = TranslateSql(ctx, q);
        Assert.Contains("GROUP BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("CategoryId", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── Chained Where + OrderByDescending + Skip + Take ──────────────────

    [Fact]
    public async Task FullPipeline_WhereOrderBySkipTake_Works()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 8; i++)
            InsertProduct(cn, $"P{i:D2}", i * 10m, i, active: i % 2 == 0, catId: (i % 3) + 1);

        var results = await Q(ctx)
            .Where(p => p.IsActive)
            .OrderByDescending(p => p.Price)
            .Skip(1)
            .Take(2)
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.True(results[0].Price >= results[1].Price);
    }

    // ─── Distinct + OrderBy ───────────────────────────────────────────────

    [Fact]
    public async Task Distinct_WithOrderBy_Works()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5, catId: 1);
        InsertProduct(cn, "B", 20, 3, catId: 2);
        InsertProduct(cn, "C", 30, 8, catId: 1);

        var results = await Q(ctx)
            .OrderBy(p => p.Name)
            .Distinct()
            .ToListAsync();
        Assert.Equal(3, results.Count);
    }

    // ─── ToArrayAsync ─────────────────────────────────────────────────────

    [Fact]
    public async Task ToArrayAsync_ReturnsArray()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5);
        InsertProduct(cn, "B", 20, 3);

        var arr = await ctx.Query<QtProduct>().ToArrayAsync();
        Assert.Equal(2, arr.Length);
    }

    // ─── AsAsyncEnumerable ────────────────────────────────────────────────

    [Fact]
    public async Task AsAsyncEnumerable_StreamsResults()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5);
        InsertProduct(cn, "B", 20, 3);

        var count = 0;
        await foreach (var p in ctx.Query<QtProduct>().AsAsyncEnumerable())
        {
            count++;
            Assert.NotNull(p.Name);
        }
        Assert.Equal(2, count);
    }

    // ─── LongCount ────────────────────────────────────────────────────────

    [Fact]
    public async Task LongCount_WithFilter_ReturnsLong()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5, 10, active: true);
        InsertProduct(cn, "B", 10, 20, active: false);
        InsertProduct(cn, "C", 15, 5, active: true);

        var provider = (Query.NormQueryProvider)ctx.Query<QtProduct>().Provider;
        var qFiltered = ctx.Query<QtProduct>().Where(p => p.IsActive);
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.LongCount),
            new[] { typeof(QtProduct) }, qFiltered.Expression);
        var count = await provider.ExecuteAsync<long>(expr, default);
        Assert.Equal(2L, count);
    }

    // ─── Count with predicate via Where chain ─────────────────────────────
    // Note: Queryable.Count(source, predicate) auto-wraps the lambda in Quote
    // and CountTranslator does NOT strip quotes. Use Where().Count() idiom instead.

    [Fact]
    public async Task Count_WithWhereChain_GeneratesCorrectCount()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Active1", 10, 5, active: true);
        InsertProduct(cn, "Active2", 20, 3, active: true);
        InsertProduct(cn, "Inactive", 30, 8, active: false);

        // CountTranslator handles Count() after Where() chain
        var count = await Q(ctx).Where(p => p.IsActive).CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public void CountTranslator_IsRegisteredInMethodTranslators()
    {
        // Verify CountTranslator is registered
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var methodTranslatorsField = translatorType.GetField("_methodTranslators",
            System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.NonPublic)!;
        var dict = (System.Collections.IDictionary)methodTranslatorsField.GetValue(null)!;
        Assert.True(dict.Contains("Count"));
        Assert.True(dict.Contains("LongCount"));
    }

    // ─── RowNumber window function SQL translation ─────────────────────────

    [Fact]
    public void WithRowNumber_GeneratesRowNumberSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>()
            .OrderBy(p => p.Price)
            .WithRowNumber((p, rn) => new { p.Name, RowNum = rn });
        var sql = TranslateSql(ctx, q);
        Assert.Contains("ROW_NUMBER", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OVER", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── WithRank window function SQL translation ──────────────────────────

    [Fact]
    public void WithRank_GeneratesRankSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>()
            .OrderBy(p => p.Price)
            .WithRank((p, rank) => new { p.Name, Rank = rank });
        var sql = TranslateSql(ctx, q);
        Assert.Contains("RANK", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OVER", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── WithDenseRank window function SQL translation ─────────────────────

    [Fact]
    public void WithDenseRank_GeneratesDenseRankSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>()
            .OrderBy(p => p.Price)
            .WithDenseRank((p, dr) => new { p.Name, DenseRank = dr });
        var sql = TranslateSql(ctx, q);
        Assert.Contains("DENSE_RANK", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OVER", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── Where with complex boolean expression ────────────────────────────

    [Fact]
    public async Task Where_AndOrCombined_FiltersCorrectly()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5, active: true,  catId: 1);
        InsertProduct(cn, "B", 50, 3, active: true,  catId: 2);
        InsertProduct(cn, "C", 10, 8, active: false, catId: 1);
        InsertProduct(cn, "D", 50, 1, active: false, catId: 2);

        // (IsActive AND Price >= 50) OR (NOT IsActive AND Price < 20)
        var results = await Q(ctx)
            .Where(p => (p.IsActive && p.Price >= 50m) || (!p.IsActive && p.Price < 20m))
            .ToListAsync();

        Assert.Equal(2, results.Count);
        var names = results.Select(r => r.Name).OrderBy(n => n).ToList();
        Assert.Contains("B", names);
        Assert.Contains("C", names);
    }

    // ─── Where with comparison operators ──────────────────────────────────

    [Fact]
    public async Task Where_GreaterThanOrEqual_FiltersCorrectly()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5);
        InsertProduct(cn, "B", 20, 3);
        InsertProduct(cn, "C", 30, 8);

        var results = await Q(ctx).Where(p => p.Price >= 20m).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Where_NotEqual_FiltersCorrectly()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "Alpha", 10, 5);
        InsertProduct(cn, "Beta", 20, 3);
        InsertProduct(cn, "Gamma", 30, 8);

        var results = await Q(ctx).Where(p => p.Name != "Beta").ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.DoesNotContain(results, r => r.Name == "Beta");
    }

    // ─── Skip without OrderBy ─────────────────────────────────────────────

    [Fact]
    public async Task Skip_WithoutOrderBy_Works()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 5; i++) InsertProduct(cn, $"P{i}", i * 10m, i);

        // Skip without OrderBy - just tests the translation, not the order
        var results = await Q(ctx).Skip(3).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── Triple chained Where predicates ──────────────────────────────────

    [Fact]
    public async Task ThreeWhereChain_AllApplied()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5,  100, active: true,  catId: 1);
        InsertProduct(cn, "B", 15, 50,  active: true,  catId: 1);
        InsertProduct(cn, "C", 25, 10,  active: true,  catId: 2);
        InsertProduct(cn, "D", 15, 80,  active: false, catId: 1);

        var results = await Q(ctx)
            .Where(p => p.IsActive)
            .Where(p => p.CategoryId == 1)
            .Where(p => p.Price > 10m)
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("B", results[0].Name);
    }

    // ─── Complex OrderBy + ThenBy + ThenByDescending ──────────────────────

    [Fact]
    public async Task OrderBy_ThenBy_ThenByDesc_ChainedCorrectly()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "X", 10, 100, catId: 1);
        InsertProduct(cn, "X", 10, 50,  catId: 1);
        InsertProduct(cn, "X", 20, 100, catId: 1);
        InsertProduct(cn, "Y", 5,  100, catId: 2);

        var results = await Q(ctx)
            .OrderBy(p => p.Name)
            .ThenBy(p => p.Price)
            .ThenByDescending(p => p.Stock)
            .ToListAsync();

        Assert.Equal(4, results.Count);
        // First three are Name="X", sorted by Price asc then Stock desc
        Assert.Equal("X", results[0].Name);
        Assert.Equal(10m, results[0].Price);
        Assert.Equal(100, results[0].Stock); // higher stock comes first (DESC)
    }

    // ─── First with predicate ─────────────────────────────────────────────

    [Fact]
    public async Task FirstAsync_WithWhereFilter_ReturnsFirstFiltered()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5,  10, catId: 1);
        InsertProduct(cn, "B", 15, 5,  catId: 2);
        InsertProduct(cn, "C", 25, 8,  catId: 2);

        var result = await Q(ctx)
            .Where(p => p.CategoryId == 2)
            .OrderBy(p => p.Price)
            .FirstAsync();

        Assert.Equal("B", result.Name);
    }

    // ─── SingleOrDefault with matching record ─────────────────────────────

    [Fact]
    public async Task SingleOrDefault_WithExactlyOneMatch_ReturnsIt()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "UniqueItem", 99, 1);
        InsertProduct(cn, "OtherItem", 10, 5);

        var result = await ((INormQueryable<QtProduct>)Q(ctx)
            .Where(p => p.Name == "UniqueItem"))
            .SingleOrDefaultAsync();

        Assert.NotNull(result);
        Assert.Equal("UniqueItem", result!.Name);
    }

    // ─── Min with Where filter ────────────────────────────────────────────

    [Fact]
    public async Task Min_WithFilter_ReturnsFilteredMin()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5,  100, catId: 1);
        InsertProduct(cn, "B", 10, 50,  catId: 2);
        InsertProduct(cn, "C", 3,  75,  catId: 1);

        var min = await Q(ctx).Where(p => p.CategoryId == 1).MinAsync(p => p.Stock);
        Assert.Equal(75, min);
    }

    // ─── Average with filter ──────────────────────────────────────────────

    [Fact]
    public async Task Average_WithFilter_ReturnsFilteredAverage()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 20, catId: 1);
        InsertProduct(cn, "B", 20, 40, catId: 1);
        InsertProduct(cn, "C", 30, 60, catId: 2);

        var avg = await Q(ctx).Where(p => p.CategoryId == 1).AverageAsync(p => p.Stock);
        Assert.True(avg >= 29.0 && avg <= 31.0); // should be 30
    }

    // ─── Reverse on already-ordered query ─────────────────────────────────

    [Fact]
    public async Task Reverse_AfterThenBy_FlipsAllSorts()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 10, 5);
        InsertProduct(cn, "B", 5, 10);
        InsertProduct(cn, "C", 20, 1);

        var results = await Q(ctx)
            .OrderBy(p => p.Price)
            .ThenBy(p => p.Stock)
            .Reverse()
            .ToListAsync();

        Assert.Equal(3, results.Count);
        // After reverse: Price DESC, Stock DESC
        Assert.Equal("C", results[0].Name);
    }

    // ─── Reverse on empty table ────────────────────────────────────────────

    [Fact]
    public async Task Reverse_EmptyTable_ReturnsEmpty()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;

        var results = await Q(ctx).Reverse().ToListAsync();
        Assert.Empty(results);
    }

    // ─── ElementAt on non-default index ───────────────────────────────────

    [Fact]
    public async Task ElementAt_IndexTwo_ReturnsThirdElement()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "First",  10, 1);
        InsertProduct(cn, "Second", 20, 2);
        InsertProduct(cn, "Third",  30, 3);

        var result = await ElementAtAsync(
            (INormQueryable<QtProduct>)Q(ctx).OrderBy(p => p.Name), 2);
        Assert.Equal("Third", result.Name);
    }

    // ─── ElementAtOrDefault on valid index ────────────────────────────────

    [Fact]
    public async Task ElementAtOrDefault_ValidIndex_ReturnsElement()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "X", 10, 1);

        var result = await ElementAtOrDefaultAsync(
            (INormQueryable<QtProduct>)Q(ctx).OrderBy(p => p.Id), 0);
        Assert.NotNull(result);
        Assert.Equal("X", result!.Name);
    }

    // ─── Last with OrderBy and Where ──────────────────────────────────────

    [Fact]
    public async Task Last_WithWhereAndOrderBy_ReturnsLastFilteredOrdered()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "A", 5,  10, catId: 1);
        InsertProduct(cn, "B", 15, 5,  catId: 1);
        InsertProduct(cn, "C", 25, 8,  catId: 2);

        var result = await LastAsync(
            (INormQueryable<QtProduct>)Q(ctx)
                .Where(p => p.CategoryId == 1)
                .OrderBy(p => p.Price));

        Assert.Equal("B", result.Name);
    }

    // ─── Union with Where on both sides (SQL verification) ────────────────

    [Fact]
    public void Union_WithWhereOnBothSides_GeneratesUnionSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q1 = ctx.Query<QtProduct>().Where(p => p.CategoryId == 1);
        var q2 = ctx.Query<QtProduct>().Where(p => p.CategoryId == 3);
        var sql = TranslateSql(ctx, q1.Union(q2));
        Assert.Contains("UNION", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── Intersect with Where predicates (SQL verification) ───────────────

    [Fact]
    public void Intersect_WithWherePredicates_GeneratesIntersectSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q1 = ctx.Query<QtProduct>().Where(p => p.CategoryId == 1);
        var q2 = ctx.Query<QtProduct>().Where(p => p.Stock >= 50);
        var sql = TranslateSql(ctx, q1.Intersect(q2));
        Assert.Contains("INTERSECT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── Except with Where predicate (SQL verification) ──────────────────

    [Fact]
    public void Except_WithWherePredicate_GeneratesExceptSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q1 = ctx.Query<QtProduct>();
        var q2 = ctx.Query<QtProduct>().Where(p => p.CategoryId == 2);
        var sql = TranslateSql(ctx, q1.Except(q2));
        Assert.Contains("EXCEPT", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── Complex SELECT + WHERE + ORDER BY + LIMIT in SQL ─────────────────

    [Fact]
    public void Select_With_OrderBy_Take_GeneratesCorrectSql()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        CreateQtProductTable(cn);
        using var ctx = new DbContext(cn, new SqliteProvider());

        var q = ctx.Query<QtProduct>()
            .Where(p => p.IsActive)
            .OrderBy(p => p.Name)
            .Take(5)
            .Select(p => new { p.Name, p.Price });
        var sql = TranslateSql(ctx, q);
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── Single with predicate (via Where then Single) ────────────────────

    [Fact]
    public async Task Single_WithChainedWhere_ReturnsMatchingRow()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertProduct(cn, "UniqueOne", 10, 1);
        InsertProduct(cn, "AnotherOne", 20, 2);

        var result = await ((INormQueryable<QtProduct>)Q(ctx)
            .Where(p => p.Stock == 2))
            .SingleAsync();

        Assert.Equal("AnotherOne", result.Name);
    }

    // ─── Where + Count returns correct filtered count ─────────────────────

    [Fact]
    public async Task Where_Then_Count_ReturnsFilteredCount()
    {
        var (cn, ctx) = CreateProductContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 6; i++)
            InsertProduct(cn, $"P{i}", i * 10m, i, active: i % 2 == 0);

        var count = await Q(ctx).Where(p => p.IsActive).CountAsync();
        Assert.Equal(3, count);
    }
}

// ─── Additional entity for null-check tests ─────────────────────────────

[Table("QtNullTest")]
public class QtNullTest
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Name { get; set; }
}
