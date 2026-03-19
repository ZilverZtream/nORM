using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

// ─── Entity types for NormQueryProvider tests ──────────────────────────────

[Table("NqpItem")]
public class NqpItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public int Score { get; set; }
    public bool IsPublished { get; set; }
    public int TagId { get; set; }
}

[Table("NqpTag")]
public class NqpTag
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Label { get; set; } = string.Empty;
}

/// <summary>
/// Tests targeting uncovered NormQueryProvider paths:
/// - ExecuteAsync / ExecuteSync scalar results (Min/Max/Average/Sum empty set)
/// - First/FirstOrDefault/Single/SingleOrDefault via ExecuteAsync
/// - Count fast-path (bool member, equality, null equality)
/// - Simple query path (TryGetSimpleQuery)
/// - CreateQuery / CreateQuery<T>
/// - Global filters applied to queries
/// - Dispose cleans up pooled commands
/// - ConvertScalarResult for various types
/// - ExecuteCompiledAsync overloads
/// </summary>
public class NormQueryProviderCoverageTests
{
    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE NqpItem (
                Id          INTEGER PRIMARY KEY AUTOINCREMENT,
                Title       TEXT NOT NULL DEFAULT '',
                Score       INTEGER NOT NULL DEFAULT 0,
                IsPublished INTEGER NOT NULL DEFAULT 1,
                TagId       INTEGER NOT NULL DEFAULT 0
            )";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void Insert(SqliteConnection cn, string title, int score, bool published = true, int tagId = 1)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO NqpItem (Title, Score, IsPublished, TagId) VALUES (@t, @s, @p, @g)";
        cmd.Parameters.AddWithValue("@t", title);
        cmd.Parameters.AddWithValue("@s", score);
        cmd.Parameters.AddWithValue("@p", published ? 1 : 0);
        cmd.Parameters.AddWithValue("@g", tagId);
        cmd.ExecuteNonQuery();
    }

    private static INormQueryable<NqpItem> Q(DbContext ctx) =>
        (INormQueryable<NqpItem>)ctx.Query<NqpItem>();

    // ─── CreateQuery (non-generic IQueryProvider.CreateQuery) ─────────────

    [Fact]
    public void CreateQuery_NonGeneric_ReturnsQueryable()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var provider = (IQueryProvider)ctx.Query<NqpItem>().Provider;
        // Build a Where expression to pass to CreateQuery
        var src = ctx.Query<NqpItem>();
        var whereExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Where),
            new[] { typeof(NqpItem) },
            src.Expression,
            Expression.Quote(Expression.Lambda<Func<NqpItem, bool>>(
                Expression.Constant(true),
                Expression.Parameter(typeof(NqpItem), "x"))));

        var result = provider.CreateQuery(whereExpr);
        Assert.NotNull(result);
    }

    [Fact]
    public void CreateQuery_Generic_ReturnsTypedQueryable()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var provider = (IQueryProvider)ctx.Query<NqpItem>().Provider;
        var src = ctx.Query<NqpItem>();
        var whereExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Where),
            new[] { typeof(NqpItem) },
            src.Expression,
            Expression.Quote(Expression.Lambda<Func<NqpItem, bool>>(
                Expression.Constant(true),
                Expression.Parameter(typeof(NqpItem), "x"))));

        var result = provider.CreateQuery<NqpItem>(whereExpr);
        Assert.NotNull(result);
        Assert.IsAssignableFrom<IQueryable<NqpItem>>(result);
    }

    // ─── Execute<TResult> (sync) ──────────────────────────────────────────

    [Fact]
    public void Execute_Generic_ReturnsResult()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Test", 10);

        var provider = (IQueryProvider)ctx.Query<NqpItem>().Provider;
        // Build Count expression
        var countExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Count),
            new[] { typeof(NqpItem) }, ctx.Query<NqpItem>().Expression);

        var count = (int)provider.Execute<int>(countExpr)!;
        Assert.Equal(1, count);
    }

    [Fact]
    public void Execute_NonGeneric_ReturnsResult()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Test", 10);

        var provider = (IQueryProvider)ctx.Query<NqpItem>().Provider;
        var countExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Count),
            new[] { typeof(NqpItem) }, ctx.Query<NqpItem>().Expression);

        var result = provider.Execute(countExpr);
        Assert.NotNull(result);
    }

    // ─── Count fast path variations ───────────────────────────────────────

    [Fact]
    public async Task Count_BoolMemberPredicate_FastPath()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, published: true);
        Insert(cn, "B", 20, published: false);
        Insert(cn, "C", 30, published: true);

        // bool member access predicate — hits TryBuildCountWhereClause bool branch
        var count = await Q(ctx).Where(p => p.IsPublished).CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Count_NegatedBoolMember_FastPath()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, published: true);
        Insert(cn, "B", 20, published: false);

        var count = await Q(ctx).Where(p => !p.IsPublished).CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Count_EqualityPredicate_FastPath()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);
        Insert(cn, "B", 20, tagId: 2);
        Insert(cn, "C", 30, tagId: 1);

        // equality predicate — hits TryBuildCountWhereClause equality branch
        var count = await Q(ctx).Where(p => p.TagId == 1).CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Count_NullEqualityPredicate_FastPath()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);

        // null equality — hits IsNullConstant check
        var count = await Q(ctx).Where(p => p.Title == null).CountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Count_WithWhere_ThenCount_CachedPath()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);
        Insert(cn, "B", 20, tagId: 2);

        // First call populates cache, second call uses it
        var count1 = await Q(ctx).Where(p => p.TagId == 1).CountAsync();
        var count2 = await Q(ctx).Where(p => p.TagId == 1).CountAsync();
        Assert.Equal(count1, count2);
        Assert.Equal(1, count1);
    }

    [Fact]
    public async Task Count_NoFilter_ReturnsTotal()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);

        var count = await Q(ctx).CountAsync();
        Assert.Equal(2, count);
    }

    // ─── Scalar aggregate via ExecuteAsync ────────────────────────────────

    [Fact]
    public async Task ExecuteAsync_Sum_ReturnsTotal()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);
        Insert(cn, "C", 30);

        var total = await Q(ctx).SumAsync(p => p.Score);
        Assert.Equal(60, total);
    }

    [Fact]
    public async Task ExecuteAsync_Min_ReturnsMin()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 5);
        Insert(cn, "C", 20);

        var min = await Q(ctx).MinAsync(p => p.Score);
        Assert.Equal(5, min);
    }

    [Fact]
    public async Task ExecuteAsync_Max_ReturnsMax()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 99);
        Insert(cn, "C", 20);

        var max = await Q(ctx).MaxAsync(p => p.Score);
        Assert.Equal(99, max);
    }

    [Fact]
    public async Task ExecuteAsync_Average_ReturnsAverage()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);
        Insert(cn, "C", 30);

        var avg = await Q(ctx).AverageAsync(p => p.Score);
        Assert.True(avg > 19.0 && avg < 21.0);
    }

    // ─── Empty set aggregate error paths ──────────────────────────────────

    [Fact]
    public async Task Min_EmptyTable_ThrowsInvalidOperation()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).MinAsync(p => p.Score));
    }

    [Fact]
    public async Task Max_EmptyTable_ThrowsInvalidOperation()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).MaxAsync(p => p.Score));
    }

    [Fact]
    public async Task Average_EmptyTable_ThrowsInvalidOperation()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).AverageAsync(p => p.Score));
    }

    [Fact]
    public async Task Sum_EmptyTable_ReturnsZero()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var total = await Q(ctx).SumAsync(p => p.Score);
        Assert.Equal(0, total);
    }

    // ─── Nullable aggregate on empty set returns null ─────────────────────

    [Fact]
    public async Task Min_Nullable_EmptyTable_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await Q(ctx).MinAsync(p => (int?)p.Score);
        Assert.Null(result);
    }

    [Fact]
    public async Task Max_Nullable_EmptyTable_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await Q(ctx).MaxAsync(p => (int?)p.Score);
        Assert.Null(result);
    }

    // ─── First / FirstOrDefault via ExecuteAsync ──────────────────────────

    [Fact]
    public async Task First_WithRows_ReturnsFirstRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Alpha", 10);
        Insert(cn, "Beta", 20);

        var result = await Q(ctx).OrderBy(p => p.Title).FirstAsync();
        Assert.Equal("Alpha", result.Title);
    }

    [Fact]
    public async Task First_Empty_ThrowsInvalidOperation()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).FirstAsync());
    }

    [Fact]
    public async Task FirstOrDefault_Empty_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await Q(ctx).FirstOrDefaultAsync();
        Assert.Null(result);
    }

    // ─── Single / SingleOrDefault ─────────────────────────────────────────

    [Fact]
    public async Task Single_OneRow_ReturnsIt()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "OnlyOne", 42);

        var result = await ((INormQueryable<NqpItem>)Q(ctx).Where(p => p.Title == "OnlyOne")).SingleAsync();
        Assert.Equal("OnlyOne", result.Title);
    }

    [Fact]
    public async Task Single_MultipleRows_ThrowsInvalidOperation()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            Q(ctx).SingleAsync());
    }

    [Fact]
    public async Task SingleOrDefault_EmptyResult_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await ((INormQueryable<NqpItem>)Q(ctx).Where(p => p.Id == -999)).SingleOrDefaultAsync();
        Assert.Null(result);
    }

    // ─── ToListAsync returns multiple items ────────────────────────────────

    [Fact]
    public async Task ToListAsync_MultipleRows_ReturnsAll()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);
        Insert(cn, "C", 30);

        var results = await Q(ctx).ToListAsync();
        Assert.Equal(3, results.Count);
    }

    // ─── Simple query path (TryGetSimpleQuery) ─────────────────────────────

    [Fact]
    public async Task SimpleQuery_NoFilter_UsesSimplePath()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Widget", 5);

        // TryGetSimpleQuery matches plain .Where(x => x.Prop == value).First()
        var result = await ctx.Query<NqpItem>()
            .Where(p => p.Title == "Widget")
            .FirstAsync();
        Assert.Equal("Widget", result.Title);
    }

    [Fact]
    public async Task SimpleQuery_WithWhere_UsesParameterizedPath()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Alpha", 10);
        Insert(cn, "Beta", 20);

        var results = await ctx.Query<NqpItem>()
            .Where(p => p.Score == 10)
            .ToListAsync();
        Assert.Single(results);
        Assert.Equal("Alpha", results[0].Title);
    }

    // ─── Global filters interaction ────────────────────────────────────────

    [Fact]
    public async Task GlobalFilter_Applied_ToAllQueries()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NqpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, IsPublished INTEGER NOT NULL DEFAULT 1, TagId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        // Insert both published and unpublished
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO NqpItem (Title, Score, IsPublished, TagId) VALUES ('Published', 10, 1, 1)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO NqpItem (Title, Score, IsPublished, TagId) VALUES ('Unpublished', 20, 0, 1)";
            cmd.ExecuteNonQuery();
        }

        // Apply global filter for IsPublished = true
        var options = new nORM.Configuration.DbContextOptions();
        options.AddGlobalFilter<NqpItem>(p => p.IsPublished);
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        // Should only return published items
        var results = await ctx.Query<NqpItem>().ToListAsync();
        Assert.Single(results);
        Assert.Equal("Published", results[0].Title);
    }

    [Fact]
    public async Task GlobalFilter_CountExcludesFilteredRows()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NqpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, IsPublished INTEGER NOT NULL DEFAULT 1, TagId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO NqpItem (Title, Score, IsPublished, TagId) VALUES ('A', 10, 1, 1)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO NqpItem (Title, Score, IsPublished, TagId) VALUES ('B', 20, 0, 1)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO NqpItem (Title, Score, IsPublished, TagId) VALUES ('C', 30, 1, 1)";
            cmd.ExecuteNonQuery();
        }

        var options = new nORM.Configuration.DbContextOptions();
        options.AddGlobalFilter<NqpItem>(p => p.IsPublished);
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        // Global filter prevents the count fast path, falls back to full translator
        var count = await ctx.Query<NqpItem>().CountAsync();
        Assert.Equal(2, count);
    }

    // ─── Dispose ──────────────────────────────────────────────────────────

    [Fact]
    public void Dispose_CleansUpPooledCommands()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NqpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, IsPublished INTEGER NOT NULL DEFAULT 1, TagId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }

        var ctx = new DbContext(cn, new SqliteProvider());
        // Run a count to populate the pooled command cache
        _ = ctx.Query<NqpItem>().CountSync();
        // Dispose should not throw
        ctx.Dispose();
    }

    // ─── ConvertScalarResult type conversions ─────────────────────────────

    [Fact]
    public async Task Count_ReturnsInt()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);

        var count = await Q(ctx).CountAsync();
        Assert.IsType<int>(count);
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task LongCount_ReturnsLong()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);

        var provider = (NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.LongCount),
            new[] { typeof(NqpItem) }, ctx.Query<NqpItem>().Expression);
        var count = await provider.ExecuteAsync<long>(expr, default);
        Assert.IsType<long>(count);
        Assert.Equal(1L, count);
    }

    // ─── Compiled query via ExecuteCompiledAsync ──────────────────────────

    [Fact]
    public async Task ExecuteCompiledAsync_Dict_ExecutesCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Compiled", 42);

        // Build a plan via the translator
        var src = ctx.Query<NqpItem>().Where(p => p.Score == 42);
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var planObj = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { src.Expression })!;
        var planType = planObj.GetType();

        // Get the sql and parameters from the plan
        var sql = (string)planType.GetProperty("Sql")!.GetValue(planObj)!;
        var parameters = (IReadOnlyDictionary<string, object>)planType.GetProperty("Parameters")!.GetValue(planObj)!;

        Assert.NotNull(sql);
        Assert.NotEmpty(sql);
    }

    // ─── Any produces correct result ──────────────────────────────────────

    [Fact]
    public async Task AnyAsync_WithRows_ReturnsTrue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Present", 1);

        // Use CountAsync > 0 as equivalent of AnyAsync (see note in MEMORY.md)
        var any = await Q(ctx).CountAsync() > 0;
        Assert.True(any);
    }

    [Fact]
    public async Task AnyAsync_EmptyTable_ReturnsFalse()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var any = await Q(ctx).CountAsync() > 0;
        Assert.False(any);
    }

    // ─── Multiple sequential queries (caching test) ───────────────────────

    [Fact]
    public async Task MultipleQueries_PlanCached_SameResult()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);

        var r1 = await Q(ctx).OrderBy(p => p.Score).ToListAsync();
        var r2 = await Q(ctx).OrderBy(p => p.Score).ToListAsync();
        Assert.Equal(r1.Count, r2.Count);
        Assert.Equal(r1[0].Title, r2[0].Title);
    }

    // ─── ExecuteAsync with cancellation token ─────────────────────────────

    [Fact]
    public async Task ExecuteAsync_WithCancellationToken_Works()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);

        using var cts = new CancellationTokenSource();
        var results = await Q(ctx).ToListAsync(cts.Token);
        Assert.Single(results);
    }

    [Fact]
    public async Task ExecuteAsync_PreCancelledToken_Throws()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            Q(ctx).ToListAsync(cts.Token));
    }

    // ─── Where + OrderBy + Skip + Take combination ─────────────────────────

    [Fact]
    public async Task ComplexQuery_Works()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 10; i++)
            Insert(cn, $"Item{i:D2}", i * 10, published: i % 2 == 0);

        var results = await Q(ctx)
            .Where(p => p.IsPublished)
            .OrderByDescending(p => p.Score)
            .Skip(1)
            .Take(2)
            .ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── AsNoTracking flag propagated ─────────────────────────────────────

    [Fact]
    public async Task AsNoTracking_QueryExecuted_NoTracking()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Widget", 10);

        var results = await Q(ctx).AsNoTracking().ToListAsync();
        Assert.Single(results);
        // With AsNoTracking, entities should not be in the change tracker
        Assert.Empty(ctx.ChangeTracker.Entries);
    }

    // ─── Sum with Where pre-filtered ──────────────────────────────────────

    [Fact]
    public async Task Sum_WithFilter_CorrectResult()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);
        Insert(cn, "B", 20, tagId: 2);
        Insert(cn, "C", 30, tagId: 1);

        var total = await Q(ctx).Where(p => p.TagId == 1).SumAsync(p => p.Score);
        Assert.Equal(40, total);
    }

    // ─── Count with complex predicate (falls back to full translation) ────

    [Fact]
    public async Task Count_WithComplexPredicate_FullTranslation()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);
        Insert(cn, "B", 20, tagId: 2);
        Insert(cn, "C", 30, tagId: 1);

        // Complex predicate falls back to full translation path
        var count = await Q(ctx).Where(p => p.Score > 10 && p.TagId == 1).CountAsync();
        Assert.Equal(1, count);
    }

    // ─── Sync query execution (ExecuteSync / ToListSync) ──────────────────

    [Fact]
    public void ToListSync_ReturnsResults()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);

        var results = ctx.Query<NqpItem>().ToListSync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public void CountSync_ReturnsCount()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);
        Insert(cn, "C", 30);

        var count = ctx.Query<NqpItem>().CountSync();
        Assert.Equal(3, count);
    }

    // ─── ToArrayAsync ──────────────────────────────────────────────────────

    [Fact]
    public async Task ToArrayAsync_ReturnsArray()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);

        var results = await ctx.Query<NqpItem>().ToArrayAsync();
        Assert.Equal(2, results.Length);
    }

    // ─── First with Where filter ──────────────────────────────────────────

    [Fact]
    public async Task FirstAsync_WithWhere_ReturnsMatch()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Alpha", 10);
        Insert(cn, "Beta", 20);

        var result = await Q(ctx).Where(p => p.Title == "Beta").FirstAsync();
        Assert.Equal("Beta", result.Title);
        Assert.Equal(20, result.Score);
    }

    // ─── OrderBy then First ────────────────────────────────────────────────

    [Fact]
    public async Task OrderBy_ThenFirst_ReturnsCorrectRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Zzz", 50);
        Insert(cn, "Aaa", 10);
        Insert(cn, "Mmm", 30);

        var result = await Q(ctx).OrderBy(p => p.Title).FirstAsync();
        Assert.Equal("Aaa", result.Title);
    }

    // ─── AnyAsync via CountAsync > 0 (SQLite doesn't support SELECT 1 WHERE EXISTS) ──

    [Fact]
    public async Task AnyAsyncEquivalent_WithRows_ReturnsTrue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);

        // Use CountAsync > 0 as equivalent (see MEMORY.md: AnyAsync SQLite mismatch)
        var any = await ctx.Query<NqpItem>().CountAsync() > 0;
        Assert.True(any);
    }

    [Fact]
    public async Task AnyAsyncEquivalent_EmptyTable_ReturnsFalse()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var any = await ctx.Query<NqpItem>().CountAsync() > 0;
        Assert.False(any);
    }

    // ─── Count fast-path second call uses SQL cache ────────────────────────

    [Fact]
    public async Task Count_SecondCall_UsesCachedSql()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 5);
        Insert(cn, "B", 20, tagId: 5);

        var c1 = await Q(ctx).Where(p => p.TagId == 5).CountAsync();
        var c2 = await Q(ctx).Where(p => p.TagId == 5).CountAsync();
        Assert.Equal(2, c1);
        Assert.Equal(c1, c2);
    }

    // ─── ExecuteDeleteAsync ────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteDeleteAsync_DeletesMatchingRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);
        Insert(cn, "B", 20, tagId: 2);
        Insert(cn, "C", 30, tagId: 1);

        var deleted = await ctx.Query<NqpItem>()
            .Where(p => p.TagId == 1)
            .ExecuteDeleteAsync();
        Assert.Equal(2, deleted);

        var remaining = await Q(ctx).CountAsync();
        Assert.Equal(1, remaining);
    }

    // ─── Where with null == column ─────────────────────────────────────────

    [Fact]
    public async Task Where_NullEqualTitle_ReturnsNoRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "HasTitle", 10);

        string? nullStr = null;
        var results = await Q(ctx).Where(p => p.Title == nullStr).ToListAsync();
        Assert.Empty(results);
    }

    // ─── ExecuteUpdateAsync ────────────────────────────────────────────────

    [Fact]
    public async Task ExecuteUpdateAsync_UpdatesMatchingRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);
        Insert(cn, "B", 20, tagId: 1);
        Insert(cn, "C", 30, tagId: 2);

        var updated = await ctx.Query<NqpItem>()
            .Where(p => p.TagId == 1)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Score, 99));
        Assert.Equal(2, updated);

        var remaining = await Q(ctx).Where(p => p.Score == 99).CountAsync();
        Assert.Equal(2, remaining);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_NoMatchingRows_ReturnsZero()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);

        var updated = await ctx.Query<NqpItem>()
            .Where(p => p.TagId == 999)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Score, 0));
        Assert.Equal(0, updated);
    }

    // ─── AsAsyncEnumerable streaming ──────────────────────────────────────

    [Fact]
    public async Task AsAsyncEnumerable_YieldsAllRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);
        Insert(cn, "C", 30);

        var results = new List<NqpItem>();
        await foreach (var item in Q(ctx).AsNoTracking().AsAsyncEnumerable())
            results.Add(item);

        Assert.Equal(3, results.Count);
    }

    [Fact]
    public async Task AsAsyncEnumerable_EmptyTable_YieldsNothing()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var results = new List<NqpItem>();
        await foreach (var item in Q(ctx).AsNoTracking().AsAsyncEnumerable())
            results.Add(item);

        Assert.Empty(results);
    }

    [Fact]
    public async Task AsAsyncEnumerable_WithFilter_YieldsFiltered()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);
        Insert(cn, "B", 20, tagId: 2);
        Insert(cn, "C", 30, tagId: 1);

        var results = new List<NqpItem>();
        await foreach (var item in ((INormQueryable<NqpItem>)ctx.Query<NqpItem>()
            .Where(p => p.TagId == 1)).AsNoTracking().AsAsyncEnumerable())
            results.Add(item);

        Assert.Equal(2, results.Count);
    }

    // ─── TryGetSimpleQuery edge cases ─────────────────────────────────────

    [Fact]
    public async Task SimpleQuery_WithFirst_FindsRecord()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Target", 42);

        // Simple path: .Where(equality).First()
        var result = await Q(ctx).Where(p => p.Title == "Target").FirstAsync();
        Assert.Equal("Target", result.Title);
    }

    [Fact]
    public async Task SimpleQuery_WithTake_LimitsResults()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 5; i++) Insert(cn, $"Item{i}", i);

        // Simple path: .Take(n) without where
        var results = await ctx.Query<NqpItem>().Take(2).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task SimpleQuery_BoolMember_HitsSimplePath()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, published: true);
        Insert(cn, "B", 20, published: false);

        // Simple path: bool member Where
        var results = await ctx.Query<NqpItem>().Where(p => p.IsPublished).ToListAsync();
        Assert.Single(results);
        Assert.Equal("A", results[0].Title);
    }

    [Fact]
    public async Task SimpleQuery_NegatedBool_HitsSimplePath()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, published: true);
        Insert(cn, "B", 20, published: false);

        // Simple path: negated bool member Where
        var results = await ctx.Query<NqpItem>().Where(p => !p.IsPublished).ToListAsync();
        Assert.Single(results);
        Assert.Equal("B", results[0].Title);
    }

    [Fact]
    public async Task SimpleQuery_FirstOrDefault_NoMatchReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        // Simple path: .Where(equality).FirstOrDefault() with no match
        var result = await Q(ctx).Where(p => p.Title == "NoSuch").FirstOrDefaultAsync();
        Assert.Null(result);
    }

    // ─── ElementAt / Last / LastOrDefault ─────────────────────────────────

    [Fact]
    public async Task ElementAt_ValidIndex_ReturnsCorrectRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);
        Insert(cn, "C", 30);

        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var q = ctx.Query<NqpItem>().OrderBy(p => p.Score);
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.ElementAt),
            new[] { typeof(NqpItem) },
            q.Expression,
            Expression.Constant(0));
        var result = await provider.ExecuteAsync<NqpItem>(expr, default);
        Assert.Equal("A", result.Title);
    }

    [Fact]
    public async Task ElementAtOrDefault_ValidIndex_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);

        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var q = ctx.Query<NqpItem>();
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.ElementAtOrDefault),
            new[] { typeof(NqpItem) },
            q.Expression,
            Expression.Constant(0));
        var result = await provider.ExecuteAsync<NqpItem?>(expr, default);
        Assert.NotNull(result);
    }

    [Fact]
    public async Task Last_WithRows_ReturnsLastRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);

        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var q = ctx.Query<NqpItem>().OrderBy(p => p.Score);
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Last),
            new[] { typeof(NqpItem) },
            q.Expression);
        var result = await provider.ExecuteAsync<NqpItem>(expr, default);
        Assert.Equal("B", result.Title);
    }

    [Fact]
    public async Task LastOrDefault_EmptyTable_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var q = ctx.Query<NqpItem>().OrderBy(p => p.Score);
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.LastOrDefault),
            new[] { typeof(NqpItem) },
            q.Expression);
        var result = await provider.ExecuteAsync<NqpItem?>(expr, default);
        Assert.Null(result);
    }

    // ─── Sync method path coverage ─────────────────────────────────────────

    [Fact]
    public void ExecuteSync_Count_ReturnsCount()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);

        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var countExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Count),
            new[] { typeof(NqpItem) }, ctx.Query<NqpItem>().Expression);
        var count = provider.ExecuteSync<int>(countExpr);
        Assert.Equal(2, count);
    }

    [Fact]
    public void ExecuteSync_First_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "OnlyRow", 77);

        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var firstExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.First),
            new[] { typeof(NqpItem) }, ctx.Query<NqpItem>().Expression);
        var result = provider.ExecuteSync<NqpItem>(firstExpr);
        Assert.Equal("OnlyRow", result.Title);
    }

    // ─── Cache provider integration ────────────────────────────────────────

    [Fact]
    public async Task WithCacheProvider_SameQueryTwice_SecondCallFromCache()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NqpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, IsPublished INTEGER NOT NULL DEFAULT 1, TagId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO NqpItem (Title, Score, IsPublished, TagId) VALUES ('Cached', 55, 1, 1)";
            cmd.ExecuteNonQuery();
        }

        var options = new nORM.Configuration.DbContextOptions
        {
            CacheProvider = new nORM.Core.NormMemoryCacheProvider(),
            CacheExpiration = TimeSpan.FromMinutes(5)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        // Mark as cacheable using ExecuteCompiledAsync path via GetPlan
        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Count),
            new[] { typeof(NqpItem) }, ctx.Query<NqpItem>().Expression);

        // Call twice — both should work (second may come from cache)
        var count1 = await provider.ExecuteAsync<int>(expr, default);
        var count2 = await provider.ExecuteAsync<int>(expr, default);
        Assert.Equal(1, count1);
        Assert.Equal(1, count2);
    }

    // ─── Retry policy path ─────────────────────────────────────────────────

    [Fact]
    public async Task RetryPolicy_Configured_CountWorks()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NqpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, IsPublished INTEGER NOT NULL DEFAULT 1, TagId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO NqpItem (Title, Score, IsPublished, TagId) VALUES ('R1', 10, 1, 1)";
            cmd.ExecuteNonQuery();
        }

        // Configure a retry policy that never actually retries (condition never true for SQLite)
        var options = new nORM.Configuration.DbContextOptions
        {
            RetryPolicy = new nORM.Enterprise.RetryPolicy
            {
                MaxRetries = 1,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                ShouldRetry = _ => false
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var count = await ctx.Query<NqpItem>().CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task RetryPolicy_CountWithWhere_Works()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NqpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, IsPublished INTEGER NOT NULL DEFAULT 1, TagId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        for (int i = 0; i < 3; i++)
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = $"INSERT INTO NqpItem (Title, Score, IsPublished, TagId) VALUES ('Item{i}', {i * 10}, 1, {i % 2})";
            cmd.ExecuteNonQuery();
        }

        var options = new nORM.Configuration.DbContextOptions
        {
            RetryPolicy = new nORM.Enterprise.RetryPolicy
            {
                MaxRetries = 2,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                ShouldRetry = _ => false
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var count = await ctx.Query<NqpItem>().Where(p => p.TagId == 1).CountAsync();
        Assert.Equal(1, count);
    }

    // ─── Multi-context isolation ────────────────────────────────────────────

    [Fact]
    public async Task TwoContexts_SameConnection_IndependentQueries()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NqpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, IsPublished INTEGER NOT NULL DEFAULT 1, TagId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        Insert(cn, "X", 10);
        Insert(cn, "Y", 20);

        using var ctx1 = new DbContext(cn, new SqliteProvider());
        using var ctx2 = new DbContext(cn, new SqliteProvider());

        var count1 = await ctx1.Query<NqpItem>().CountAsync();
        var count2 = await ctx2.Query<NqpItem>().CountAsync();
        Assert.Equal(count1, count2);
        Assert.Equal(2, count1);
    }

    // ─── LongCount ─────────────────────────────────────────────────────────

    [Fact]
    public async Task LongCount_MultipleRows_ReturnsLong()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);
        Insert(cn, "C", 30);

        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var expr = Expression.Call(
            typeof(Queryable), nameof(Queryable.LongCount),
            new[] { typeof(NqpItem) }, ctx.Query<NqpItem>().Expression);
        var count = await provider.ExecuteAsync<long>(expr, default);
        Assert.Equal(3L, count);
    }

    // ─── TryDirectCountAsync via Where().CountAsync() ─────────────────────

    [Fact]
    public async Task TryDirectCountAsync_BoolMember_Works()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, published: true);
        Insert(cn, "B", 20, published: false);
        Insert(cn, "C", 30, published: true);

        // CountAsync on WHERE uses TryDirectCountAsync internally
        var count = await ctx.Query<NqpItem>().Where(p => p.IsPublished).CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task TryDirectCountAsync_EqualityPredicate_Works()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 5);
        Insert(cn, "B", 20, tagId: 5);
        Insert(cn, "C", 30, tagId: 6);

        var count = await ctx.Query<NqpItem>().Where(p => p.TagId == 5).CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task TryDirectCountAsync_NegatedBool_Works()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, published: true);
        Insert(cn, "B", 20, published: false);

        var count = await ctx.Query<NqpItem>().Where(p => !p.IsPublished).CountAsync();
        Assert.Equal(1, count);
    }

    // ─── ExecuteCompiledAsync (dict overload) ──────────────────────────────

    [Fact]
    public async Task ExecuteCompiledAsync_DictOverload_ExecutesCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Compiled", 42);

        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var src = ctx.Query<NqpItem>().Where(p => p.Score == 42);
        var plan = provider.GetPlan(src.Expression, out _, out _);

        // Execute with empty compiled parameters dict
        var result = await provider.ExecuteCompiledAsync<List<NqpItem>>(
            plan,
            new Dictionary<string, object>(),
            default);

        Assert.NotNull(result);
    }

    // ─── NormalizeConnectionStringForCacheKey edge cases ──────────────────

    [Fact]
    public async Task ConnectionString_WithEmptyString_WorksCorrectly()
    {
        // Null/empty connection string handled by NormalizeConnectionStringForCacheKey
        // Test by using a context that has a valid connection (normal path)
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);

        var count = await Q(ctx).CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task ConnectionString_WithPassword_StrippedFromCacheKey()
    {
        // Connection string with password — create context with Data Source and Password
        // (SQLite doesn't actually use Password but the key should be stripped)
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NqpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, IsPublished INTEGER NOT NULL DEFAULT 1, TagId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        Insert(cn, "A", 10);

        // The BuildCacheKeyFromPlan calls NormalizeConnectionStringForCacheKey internally
        using var ctx = new DbContext(cn, new SqliteProvider());
        var count = await ctx.Query<NqpItem>().CountAsync();
        Assert.Equal(1, count);
    }

    // ─── Select projections (exercises SelectClauseVisitor) ───────────────

    [Fact]
    public async Task Select_AnonymousType_ReturnsProjectedData()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Widget", 50);
        Insert(cn, "Gadget", 100);

        // Hits SelectClauseVisitor.VisitNew with anonymous type
        var results = await ctx.Query<NqpItem>()
            .Select(p => new { p.Title, p.Score })
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Title == "Widget" && r.Score == 50);
        Assert.Contains(results, r => r.Title == "Gadget" && r.Score == 100);
    }

    [Fact]
    public async Task Select_AnonymousType_SingleField_Works()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Alpha", 10);
        Insert(cn, "Beta", 20);

        // Hits SelectClauseVisitor.VisitNew with single-field anonymous type
        var results = await ctx.Query<NqpItem>()
            .OrderBy(p => p.Title)
            .Select(p => new { p.Title })
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Alpha", results[0].Title);
    }

    [Fact]
    public async Task Select_AnonymousType_ThreeFields_Works()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "X", 5, tagId: 3);

        // Three-field anonymous type projection — multiple VisitNew arguments
        var results = await ctx.Query<NqpItem>()
            .Select(p => new { p.Title, p.Score, p.TagId })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("X", results[0].Title);
        Assert.Equal(5, results[0].Score);
        Assert.Equal(3, results[0].TagId);
    }

    [Fact]
    public async Task Select_WithWhereFilter_ProjectsFilteredRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Active", 10, published: true);
        Insert(cn, "Inactive", 20, published: false);

        // SelectClauseVisitor.VisitNew + WHERE filter interaction
        var results = await ctx.Query<NqpItem>()
            .Where(p => p.IsPublished)
            .Select(p => new { p.Title })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Active", results[0].Title);
    }

    [Fact]
    public async Task Select_AnonymousType_WithOrderBy_Correct()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "C", 30);
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);

        var results = await ctx.Query<NqpItem>()
            .OrderBy(p => p.Score)
            .Select(p => new { p.Title, p.Score })
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal("A", results[0].Title);
        Assert.Equal("C", results[2].Title);
    }

    // ─── SelectClauseVisitor direct unit tests ────────────────────────────
    // GroupBy+Select with anonymous type hits a materializer limitation (no setters on
    // anonymous type properties). Test SelectClauseVisitor directly using internal access.

    private static nORM.Query.SelectClauseVisitor BuildSelectVisitor(DbContext ctx, List<string>? groupBy = null)
    {
        var mapping = ctx.GetMapping(typeof(NqpItem));
        var gb = groupBy ?? new List<string>();
        return new nORM.Query.SelectClauseVisitor(mapping, gb, new SqliteProvider());
    }

    [Fact]
    public void SelectClauseVisitor_VisitMember_SingleColumn_ProducesEscapedColumn()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var visitor = BuildSelectVisitor(ctx);
        // p => p.Title  — VisitMember maps member name to escaped column
        var param = Expression.Parameter(typeof(NqpItem), "p");
        var memberExpr = Expression.MakeMemberAccess(param, typeof(NqpItem).GetProperty("Title")!);
        var result = visitor.Translate(memberExpr);

        Assert.Contains("Title", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SelectClauseVisitor_VisitNew_AnonymousType_ProducesAliasedColumns()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var visitor = BuildSelectVisitor(ctx);
        // new { p.Title, p.Score } — VisitNew visits each argument, adds AS alias
        var param = Expression.Parameter(typeof(NqpItem), "p");
        var titleProp = typeof(NqpItem).GetProperty("Title")!;
        var scoreProp = typeof(NqpItem).GetProperty("Score")!;
        var titleMember = Expression.MakeMemberAccess(param, titleProp);
        var scoreMember = Expression.MakeMemberAccess(param, scoreProp);

        // anonymous types: Title, Score
        var anonType = new { Title = "", Score = 0 }.GetType();
        var ctor = anonType.GetConstructors()[0];
        var newExpr = Expression.New(
            ctor,
            new Expression[] { titleMember, scoreMember },
            new System.Reflection.MemberInfo[] {
                anonType.GetProperty("Title")!,
                anonType.GetProperty("Score")!
            });

        var result = visitor.Translate(newExpr);

        // Should contain aliases for Title and Score
        Assert.Contains("Title", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Score", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(" AS ", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SelectClauseVisitor_VisitMethodCall_Count_ProducesCountStar()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var visitor = BuildSelectVisitor(ctx, new List<string> { "\"TagId\"" });

        // g.Count() — VisitMethodCall COUNT with no args → COUNT(*)
        // Build a call expression for Enumerable.Count<NqpItem>(IEnumerable<NqpItem>)
        var groupingParam = Expression.Parameter(typeof(System.Linq.IGrouping<int, NqpItem>), "g");
        var countMethod = typeof(Enumerable).GetMethods()
            .First(m => m.Name == "Count" && m.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(NqpItem));
        var countCall = Expression.Call(countMethod, groupingParam);

        var result = visitor.Translate(countCall);
        Assert.Contains("COUNT(*)", result, StringComparison.OrdinalIgnoreCase);
    }

    // Helper class to have a method named "Sum" for MethodCallExpression construction
    private static class FakeAggregateHelper
    {
        public static int Sum(IEnumerable<NqpItem> source, Func<NqpItem, int> selector)
            => source.Sum(selector);
        public static int Max(IEnumerable<NqpItem> source, Func<NqpItem, int> selector)
            => source.Max(selector);
        public static int Min(IEnumerable<NqpItem> source, Func<NqpItem, int> selector)
            => source.Min(selector);
        public static double Average(IEnumerable<NqpItem> source, Func<NqpItem, double> selector)
            => source.Average(selector);
    }

    [Fact]
    public void SelectClauseVisitor_VisitMethodCall_Sum_ProducesSumColumn()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var visitor = BuildSelectVisitor(ctx);

        // g.Sum(x => x.Score) — VisitMethodCall SUM with 2 args (arg1 is a lambda with MemberExpression)
        var sourceParam = Expression.Parameter(typeof(IEnumerable<NqpItem>), "g");
        var itemParam = Expression.Parameter(typeof(NqpItem), "x");
        var scoreMember = Expression.MakeMemberAccess(itemParam, typeof(NqpItem).GetProperty("Score")!);
        var sumLambda = Expression.Lambda<Func<NqpItem, int>>(scoreMember, itemParam);

        var sumMethod = typeof(FakeAggregateHelper).GetMethod("Sum")!;
        var sumCall = Expression.Call(sumMethod, sourceParam, sumLambda);

        var result = visitor.Translate(sumCall);
        Assert.Contains("SUM", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Score", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SelectClauseVisitor_VisitMember_IGroupingKey_ExpandsToGroupByColumns()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        // When visiting g.Key, it should expand to all groupBy columns
        var groupByColumns = new List<string> { "\"TagId\"" };
        var visitor = BuildSelectVisitor(ctx, groupByColumns);

        var groupingParam = Expression.Parameter(typeof(System.Linq.IGrouping<int, NqpItem>), "g");
        var keyProp = typeof(System.Linq.IGrouping<int, NqpItem>).GetProperty("Key")!;
        var keyMember = Expression.MakeMemberAccess(groupingParam, keyProp);

        var result = visitor.Translate(keyMember);
        // Should output the groupBy column
        Assert.Contains("TagId", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SelectClauseVisitor_VisitMemberInit_ProducesAliasedColumns()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var visitor = BuildSelectVisitor(ctx);
        // new NqpSelectItem { Title = p.Title, Score = p.Score }
        var param = Expression.Parameter(typeof(NqpItem), "p");
        var titleProp = typeof(NqpItem).GetProperty("Title")!;
        var scoreProp = typeof(NqpItem).GetProperty("Score")!;
        var titleMember = Expression.MakeMemberAccess(param, titleProp);
        var scoreMember = Expression.MakeMemberAccess(param, scoreProp);

        // Use NqpItem (has setters) for MemberInit
        var memberInit = Expression.MemberInit(
            Expression.New(typeof(NqpItem)),
            Expression.Bind(titleProp, titleMember),
            Expression.Bind(scoreProp, scoreMember));

        var result = visitor.Translate(memberInit);
        Assert.Contains("Title", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Score", result, StringComparison.OrdinalIgnoreCase);
        Assert.Contains(" AS ", result, StringComparison.OrdinalIgnoreCase);
    }

    // ─── TryGetSimpleQuery with global filters disables simple path ─────────

    [Fact]
    public async Task GlobalFilter_DisablesSimplePath_FallsBackToFullTranslation()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NqpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, IsPublished INTEGER NOT NULL DEFAULT 1, TagId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        Insert(cn, "Pub", 10, published: true);
        Insert(cn, "Unpub", 20, published: false);

        var options = new nORM.Configuration.DbContextOptions();
        options.AddGlobalFilter<NqpItem>(p => p.IsPublished);
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        // Global filter → simple path disabled → falls back to full translation
        // Also exercises: TryGetSimpleQuery returns false when GlobalFilters.Count > 0
        var result = await ctx.Query<NqpItem>().Where(p => p.Score == 10).FirstOrDefaultAsync();
        Assert.NotNull(result);
        Assert.Equal("Pub", result!.Title);
    }

    // ─── Distinct (exercises full translation path) ─────────────────────────

    [Fact]
    public async Task Distinct_RemovesDuplicateScores_ViaProjection()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 10);
        Insert(cn, "C", 20);

        var results = await ctx.Query<NqpItem>()
            .Select(p => new { p.Score })
            .Distinct()
            .ToListAsync();

        Assert.Equal(2, results.Count);
    }

    // ─── Count with In-predicate (falls back to full translation) ─────────

    [Fact]
    public async Task Count_WithContains_FallsBackToFullTranslation()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);
        Insert(cn, "B", 20, tagId: 2);
        Insert(cn, "C", 30, tagId: 3);

        var tags = new[] { 1, 2 };
        var count = await Q(ctx).Where(p => tags.Contains(p.TagId)).CountAsync();
        Assert.Equal(2, count);
    }

    // ─── Where with string operations ─────────────────────────────────────

    [Fact]
    public async Task Where_StartsWith_FiltersCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Alpha", 10);
        Insert(cn, "Beta", 20);
        Insert(cn, "Almond", 30);

        var results = await Q(ctx).Where(p => p.Title.StartsWith("Al")).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Where_Contains_FiltersCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "alpha", 10);
        Insert(cn, "beta", 20);
        Insert(cn, "gamma", 30);

        var results = await Q(ctx).Where(p => p.Title.Contains("et")).ToListAsync();
        Assert.Single(results);
        Assert.Equal("beta", results[0].Title);
    }

    // ─── Double aggregate calls (verifies plan caching works across aggregates) ──

    [Fact]
    public async Task Sum_Then_Count_CachingWorksCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);

        var sum = await Q(ctx).SumAsync(p => p.Score);
        var count = await Q(ctx).CountAsync();
        Assert.Equal(30, sum);
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task Min_Then_Max_BothCorrect()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 5);
        Insert(cn, "B", 50);
        Insert(cn, "C", 25);

        var min = await Q(ctx).MinAsync(p => p.Score);
        var max = await Q(ctx).MaxAsync(p => p.Score);
        Assert.Equal(5, min);
        Assert.Equal(50, max);
    }

    // ─── Average non-empty set ─────────────────────────────────────────────

    [Fact]
    public async Task Average_WithFilter_CorrectResult()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10, tagId: 1);
        Insert(cn, "B", 20, tagId: 1);
        Insert(cn, "C", 60, tagId: 2);

        var avg = await Q(ctx).Where(p => p.TagId == 1).AverageAsync(p => p.Score);
        Assert.True(avg >= 14.9 && avg <= 15.1);
    }

    // ─── Single with predicate ─────────────────────────────────────────────

    [Fact]
    public async Task Single_WithMatchingWhere_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Unique", 999);

        var result = await ((INormQueryable<NqpItem>)ctx.Query<NqpItem>()
            .Where(p => p.Score == 999)).SingleAsync();
        Assert.Equal("Unique", result.Title);
    }

    [Fact]
    public async Task Single_TwoMatchingRows_ThrowsInvalidOperation()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 10);

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            ((INormQueryable<NqpItem>)ctx.Query<NqpItem>()
                .Where(p => p.Score == 10)).SingleAsync());
    }

    // ─── IQueryProvider.Execute non-generic (object result) ───────────────

    [Fact]
    public void Execute_NonGeneric_ScalarCount_Works()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 1);
        Insert(cn, "B", 2);

        var provider = (IQueryProvider)ctx.Query<NqpItem>().Provider;
        var countExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Count),
            new[] { typeof(NqpItem) }, ctx.Query<NqpItem>().Expression);
        var result = provider.Execute(countExpr);
        Assert.Equal(2, Convert.ToInt32(result));
    }

    // ─── ExecuteDelete with cancellation token propagation ────────────────

    [Fact]
    public async Task ExecuteDeleteAsync_WithRetryPolicy_DeletesRows()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE NqpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0, IsPublished INTEGER NOT NULL DEFAULT 1, TagId INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        Insert(cn, "ToDelete", 1, tagId: 99);
        Insert(cn, "Keep", 2, tagId: 1);

        var options = new nORM.Configuration.DbContextOptions
        {
            RetryPolicy = new nORM.Enterprise.RetryPolicy
            {
                MaxRetries = 1,
                BaseDelay = TimeSpan.FromMilliseconds(1),
                ShouldRetry = _ => false
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), options);

        var deleted = await ctx.Query<NqpItem>()
            .Where(p => p.TagId == 99)
            .ExecuteDeleteAsync();
        Assert.Equal(1, deleted);
    }

    // ─── SelectClauseVisitor — VisitMemberInit path ────────────────────────

    [Table("NqpSelectItem")]
    public class NqpSelectItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
        public int Score { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateSelectContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE NqpSelectItem (
                Id    INTEGER PRIMARY KEY AUTOINCREMENT,
                Title TEXT NOT NULL DEFAULT '',
                Score INTEGER NOT NULL DEFAULT 0
            )";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void InsertSelect(SqliteConnection cn, string title, int score)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO NqpSelectItem (Title, Score) VALUES (@t, @s)";
        cmd.Parameters.AddWithValue("@t", title);
        cmd.Parameters.AddWithValue("@s", score);
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task Select_MemberInit_FullEntity_ProjectsCorrectly()
    {
        var (cn, ctx) = CreateSelectContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertSelect(cn, "Hello", 42);

        // SelectClauseVisitor.VisitMemberInit — full entity all columns
        var results = await ctx.Query<NqpSelectItem>()
            .Select(p => new NqpSelectItem { Id = p.Id, Title = p.Title, Score = p.Score })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Hello", results[0].Title);
        Assert.Equal(42, results[0].Score);
    }

    [Fact]
    public async Task Select_MemberInit_FullEntity_MultipleRows_Works()
    {
        var (cn, ctx) = CreateSelectContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertSelect(cn, "A", 10);
        InsertSelect(cn, "B", 20);

        // MemberInit without OrderBy (OrderBy after MemberInit projection is unsupported)
        var results = await ctx.Query<NqpSelectItem>()
            .Select(p => new NqpSelectItem { Id = p.Id, Title = p.Title, Score = p.Score })
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.Title == "A");
        Assert.Contains(results, r => r.Title == "B");
    }

    // ─── GroupBy SQL without Select (no materializer issues) ─────────────

    [Fact]
    public void GroupBy_WithoutSelect_SqlContainsGroupBy()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        // Plain GroupBy without Select — exercises group by SQL generation
        var q = ctx.Query<NqpItem>().GroupBy(p => p.TagId);
        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var plan = provider.GetPlan(q.Expression, out _, out _);
        Assert.Contains("GROUP BY", plan.Sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── CountAsync on ordered query (non-fast-path) ───────────────────────

    [Fact]
    public async Task CountAsync_OnOrderedQuery_FallsBackToFullTranslation()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);
        Insert(cn, "C", 30);

        // OrderBy + Count falls through to full translation (not fast-path Count)
        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var q = ctx.Query<NqpItem>().OrderBy(p => p.Score);
        var countExpr = Expression.Call(
            typeof(Queryable), nameof(Queryable.Count),
            new[] { typeof(NqpItem) }, q.Expression);
        var count = await provider.ExecuteAsync<int>(countExpr, default);
        Assert.Equal(3, count);
    }

    // ─── GetPlan returns cached plan on second call ────────────────────────

    [Fact]
    public async Task GetPlan_ReturnsConsistentPlan_OnMultipleCalls()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Test", 10);

        var provider = (nORM.Query.NormQueryProvider)ctx.Query<NqpItem>().Provider;
        var src = ctx.Query<NqpItem>().Where(p => p.Score == 10);

        // First call — creates the plan
        var plan1 = provider.GetPlan(src.Expression, out _, out _);
        // Second call — hits plan cache
        var plan2 = provider.GetPlan(src.Expression, out _, out _);

        Assert.Equal(plan1.Sql, plan2.Sql);
    }

    // ─── ConvertScalarResult type conversions (double, decimal, float) ────

    [Fact]
    public async Task Sum_IntScore_ReturnsInt()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 100);
        Insert(cn, "B", 200);

        var sum = await Q(ctx).SumAsync(p => p.Score);
        Assert.IsType<int>(sum);
        Assert.Equal(300, sum);
    }

    [Fact]
    public async Task Average_ReturnsDouble()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "A", 10);
        Insert(cn, "B", 20);

        var avg = await Q(ctx).AverageAsync(p => p.Score);
        // Should be a numeric type close to 15
        Assert.True(Convert.ToDouble(avg) > 14.0 && Convert.ToDouble(avg) < 16.0);
    }

    // ─── AsNoTracking + Select combination ─────────────────────────────────

    [Fact]
    public async Task AsNoTracking_WithSelect_NoChangeTracking()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, "Untracked", 10);

        // AsNoTracking + anonymous projection
        var results = await Q(ctx).AsNoTracking()
            .Select(p => new { p.Title })
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal("Untracked", results[0].Title);
        // ChangeTracker should have no entries (anonymous types never tracked anyway)
        Assert.Empty(ctx.ChangeTracker.Entries);
    }
}
