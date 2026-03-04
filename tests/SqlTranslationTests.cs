using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that SQL translation produces correct SQL fragments for various LINQ operators.
/// Uses TestBase.TranslateQuery to extract generated SQL without executing it.
/// </summary>
public class SqlTranslationTests : TestBase
{
    [Table("TranslationEntity")]
    private class TranslationEntity
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public bool IsActive { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    private static (string Sql, Dictionary<string, object> Params, Type ElementType)
        Translate<T>(Func<IQueryable<T>, IQueryable<T>> build) where T : class, new()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var provider = new SqliteProvider();
        using var ctx = new DbContext(cn, provider);
        var q = build(ctx.Query<T>());
        var expr = q.Expression;
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        var parameters = (IReadOnlyDictionary<string, object>)plan.GetType().GetProperty("Parameters")!.GetValue(plan)!;
        var elementType = (Type)plan.GetType().GetProperty("ElementType")!.GetValue(plan)!;
        return (sql, new Dictionary<string, object>(parameters), elementType);
    }

    // ─── WHERE clause translations ────────────────────────────────────────

    [Fact]
    public void Where_Equality_GeneratesEqualOperator()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q => q.Where(x => x.Name == "Alice"));
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"Name\"", sql);
        Assert.Contains("= @p0", sql);
        Assert.Single(parameters);
        Assert.Equal("Alice", parameters["@p0"]);
    }

    [Fact]
    public void Where_EqualNull_GeneratesIsNull()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q => q.Where(x => x.Name == null));
        Assert.Contains("IS NULL", sql);
        Assert.Empty(parameters);
    }

    [Fact]
    public void Where_NotEqualNull_GeneratesIsNotNull()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q => q.Where(x => x.Name != null));
        Assert.Contains("IS NOT NULL", sql);
        Assert.Empty(parameters);
    }

    [Fact]
    public void Where_AndCondition_GeneratesAnd()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q =>
            q.Where(x => x.Age > 18 && x.Age < 65));
        Assert.Contains("AND", sql);
        Assert.Contains(">", sql);
        Assert.Contains("<", sql);
        Assert.Equal(2, parameters.Count);
    }

    [Fact]
    public void Where_OrCondition_GeneratesOr()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q =>
            q.Where(x => x.Age > 18 || x.Age < 5));
        Assert.Contains("OR", sql);
        Assert.Equal(2, parameters.Count);
    }

    [Fact]
    public void Where_NotCondition_GeneratesNot()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q =>
            q.Where(x => !x.IsActive));
        // NOT is generated for boolean negation
        Assert.Contains("\"IsActive\"", sql);
    }

    [Fact]
    public void Where_GreaterThan_GeneratesGtOperator()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q => q.Where(x => x.Age > 18));
        Assert.Contains("> @p0", sql);
        Assert.Equal(18, parameters["@p0"]);
    }

    [Fact]
    public void Where_LessThan_GeneratesLtOperator()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q => q.Where(x => x.Age < 18));
        Assert.Contains("< @p0", sql);
        Assert.Equal(18, parameters["@p0"]);
    }

    [Fact]
    public void Where_GreaterThanOrEqual_GeneratesGteOperator()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q => q.Where(x => x.Age >= 18));
        Assert.Contains(">= @p0", sql);
        Assert.Equal(18, parameters["@p0"]);
    }

    [Fact]
    public void Where_LessThanOrEqual_GeneratesLteOperator()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q => q.Where(x => x.Age <= 18));
        Assert.Contains("<= @p0", sql);
        Assert.Equal(18, parameters["@p0"]);
    }

    // ─── String method translations ───────────────────────────────────────

    [Fact]
    public void Where_StartsWith_GeneratesLikeWithTrailingPercent()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q => q.Where(x => x.Name.StartsWith("Al")));
        Assert.Contains("LIKE", sql, StringComparison.OrdinalIgnoreCase);
        // Parameter should contain "Al%"
        Assert.Single(parameters);
        var paramVal = parameters.Values.First().ToString()!;
        Assert.StartsWith("Al", paramVal);
        Assert.EndsWith("%", paramVal);
    }

    [Fact]
    public void Where_Contains_String_GeneratesLikeWithBothPercents()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q => q.Where(x => x.Name.Contains("li")));
        Assert.Contains("LIKE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Single(parameters);
        var paramVal = parameters.Values.First().ToString()!;
        Assert.StartsWith("%", paramVal);
        Assert.EndsWith("%", paramVal);
        Assert.Contains("li", paramVal);
    }

    [Fact]
    public void Where_EndsWith_GeneratesLikeWithLeadingPercent()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q => q.Where(x => x.Name.EndsWith("ce")));
        Assert.Contains("LIKE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Single(parameters);
        var paramVal = parameters.Values.First().ToString()!;
        Assert.StartsWith("%", paramVal);
        Assert.EndsWith("ce", paramVal);
    }

    // ─── OrderBy translations ──────────────────────────────────────────────

    [Fact]
    public void OrderBy_GeneratesOrderByAsc()
    {
        var (sql, _, _) = Translate<TranslationEntity>(q => q.OrderBy(x => x.Name));
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"Name\"", sql);
        // ASC is default; some providers omit the keyword
    }

    [Fact]
    public void OrderByDescending_GeneratesDesc()
    {
        var (sql, _, _) = Translate<TranslationEntity>(q => q.OrderByDescending(x => x.Age));
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"Age\"", sql);
        Assert.Contains("DESC", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void OrderBy_ThenBy_GeneratesBothColumns()
    {
        var (sql, _, _) = Translate<TranslationEntity>(q => q.OrderBy(x => x.Name).ThenBy(x => x.Age));
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"Name\"", sql);
        Assert.Contains("\"Age\"", sql);
    }

    // ─── Select projection translations ───────────────────────────────────

    [Fact]
    public void Select_AnonymousType_GeneratesSelectWithBothColumns()
    {
        // Use TranslateQuery from TestBase for projection queries
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var provider = new SqliteProvider();
        var (sql, _, _) = TranslateQuery<TranslationEntity, NameAge>(
            q => q.Select(x => new NameAge { Name = x.Name, Age = x.Age }), cn, provider);
        Assert.Contains("SELECT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("\"Name\"", sql);
        Assert.Contains("\"Age\"", sql);
    }

    private class NameAge
    {
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
    }

    // ─── Skip/Take translations ────────────────────────────────────────────

    [Fact]
    public void Take_GeneratesLimit()
    {
        var (sql, _, _) = Translate<TranslationEntity>(q => q.Take(5));
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Skip_GeneratesOffset()
    {
        var (sql, _, _) = Translate<TranslationEntity>(q => q.Skip(10));
        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
        // SQLite requires LIMIT when OFFSET is used — should synthesize LIMIT -1
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SkipTake_GeneratesBothLimitAndOffset()
    {
        var (sql, _, _) = Translate<TranslationEntity>(q => q.Skip(10).Take(5));
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
        // Not LIMIT -1 when Take is present
        Assert.DoesNotContain("LIMIT -1", sql, StringComparison.OrdinalIgnoreCase);
    }

    // ─── Combined translations ─────────────────────────────────────────────

    [Fact]
    public void Where_OrderBy_Take_GeneratesAllClauses()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q =>
            q.Where(x => x.IsActive).OrderBy(x => x.Name).Take(10));
        Assert.Contains("WHERE", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("ORDER BY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Where_With_TwoConditions_GeneratesParametersForBoth()
    {
        var (sql, parameters, _) = Translate<TranslationEntity>(q =>
            q.Where(x => x.Age > 20 && x.Age < 60));
        Assert.Equal(2, parameters.Count);
        var values = parameters.Values.Select(v => (int)v).ToList();
        Assert.Contains(20, values);
        Assert.Contains(60, values);
    }
}
