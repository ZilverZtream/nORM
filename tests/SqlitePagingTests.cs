using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SQL-1: Verifies that SQLite paging SQL is generated correctly for Skip/Take combinations.
/// SQLite requires LIMIT when OFFSET is used; emitting OFFSET without LIMIT is invalid syntax.
/// </summary>
public class SqlitePagingTests : TestBase
{
    [Table("PagingEntity")]
    private class PagingEntity
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static (string Sql, System.Collections.Generic.Dictionary<string, object> Params, Type ElementType)
        TranslatePaging(System.Func<System.Linq.IQueryable<PagingEntity>, System.Linq.IQueryable<PagingEntity>> query)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var provider = new SqliteProvider();
        using var ctx = new DbContext(cn, provider);
        var q = query(ctx.Query<PagingEntity>());
        var expr = q.Expression;
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        var parameters = (System.Collections.Generic.IReadOnlyDictionary<string, object>)plan.GetType().GetProperty("Parameters")!.GetValue(plan)!;
        var elementType = (Type)plan.GetType().GetProperty("ElementType")!.GetValue(plan)!;
        return (sql, new System.Collections.Generic.Dictionary<string, object>(parameters), elementType);
    }

    [Fact]
    public void Skip_Without_Take_GeneratesLimitNegativeOne()
    {
        // SQL-1: Skip without Take must synthesize LIMIT -1 for SQLite
        var (sql, _, _) = TranslatePaging(q => q.Skip(10));

        Assert.Contains("LIMIT -1", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
        // Must NOT contain a numeric LIMIT other than -1
        // The pattern "LIMIT -1 OFFSET" should appear
        Assert.Contains("LIMIT -1 OFFSET", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Skip_And_Take_GeneratesLimitAndOffset()
    {
        // SQL-1: Skip + Take must produce LIMIT n OFFSET m
        var (sql, _, _) = TranslatePaging(q => q.Skip(10).Take(5));

        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
        // LIMIT -1 must NOT appear when Take is specified
        Assert.DoesNotContain("LIMIT -1", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Take_Without_Skip_GeneratesLimitOnly()
    {
        // SQL-1: Take without Skip must produce LIMIT n with no OFFSET
        var (sql, _, _) = TranslatePaging(q => q.Take(5));

        Assert.Contains("LIMIT", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("OFFSET", sql, StringComparison.OrdinalIgnoreCase);
    }
}
