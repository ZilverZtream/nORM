using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// <c>string.Join(separator, params string[])</c> emit was hard-coded to
/// use the <c>||</c> concat operator on both the WHERE side
/// (ExpressionToSqlVisitor) and the projection side (SelectClauseVisitor).
/// <c>||</c> is valid on SQLite and PostgreSQL but NOT SQL Server (uses
/// <c>+</c> for string concat) or MySQL (uses <c>CONCAT(...)</c> -- there
/// is no infix operator). The translator must route the concat through
/// each provider's <c>GetConcatSql</c> hook so the emitted SQL is
/// portable.
/// </summary>
[Trait("Category", "Fast")]
public sealed class StringJoinProviderConcatShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public string First { get; set; } = string.Empty;
        public string Last { get; set; } = string.Empty;
    }

    public static IEnumerable<object[]> Providers()
    {
        yield return new object[] { ProviderKind.Sqlite };
        yield return new object[] { ProviderKind.SqlServer };
        yield return new object[] { ProviderKind.Postgres };
        yield return new object[] { ProviderKind.MySql };
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Where_with_string_Join_uses_provider_concat_primitive(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        // string.Join(" ", row.First, row.Last) == "Ada Lovelace"
        var param = Expression.Parameter(typeof(Row), "r");
        var first = Expression.Property(param, nameof(Row.First));
        var last = Expression.Property(param, nameof(Row.Last));
        var joinMethod = typeof(string).GetMethod(nameof(string.Join), new[] { typeof(string), typeof(string[]) })!;
        var arr = Expression.NewArrayInit(typeof(string), first, last);
        var call = Expression.Call(joinMethod, Expression.Constant(" "), arr);
        var body = Expression.Equal(call, Expression.Constant("Ada Lovelace"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);

        // SQLite overrides GetConcatSql to use ||; the other three providers
        // fall through to the DatabaseProvider default CONCAT(a, b) -- which
        // is valid SQL on SqlServer (2012+), PostgreSQL (9.1+), and MySQL.
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("||", sql);
                Assert.DoesNotContain("CONCAT(", sql);
                break;
            case ProviderKind.SqlServer:
            case ProviderKind.Postgres:
            case ProviderKind.MySql:
                Assert.Contains("CONCAT(", sql);
                Assert.DoesNotContain("||", sql);
                break;
        }
    }
}
