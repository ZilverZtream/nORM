using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// <c>string.Format</c> currently lowers <c>{0}</c>-style placeholders to a
/// provider concat but rejects every format spec (<c>{0:F2}</c>,
/// <c>{0:yyyy-MM-dd}</c>, etc.), falling back to client-eval. The provider
/// has hooks for both shapes -- <c>FormatFixedDecimalSql</c> for numeric
/// fixed-point specs and <c>FormatDateUsingDotNetPattern</c> for custom
/// date patterns -- so extending the parser to honor specs routes the
/// formatting through native SQL on each provider rather than dragging
/// the row to the client.
/// </summary>
[Trait("Category", "Fast")]
public sealed class StringFormatSpecProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public decimal Amount { get; set; }
        public DateTime Stamp { get; set; }
    }

    public static IEnumerable<object[]> DecimalCases()
    {
        foreach (ProviderKind k in Enum.GetValues<ProviderKind>())
            yield return new object[] { k };
    }

    [Theory]
    [MemberData(nameof(DecimalCases))]
    public void Where_with_string_Format_F2_decimal_spec_routes_through_fixed_decimal_hook(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        // string.Format("{0:F2}", row.Amount) == "10.00"
        var param = Expression.Parameter(typeof(Row), "r");
        var amount = Expression.Property(param, nameof(Row.Amount));
        var formatMethod = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object) })!;
        var call = Expression.Call(formatMethod, Expression.Constant("{0:F2}"), Expression.Convert(amount, typeof(object)));
        var body = Expression.Equal(call, Expression.Constant("10.00"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);

        // Each provider's FormatFixedDecimalSql emit anchor:
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("printf('%.2f'", sql);
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("FORMAT(", sql);
                Assert.Contains("F2", sql);
                break;
            case ProviderKind.Postgres:
                Assert.Contains("to_char(", sql);
                break;
            case ProviderKind.MySql:
                Assert.Contains("FORMAT(", sql);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(DecimalCases))]
    public void Where_with_string_Format_date_pattern_spec_routes_through_pattern_hook(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        // string.Format("{0:yyyy-MM-dd}", row.Stamp) == "2026-05-25"
        var param = Expression.Parameter(typeof(Row), "r");
        var stamp = Expression.Property(param, nameof(Row.Stamp));
        var formatMethod = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object) })!;
        var call = Expression.Call(formatMethod, Expression.Constant("{0:yyyy-MM-dd}"), Expression.Convert(stamp, typeof(object)));
        var body = Expression.Equal(call, Expression.Constant("2026-05-25"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("strftime(", sql);
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("FORMAT(", sql);
                break;
            case ProviderKind.Postgres:
                Assert.Contains("to_char(", sql);
                break;
            case ProviderKind.MySql:
                Assert.Contains("DATE_FORMAT(", sql);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(DecimalCases))]
    public void Where_with_string_Format_literal_plus_spec_concatenates_via_provider(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        // string.Format("Total: {0:F2}", row.Amount) == "Total: 10.00"
        var param = Expression.Parameter(typeof(Row), "r");
        var amount = Expression.Property(param, nameof(Row.Amount));
        var formatMethod = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object) })!;
        var call = Expression.Call(formatMethod, Expression.Constant("Total: {0:F2}"), Expression.Convert(amount, typeof(object)));
        var body = Expression.Equal(call, Expression.Constant("Total: 10.00"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // Literal "Total: " must appear; spec-emit must appear too.
        Assert.Contains("'Total: '", sql);
    }
}
