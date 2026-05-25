using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// The string.Format parser previously rejected any alignment spec
/// (<c>{0,5}</c>, <c>{0,-5}</c>) and the call dropped to client-eval.
/// Per-provider PadLeft / PadRight already ship (3b8f7af); composing
/// the format-spec output with the alignment width via those primitives
/// keeps the projection server-side.
/// </summary>
[Trait("Category", "Fast")]
public sealed class StringFormatAlignmentProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public string Code { get; set; } = string.Empty;
        public decimal Amount { get; set; }
    }

    public static IEnumerable<object[]> Providers()
    {
        foreach (ProviderKind k in Enum.GetValues<ProviderKind>())
            yield return new object[] { k };
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Where_with_string_Format_right_aligned_positive_width_pads_left(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        // string.Format("{0,5}", row.Code) -- positive width = right-align,
        // padded on the LEFT to reach width 5.
        var param = Expression.Parameter(typeof(Row), "r");
        var code = Expression.Property(param, nameof(Row.Code));
        var formatMethod = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object) })!;
        var call = Expression.Call(formatMethod, Expression.Constant("{0,5}"), Expression.Convert(code, typeof(object)));
        var body = Expression.Equal(call, Expression.Constant("  abc"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // Provider's PadLeft emit shape:
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("zeroblob", sql); // SQLite's PadLeft uses replace(hex(zeroblob(...)), ...)
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("REPLICATE(", sql);
                break;
            case ProviderKind.Postgres:
            case ProviderKind.MySql:
                Assert.Contains("LPAD(", sql);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Where_with_string_Format_left_aligned_negative_width_pads_right(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        // string.Format("{0,-5}", row.Code) -- negative width = left-align,
        // padded on the RIGHT to reach width 5.
        var param = Expression.Parameter(typeof(Row), "r");
        var code = Expression.Property(param, nameof(Row.Code));
        var formatMethod = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object) })!;
        var call = Expression.Call(formatMethod, Expression.Constant("{0,-5}"), Expression.Convert(code, typeof(object)));
        var body = Expression.Equal(call, Expression.Constant("abc  "));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("zeroblob", sql);
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("REPLICATE(", sql);
                break;
            case ProviderKind.Postgres:
            case ProviderKind.MySql:
                Assert.Contains("RPAD(", sql);
                break;
        }
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void Where_with_string_Format_alignment_plus_format_spec_composes_both(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        // string.Format("{0,10:F2}", row.Amount) -- F2 formats first, then
        // right-align to width 10.
        var param = Expression.Parameter(typeof(Row), "r");
        var amount = Expression.Property(param, nameof(Row.Amount));
        var formatMethod = typeof(string).GetMethod(nameof(string.Format), new[] { typeof(string), typeof(object) })!;
        var call = Expression.Call(formatMethod, Expression.Constant("{0,10:F2}"), Expression.Convert(amount, typeof(object)));
        var body = Expression.Equal(call, Expression.Constant("     10.00"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        // Both the fixed-decimal hook AND the pad wrap must appear.
        switch (providerKind)
        {
            case ProviderKind.Sqlite:
                Assert.Contains("printf('%.2f'", sql);
                Assert.Contains("zeroblob", sql);
                break;
            case ProviderKind.SqlServer:
                Assert.Contains("F2", sql);
                Assert.Contains("REPLICATE(", sql);
                break;
            case ProviderKind.Postgres:
                Assert.Contains("to_char(", sql);
                Assert.Contains("LPAD(", sql);
                break;
            case ProviderKind.MySql:
                Assert.Contains("FORMAT(", sql);
                Assert.Contains("LPAD(", sql);
                break;
        }
    }
}
