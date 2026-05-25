using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// <c>Enum.GetName(typeof(T), value)</c> previously had no translation
/// path -- it threw NormUnsupportedFeatureException, even though the
/// already-existing BuildEnumToStringCase helper produces the right
/// SQL shape (CASE WHEN col = 0 THEN 'A' WHEN col = 1 THEN 'B' ...).
/// Route the static method to the same emit.
/// </summary>
[Trait("Category", "Fast")]
public sealed class EnumGetNameProviderShapeTests : TestBase
{
    public enum Status { Active = 0, Cancelled = 1, Done = 2 }

    private sealed class Row
    {
        public int Id { get; set; }
        public Status Status { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Where_with_Enum_GetName_emits_CASE_WHEN_per_value(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var method = typeof(Enum).GetMethod(nameof(Enum.GetName), new[] { typeof(Type), typeof(object) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var status = Expression.Property(param, nameof(Row.Status));
        var call = Expression.Call(method, Expression.Constant(typeof(Status)), Expression.Convert(status, typeof(object)));
        var body = Expression.Equal(call, Expression.Constant("Active"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains("CASE", sql);
        // Each defined value name must appear as a WHEN-arm literal.
        Assert.Contains("'Active'", sql);
        Assert.Contains("'Cancelled'", sql);
        Assert.Contains("'Done'", sql);
    }
}
