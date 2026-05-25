using System;
using System.Linq;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Companion of 5f63a8e. The .NET 5+ generic form
/// <c>Enum.GetName&lt;T&gt;(T value)</c> takes the value directly without the
/// typeof wrapper. The legacy 2-arg form Enum.GetName(typeof(T), object)
/// already translates; the generic 1-arg form fell through to the
/// generic method dispatch and threw NormUnsupportedFeatureException.
/// </summary>
[Trait("Category", "Fast")]
public sealed class EnumGetNameGenericProviderShapeTests : TestBase
{
    public enum Severity { Low = 0, Medium = 1, High = 2 }

    private sealed class Row
    {
        public int Id { get; set; }
        public Severity Level { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite)]
    [InlineData(ProviderKind.SqlServer)]
    [InlineData(ProviderKind.Postgres)]
    [InlineData(ProviderKind.MySql)]
    public void Where_with_generic_Enum_GetName_emits_CASE_WHEN_per_value(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var genericMethod = typeof(Enum).GetMethods()
            .First(m => m.Name == nameof(Enum.GetName)
                        && m.IsGenericMethodDefinition
                        && m.GetParameters().Length == 1)
            .MakeGenericMethod(typeof(Severity));
        var param = Expression.Parameter(typeof(Row), "r");
        var level = Expression.Property(param, nameof(Row.Level));
        var call = Expression.Call(genericMethod, level);
        var body = Expression.Equal(call, Expression.Constant("Low"));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains("CASE", sql);
        Assert.Contains("'Low'", sql);
        Assert.Contains("'Medium'", sql);
        Assert.Contains("'High'", sql);
    }
}
