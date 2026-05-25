using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `Where(p =&gt; p.Date.AddDays(7) &gt; threshold)` where
/// p.Date is DateOnly. Implementation work: new provider hook
/// AddDaysToDateOnlySql with per-provider native date arithmetic
/// (SQLite strftime modifier, SqlServer DATEADD, Postgres native +,
/// MySQL DATE_ADD).
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateOnlyAddDaysProviderShapeTests : TestBase
{
    private sealed class DoaItem
    {
        public int Id { get; set; }
        public DateOnly Date { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_DateOnly_AddDays_uses_provider_specific_function(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var threshold = new DateOnly(2026, 6, 1);
        Expression<Func<DoaItem, bool>> expr = e => e.Date.AddDays(7) > threshold;
        var (sql, _) = Translate<DoaItem>(expr, connection, provider);

        var col = $"{provider.Escape("T0")}.{provider.Escape("Date")}";
        // nORM parameterizes the literal 7 as @p0; assert the provider hook
        // emit shape with the parameter placeholder instead.
        var expectedArith = provider.AddDaysToDateOnlySql(col, "@p0");
        Assert.NotNull(expectedArith);
        Assert.Contains(expectedArith!, sql);
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("strftime(", sql);
    }
}
