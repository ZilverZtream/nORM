using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for DateTimeOffset arithmetic across providers. The
/// DateTime variants (c56cb4d / ae8063c / 3f99e62) cover DateTime;
/// verify DateTimeOffset takes the same provider-hook code paths
/// (FormatDateUsingDotNetPattern, AddSecondsToDateTimeSql,
/// AddTimeSpanColumnToDateTimeSql, GetDateTimeDifferenceSecondsSql)
/// since ETSV / SCV blocks all check for both DateTime and
/// DateTimeOffset.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeOffsetArithmeticProviderShapeTests : TestBase
{
    private sealed class DtoItem
    {
        public int Id { get; set; }
        public DateTimeOffset Stamp { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_DateTimeOffset_plus_constant_TimeSpan_uses_provider_specific_function(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var threshold = new DateTimeOffset(2026, 5, 25, 12, 0, 0, TimeSpan.Zero);
        var span = TimeSpan.FromHours(1);
        Expression<Func<DtoItem, bool>> expr = e => e.Stamp + span > threshold;
        var (sql, _) = Translate<DtoItem>(expr, connection, provider);

        var col = $"{provider.Escape("T0")}.{provider.Escape("Stamp")}";
        var expectedArith = provider.AddSecondsToDateTimeSql(col, "3600");
        Assert.NotNull(expectedArith);
        Assert.Contains(expectedArith!, sql);
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("strftime(", sql);
    }
}
