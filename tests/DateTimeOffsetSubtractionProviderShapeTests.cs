using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `Select(p =&gt; (p.End - p.Start).TotalSeconds)` over
/// DateTimeOffset columns. Sister of DateTime subtraction (3f99e62
/// routed through GetDateTimeDifferenceSecondsSql). Verify the same
/// path applies to DateTimeOffset.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeOffsetSubtractionProviderShapeTests : TestBase
{
    private sealed class DtosItem
    {
        public int Id { get; set; }
        public DateTimeOffset Start { get; set; }
        public DateTimeOffset End { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_DateTimeOffset_diff_TotalSeconds_uses_provider_date_diff(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        Expression<Func<DtosItem, bool>> expr = e => (e.End - e.Start).TotalSeconds > 60.0;
        var (sql, _) = Translate<DtosItem>(expr, connection, provider);

        var endCol = $"{provider.Escape("T0")}.{provider.Escape("End")}";
        var startCol = $"{provider.Escape("T0")}.{provider.Escape("Start")}";
        var expectedDiff = provider.GetDateTimeDifferenceSecondsSql(endCol, startCol);
        Assert.Contains(expectedDiff, sql);
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("julianday(", sql);
    }
}
