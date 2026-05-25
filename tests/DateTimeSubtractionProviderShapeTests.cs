using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `Select(p =&gt; (p.End - p.Start).TotalSeconds)` (and
/// the direct DateTime-minus-DateTime projection that materializes as
/// TimeSpan). SCV used to hardcode SQLite's `julianday(...)` for the
/// REAL-seconds computation -- non-SQLite providers need their native
/// date-difference function (DATEDIFF_BIG / EXTRACT EPOCH /
/// TIMESTAMPDIFF). Routes through the existing
/// GetDateTimeDifferenceSecondsSql hook now.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeSubtractionProviderShapeTests : TestBase
{
    private sealed class DtsItem
    {
        public int Id { get; set; }
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_DateTime_diff_TotalSeconds_uses_provider_specific_date_diff(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        Expression<Func<DtsItem, bool>> expr = e => (e.End - e.Start).TotalSeconds > 60.0;
        var (sql, _) = Translate<DtsItem>(expr, connection, provider);

        var endCol = $"{provider.Escape("T0")}.{provider.Escape("End")}";
        var startCol = $"{provider.Escape("T0")}.{provider.Escape("Start")}";
        var expectedDiff = provider.GetDateTimeDifferenceSecondsSql(endCol, startCol);
        Assert.Contains(expectedDiff, sql);
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("julianday(", sql);
    }
}
