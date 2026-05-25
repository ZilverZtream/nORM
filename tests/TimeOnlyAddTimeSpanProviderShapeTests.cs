using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `Where(p =&gt; p.Start.Add(span) &gt; threshold)` where
/// p.Start is TimeOnly and span is a closure-captured constant TimeSpan.
/// Real implementation across 4 providers via the new
/// AddSecondsToTimeOnlySql provider hook. SqlServer needs CAST back to
/// TIME because DATEADD on TIME returns DATETIME.
/// </summary>
[Trait("Category", "Fast")]
public sealed class TimeOnlyAddTimeSpanProviderShapeTests : TestBase
{
    private sealed class TocItem
    {
        public int Id { get; set; }
        public TimeOnly Start { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_TimeOnly_Add_constant_TimeSpan_uses_provider_specific_function(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var threshold = new TimeOnly(13, 30, 0);
        var span = TimeSpan.FromHours(1);
        Expression<Func<TocItem, bool>> expr = e => e.Start.Add(span) > threshold;
        var (sql, _) = Translate<TocItem>(expr, connection, provider);

        var col = $"{provider.Escape("T0")}.{provider.Escape("Start")}";
        // TimeOnly.Add folds the constant TimeSpan to a literal total-seconds
        // value (3600 for 1 hour) and embeds inline -- assert against the
        // literal-fragment shape per provider.
        var expectedArith = provider.AddSecondsToTimeOnlySql(col, "3600");
        Assert.NotNull(expectedArith);
        Assert.Contains(expectedArith!, sql);
    }
}
