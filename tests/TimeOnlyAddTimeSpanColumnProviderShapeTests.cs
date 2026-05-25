using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `Where(p =&gt; p.Start.Add(p.Duration) &gt; threshold)`
/// where Duration is a TimeSpan column (not a closure-folded constant).
/// 8aa8f14 supported the constant-TimeSpan path; this is the column-
/// TimeSpan sister via a new AddTimeSpanColumnToTimeOnlySql provider hook.
/// </summary>
[Trait("Category", "Fast")]
public sealed class TimeOnlyAddTimeSpanColumnProviderShapeTests : TestBase
{
    private sealed class TocItem
    {
        public int Id { get; set; }
        public TimeOnly Start { get; set; }
        public TimeSpan Duration { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_TimeOnly_Add_TimeSpan_column_uses_provider_specific_function(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var threshold = new TimeOnly(13, 30, 0);
        Expression<Func<TocItem, bool>> expr = e => e.Start.Add(e.Duration) > threshold;
        var (sql, _) = Translate<TocItem>(expr, connection, provider);

        var timeCol = $"{provider.Escape("T0")}.{provider.Escape("Start")}";
        var durCol = $"{provider.Escape("T0")}.{provider.Escape("Duration")}";
        var expectedArith = provider.AddTimeSpanColumnToTimeOnlySql(timeCol, durCol, subtract: false);
        Assert.NotNull(expectedArith);
        Assert.Contains(expectedArith!, sql);
    }
}
