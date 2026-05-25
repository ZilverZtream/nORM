using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Shape pin for `DateTime.AddMilliseconds(N)` and `DateTime.AddTicks(N)`
/// in WHERE. SQLite has both via strftime; SqlServer / Postgres / MySQL
/// previously missing -- this probe surfaces the gap and the fix lands
/// per-provider native shapes.
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeAddMillisecondsTicksProviderShapeTests : TestBase
{
    private sealed class DtmtItem
    {
        public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }

    public static IEnumerable<object[]> AllProviders()
    {
        foreach (ProviderKind kind in Enum.GetValues<ProviderKind>())
            yield return new object[] { kind };
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_DateTime_AddMilliseconds_translates_per_provider(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var threshold = new DateTime(2026, 1, 1);
        Expression<Func<DtmtItem, bool>> expr = e => e.Stamp.AddMilliseconds(500) > threshold;
        var (sql, _) = Translate<DtmtItem>(expr, connection, provider);

        // Sanity: SQL got produced, and on non-SQLite it shouldn't contain strftime.
        Assert.Contains("@", sql); // parameter was bound
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("strftime(", sql);
    }

    [Theory]
    [MemberData(nameof(AllProviders))]
    public void Where_with_DateTime_AddTicks_translates_per_provider(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var threshold = new DateTime(2026, 1, 1);
        Expression<Func<DtmtItem, bool>> expr = e => e.Stamp.AddTicks(10_000_000L) > threshold;
        var (sql, _) = Translate<DtmtItem>(expr, connection, provider);
        Assert.Contains("@", sql);
        if (providerKind != ProviderKind.Sqlite)
            Assert.DoesNotContain("strftime(", sql);
    }
}
