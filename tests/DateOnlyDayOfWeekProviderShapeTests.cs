using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// DateOnly.DayOfWeek wasn't in any provider's DateOnly switch -- only
/// DateTime.DayOfWeek translated. The .NET property returns
/// <see cref="System.DayOfWeek"/> (0=Sun..6=Sat) on both DateTime and
/// DateOnly. Add a per-provider entry that mirrors the existing
/// DateTime.DayOfWeek emit, accounting for each engine's native day-of-
/// week semantics:
///   SQLite:    CAST(strftime('%w', col) AS INTEGER)
///   SQL Server: ((DATEPART(weekday, col) + @@DATEFIRST - 1) % 7)
///   Postgres:  EXTRACT(DOW FROM col)
///   MySQL:     (DAYOFWEEK(col) - 1)
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateOnlyDayOfWeekProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public DateOnly D { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite,    "strftime")]
    [InlineData(ProviderKind.SqlServer, "DATEPART")]
    [InlineData(ProviderKind.Postgres,  "EXTRACT(DOW")]
    [InlineData(ProviderKind.MySql,     "DAYOFWEEK(")]
    public void Where_with_DateOnly_DayOfWeek_translates_per_provider(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var d = Expression.Property(param, nameof(Row.D));
        var dow = Expression.Property(d, nameof(DateOnly.DayOfWeek));
        // .NET DayOfWeek enum: 0=Sun..6=Sat. Compare against Monday.
        var body = Expression.Equal(dow, Expression.Constant(DayOfWeek.Monday));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }
}
