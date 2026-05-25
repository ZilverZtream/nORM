using System;
using System.Linq.Expressions;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Companion of 8875ae5. The 6-arg DateTime constructor
/// <c>new DateTime(year, month, day, hour, minute, second)</c> with
/// column args also fell through the const-fold path. Reuse the per-
/// provider DateTime-from-parts primitive with non-zero h/m/s.
///
///   SQL Server: DATETIME2FROMPARTS(y, m, d, h, mi, s, 0, 7)
///   PostgreSQL: MAKE_TIMESTAMP(y, m, d, h, mi, s)
///   MySQL:      STR_TO_DATE(CONCAT_WS('-', y, LPAD(m,2,'0'), LPAD(d,2,'0'))
///                          ||' '|| CONCAT_WS(':', LPAD(h,2,'0'), LPAD(mi,2,'0'), LPAD(s,2,'0')), '%Y-%m-%d %H:%i:%s')
///   SQLite:     strftime + printf with H, M, S included
/// </summary>
[Trait("Category", "Fast")]
public sealed class DateTimeFromPartsSixArgProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public int Year { get; set; }
        public int Month { get; set; }
        public int Day { get; set; }
        public int Hour { get; set; }
        public int Minute { get; set; }
        public int Second { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.SqlServer, "DATETIME2FROMPARTS")]
    [InlineData(ProviderKind.Postgres,  "MAKE_TIMESTAMP")]
    [InlineData(ProviderKind.MySql,     "STR_TO_DATE")]
    [InlineData(ProviderKind.Sqlite,    "strftime")]
    public void Where_with_new_DateTime_six_arg_from_int_columns_emits_provider_from_parts(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var ctor = typeof(DateTime).GetConstructor(new[] { typeof(int), typeof(int), typeof(int), typeof(int), typeof(int), typeof(int) })!;
        var param = Expression.Parameter(typeof(Row), "r");
        var y = Expression.Property(param, nameof(Row.Year));
        var m = Expression.Property(param, nameof(Row.Month));
        var d = Expression.Property(param, nameof(Row.Day));
        var h = Expression.Property(param, nameof(Row.Hour));
        var mi = Expression.Property(param, nameof(Row.Minute));
        var s = Expression.Property(param, nameof(Row.Second));
        var call = Expression.New(ctor, y, m, d, h, mi, s);
        var body = Expression.GreaterThan(call, Expression.Constant(new DateTime(2026, 1, 1)));
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
        // For SqlServer / Postgres the hour / minute / second column refs
        // must appear; SQLite / MySQL build via text concat.
        switch (providerKind)
        {
            case ProviderKind.SqlServer:
            case ProviderKind.Postgres:
                Assert.Contains("Hour", sql);
                Assert.Contains("Minute", sql);
                Assert.Contains("Second", sql);
                break;
        }
    }
}
