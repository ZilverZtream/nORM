using System;
using System.Linq.Expressions;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// nORM had no SQL-side equivalents of the side-effect primitives
/// <c>DateTime.UtcNow</c> / <c>Guid.NewGuid()</c> / <c>random</c> --
/// users who wanted "server-side now" (not the client-clock moment
/// frozen at translation time via constant folding) had no escape
/// hatch. Add three NormFunctions entry points that translate to the
/// provider-native nullary functions:
///
///   NormFunctions.ServerUtcNow() -> SQLite datetime('now'); SqlServer
///     GETUTCDATE(); Postgres (NOW() AT TIME ZONE 'UTC')::timestamp;
///     MySQL UTC_TIMESTAMP().
///   NormFunctions.ServerNewGuid() -> SQLite randomblob(16); SqlServer
///     NEWID(); Postgres gen_random_uuid(); MySQL UUID().
///   NormFunctions.ServerRandom() -> SQLite ABS(random())/9.22e18;
///     SqlServer RAND(); Postgres RANDOM(); MySQL RAND().
/// </summary>
[Trait("Category", "Fast")]
public sealed class NormFunctionsServerSideProviderShapeTests : TestBase
{
    private sealed class Row
    {
        public int Id { get; set; }
        public DateTime Stamp { get; set; }
        public Guid Token { get; set; }
        public double Score { get; set; }
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite,    "datetime('now')")]
    [InlineData(ProviderKind.SqlServer, "GETUTCDATE")]
    [InlineData(ProviderKind.Postgres,  "AT TIME ZONE")]
    [InlineData(ProviderKind.MySql,     "UTC_TIMESTAMP")]
    public void Where_with_ServerUtcNow_emits_provider_now_function(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var stamp = Expression.Property(param, nameof(Row.Stamp));
        var call = Expression.Call(typeof(NormFunctions).GetMethod(nameof(NormFunctions.ServerUtcNow))!);
        var body = Expression.GreaterThan(stamp, call);
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite,    "randomblob")]
    [InlineData(ProviderKind.SqlServer, "NEWID")]
    [InlineData(ProviderKind.Postgres,  "gen_random_uuid")]
    [InlineData(ProviderKind.MySql,     "UUID")]
    public void Where_with_ServerNewGuid_emits_provider_uuid_function(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var token = Expression.Property(param, nameof(Row.Token));
        var call = Expression.Call(typeof(NormFunctions).GetMethod(nameof(NormFunctions.ServerNewGuid))!);
        var body = Expression.NotEqual(token, call);
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }

    [Theory]
    [InlineData(ProviderKind.Sqlite,    "random")]
    [InlineData(ProviderKind.SqlServer, "RAND")]
    [InlineData(ProviderKind.Postgres,  "RANDOM")]
    [InlineData(ProviderKind.MySql,     "RAND")]
    public void Where_with_ServerRandom_emits_provider_random_function(ProviderKind providerKind, string expectedFragment)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var param = Expression.Parameter(typeof(Row), "r");
        var score = Expression.Property(param, nameof(Row.Score));
        var call = Expression.Call(typeof(NormFunctions).GetMethod(nameof(NormFunctions.ServerRandom))!);
        var body = Expression.LessThan(score, call);
        var lambda = Expression.Lambda<Func<Row, bool>>(body, param);

        var (sql, _) = Translate<Row>(lambda, connection, provider);
        Assert.Contains(expectedFragment, sql);
    }
}
