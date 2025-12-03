using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

public class CacheKeyTests
{
    private static DbContext CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return new DbContext(cn, new SqliteProvider());
    }

    private static QueryPlan CreatePlan(IReadOnlyDictionary<string, object> parameters)
    {
        static Task<object> Materializer(DbDataReader r, CancellationToken ct) => Task.FromResult<object>(null!);
        static object SyncMaterializer(DbDataReader _) => null!;

        return new QueryPlan(
            Sql: "select 1",
            Parameters: parameters,
            CompiledParameters: Array.Empty<string>(),
            Materializer: Materializer,
            SyncMaterializer: SyncMaterializer,
            ElementType: typeof(int),
            IsScalar: true,
            SingleResult: true,
            NoTracking: true,
            MethodName: "Test",
            Includes: new List<IncludePlan>(),
            GroupJoinInfo: null,
            Tables: Array.Empty<string>(),
            SplitQuery: false,
            CommandTimeout: TimeSpan.FromSeconds(30),
            IsCacheable: true,
            CacheExpiration: null,
            Fingerprint: 0);
    }

    private static string BuildKey(NormQueryProvider provider, QueryPlan plan, IReadOnlyDictionary<string, object> parameters)
    {
        var method = typeof(NormQueryProvider)
            .GetMethod("BuildCacheKeyFromPlan", BindingFlags.Instance | BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(int));
        return (string)method.Invoke(provider, new object[] { plan, parameters })!;
    }

    private sealed class BadHash
    {
        private readonly int _value;
        public BadHash(int value) => _value = value;
        public override int GetHashCode() => 1;
        public override string ToString() => _value.ToString();
    }

    [Fact]
    public void DifferentParameterValuesProduceDistinctCacheKeys()
    {
        using var ctx = CreateContext();
        var provider = new NormQueryProvider(ctx);

        var p1 = new Dictionary<string, object> { { "p", new BadHash(1) } };
        var p2 = new Dictionary<string, object> { { "p", new BadHash(2) } };

        var plan1 = CreatePlan(p1);
        var plan2 = CreatePlan(p2);

        var key1 = BuildKey(provider, plan1, p1);
        var key2 = BuildKey(provider, plan2, p2);

        Assert.NotEqual(key1, key2);
    }
}
