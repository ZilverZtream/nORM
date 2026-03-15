using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that the result cache key includes the database identity (normalized
/// connection string) so that two contexts pointing to different databases do not share
/// a cache entry and return data from the wrong database.
/// </summary>
public class ResultCacheDbIdentityTests
{
    [Table("RcWidget")]
    private class RcWidget
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }

 // ─── Two databases, same query/params → different cache entries ──────

    [Fact]
    public void BuildCacheKey_DifferentConnectionStrings_ProduceDifferentKeys()
    {
 // Structural test — verify that the cache key builder includes the
 // connection string identity component by comparing keys from two contexts
 // with different SQLite file paths.
        using var cn1 = new SqliteConnection("Data Source=file1.db;Mode=Memory;Cache=Shared");
        cn1.Open();
        using var cn2 = new SqliteConnection("Data Source=file2.db;Mode=Memory;Cache=Shared");
        cn2.Open();

        using var ctx1 = new DbContext(cn1, new SqliteProvider());
        using var ctx2 = new DbContext(cn2, new SqliteProvider());

        var provider1 = new NormQueryProvider(ctx1);
        var provider2 = new NormQueryProvider(ctx2);

 // Create identical plans (same SQL, same TResult, same parameters).
        var plan = CreatePlan("SELECT 1", new Dictionary<string, object>());

        var key1 = BuildCacheKey<int>(provider1, plan, new Dictionary<string, object>());
        var key2 = BuildCacheKey<int>(provider2, plan, new Dictionary<string, object>());

 // Different databases → different keys.
        Assert.NotEqual(key1, key2);
    }

    [Fact]
    public void BuildCacheKey_SameConnectionString_ProduceSameKey()
    {
 // Two providers with the same connection string must produce the same
 // cache key (assuming same SQL and parameters) — they share a DB identity.
        using var cn1 = new SqliteConnection("Data Source=samedb.db;Mode=Memory;Cache=Shared");
        cn1.Open();
        using var cn2 = new SqliteConnection("Data Source=samedb.db;Mode=Memory;Cache=Shared");
        cn2.Open();

        using var ctx1 = new DbContext(cn1, new SqliteProvider());
        using var ctx2 = new DbContext(cn2, new SqliteProvider());

        var provider1 = new NormQueryProvider(ctx1);
        var provider2 = new NormQueryProvider(ctx2);

        var plan = CreatePlan("SELECT 1", new Dictionary<string, object>());

        var key1 = BuildCacheKey<int>(provider1, plan, new Dictionary<string, object>());
        var key2 = BuildCacheKey<int>(provider2, plan, new Dictionary<string, object>());

 // Same DB identity → same key.
        Assert.Equal(key1, key2);
    }

    [Fact]
    public void BuildCacheKey_DifferentSql_ProduceDifferentKeys()
    {
 // Regression: Different SQL on the same connection → different keys (unchanged behavior).
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var provider = new NormQueryProvider(ctx);

        var plan1 = CreatePlan("SELECT 1", new Dictionary<string, object>());
        var plan2 = CreatePlan("SELECT 2", new Dictionary<string, object>());

        var key1 = BuildCacheKey<int>(provider, plan1, new Dictionary<string, object>());
        var key2 = BuildCacheKey<int>(provider, plan2, new Dictionary<string, object>());

        Assert.NotEqual(key1, key2);
    }

 // ─── Helpers ──────────────────────────────────────────────────────────────

    private static QueryPlan CreatePlan(string sql, IReadOnlyDictionary<string, object> parameters)
    {
        static Task<object> Materializer(System.Data.Common.DbDataReader r, CancellationToken ct)
            => Task.FromResult<object>(null!);
        static object SyncMaterializer(System.Data.Common.DbDataReader _) => null!;

        return new QueryPlan(
            Sql: sql,
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
            Fingerprint: default);
    }

    private static string BuildCacheKey<TResult>(NormQueryProvider provider, QueryPlan plan, IReadOnlyDictionary<string, object> parameters)
    {
        var method = typeof(NormQueryProvider)
            .GetMethod("BuildCacheKeyFromPlan", BindingFlags.Instance | BindingFlags.NonPublic)!
            .MakeGenericMethod(typeof(TResult));
        return (string)method.Invoke(provider, new object[] { plan, parameters })!;
    }
}
