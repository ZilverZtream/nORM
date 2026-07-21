using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live validation of GroupBy over a JOIN — key from the outer table, aggregates over an inner
/// member: `Join(...).GroupBy(x => x.OuterKey).Select(g => new { g.Key, g.Count(), g.Sum(x => x.InnerMember) })`.
/// Exercises the join-alias key/aggregate resolution AND ONLY_FULL_GROUP_BY (MySQL/PostgreSQL).
/// Int keys/values are collation-free so the oracle holds across providers. Skips unconfigured providers.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class JoinGroupByLiveTests
{
    [Table("JgkLiveParent")]
    private class Parent { [Key] public int Id { get; set; } public int PVal { get; set; } }
    [Table("JgkLiveChild")]
    private class Child { [Key] public int Id { get; set; } public int ParentId { get; set; } public int ChildVal { get; set; } }

    private static readonly (int Id, int PVal)[] Parents = { (1, 10), (2, 10), (3, 20), (4, 30) };
    private static readonly (int Id, int ParentId, int ChildVal)[] Children =
        { (1, 1, 5), (2, 1, 7), (3, 2, 3), (4, 3, 9), (5, 3, 1) }; // parent 4 childless (inner join drops it)

    private static (Func<DbConnection>?, DatabaseProvider?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
                var mcs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(mcs)) return (null, null);
                return (() => Open(Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!, mcs), new MySqlProvider(new SqliteParameterFactory()));
            case "postgres":
                var pcs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(pcs)) return (null, null);
                return (() => Open(Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!, pcs), new PostgresProvider(new SqliteParameterFactory()));
            case "sqlserver":
                var scs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(scs)) return (null, null);
                return (() => Open(Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!, scs), new SqlServerProvider());
            default: throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static DbConnection Open(Type t, string cs) { var cn = (DbConnection)Activator.CreateInstance(t, cs)!; cn.Open(); return cn; }
    private static void Exec(Func<DbConnection> f, string sql) { using var cn = f(); using var cmd = cn.CreateCommand(); cmd.CommandText = sql; cmd.ExecuteNonQuery(); }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Join_groupby_outer_key_aggregate_inner_matches_linq_live(string kind)
    {
        var (factory, provider) = OpenLive(kind);
        if (factory == null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        Exec(factory, $"DROP TABLE IF EXISTS {T("JgkLiveChild")}");
        Exec(factory, $"DROP TABLE IF EXISTS {T("JgkLiveParent")}");
        Exec(factory, $"CREATE TABLE {T("JgkLiveParent")} ({q}Id{q} INT PRIMARY KEY, {q}PVal{q} INT NOT NULL)");
        Exec(factory, $"CREATE TABLE {T("JgkLiveChild")} ({q}Id{q} INT PRIMARY KEY, {q}ParentId{q} INT NOT NULL, {q}ChildVal{q} INT NOT NULL)");
        try
        {
            using var ctx = new DbContext(factory(), provider!);
            foreach (var p in Parents) ctx.Add(new Parent { Id = p.Id, PVal = p.PVal });
            foreach (var c in Children) ctx.Add(new Child { Id = c.Id, ParentId = c.ParentId, ChildVal = c.ChildVal });
            await ctx.SaveChangesAsync();

            var expected = Parents.Join(Children, p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, c.ChildVal })
                .GroupBy(x => x.PVal).Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.ChildVal) })
                .OrderBy(r => r.Key).Select(r => (r.Key, r.C, r.S)).ToList();
            var actual = ctx.Query<Parent>().Join(ctx.Query<Child>(), p => p.Id, c => c.ParentId, (p, c) => new { p.PVal, c.ChildVal })
                .GroupBy(x => x.PVal).Select(g => new { g.Key, C = g.Count(), S = g.Sum(x => x.ChildVal) })
                .OrderBy(r => r.Key).ToList().Select(r => (r.Key, r.C, r.S)).ToList();
            Assert.Equal(expected, actual);
        }
        finally
        {
            Exec(factory, $"DROP TABLE IF EXISTS {T("JgkLiveChild")}");
            Exec(factory, $"DROP TABLE IF EXISTS {T("JgkLiveParent")}");
        }
    }
}
