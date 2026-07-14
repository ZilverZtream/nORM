using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Consecutive set-based writes whose Contains needle comes from a closure must
/// each bind the CURRENT needle: two ExecuteDeleteAsync calls with the same
/// shape but different captured strings share a cached plan, and a replayed
/// first-execution needle deletes the WRONG rows — silently.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class BulkCudContainsRebindLiveTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("BcRebind_Test")]
    public class Row
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenLive(string kind)
    {
        static DbConnection Open(Type t, string cs)
        {
            var cn = (DbConnection)Activator.CreateInstance(t, cs)!;
            cn.Open();
            return cn;
        }
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_MYSQL not set");
                if (!cs.Contains("UseAffectedRows", StringComparison.OrdinalIgnoreCase))
                    cs = cs.TrimEnd(';') + ";UseAffectedRows=false";
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                var csMysql = cs;
                return (() => Open(t, csMysql), new MySqlProvider(new SqliteParameterFactory(), useAffectedRowsSemantics: false), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_POSTGRES not set");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
            }
            default:
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_SQLSERVER not set");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider(), null);
            }
        }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Contains_needle_rebinds_across_cached_bulk_delete_plans(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        void Exec(string sql)
        {
            using var cn = factory!();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = sql;
            cmd.ExecuteNonQuery();
        }

        Exec(kind == "postgres"
            ? "DROP TABLE IF EXISTS \"BcRebind_Test\""
            : kind == "sqlserver"
                ? "IF OBJECT_ID('BcRebind_Test') IS NOT NULL DROP TABLE BcRebind_Test"
                : "DROP TABLE IF EXISTS BcRebind_Test");
        Exec(kind == "postgres"
            ? "CREATE TABLE \"BcRebind_Test\" (\"Id\" INT PRIMARY KEY, \"Name\" TEXT NOT NULL)"
            : kind == "sqlserver"
                ? "CREATE TABLE BcRebind_Test (Id INT PRIMARY KEY, Name NVARCHAR(64) NOT NULL)"
                : "CREATE TABLE BcRebind_Test (Id INT PRIMARY KEY, Name VARCHAR(64) NOT NULL)");
        try
        {
            using (var seedCtx = new DbContext(factory!(), provider!))
            {
                seedCtx.Add(new Row { Id = 1, Name = "alpha" });
                seedCtx.Add(new Row { Id = 2, Name = "beta" });
                seedCtx.Add(new Row { Id = 3, Name = "gamma" });
                await seedCtx.SaveChangesAsync();
            }

            int Delete(string needle)
            {
                using var ctx = new DbContext(factory!(), provider!);
                var n = needle;
                return ctx.Query<Row>().Where(x => x.Name.Contains(n)).ExecuteDeleteAsync().GetAwaiter().GetResult();
            }

            var first = Delete("alpha");   // caches the plan
            Assert.True(first == 1, $"first delete: expected 1, got {first}");

            var second = Delete("beta");   // must bind "beta", not replay "alpha"
            Assert.True(second == 1, $"second delete: expected 1, got {second} (replayed needle?)");

            using var verifyCtx = new DbContext(factory!(), provider!);
            var remaining = (await verifyCtx.Query<Row>().ToListAsync()).Select(r => r.Name).OrderBy(n => n).ToList();
            Assert.True(remaining.Count == 1 && remaining[0] == "gamma",
                $"remaining rows: [{string.Join(",", remaining)}] (expected only gamma)");
        }
        finally
        {
            Exec(kind == "postgres"
                ? "DROP TABLE IF EXISTS \"BcRebind_Test\""
                : kind == "sqlserver"
                    ? "IF OBJECT_ID('BcRebind_Test') IS NOT NULL DROP TABLE BcRebind_Test"
                    : "DROP TABLE IF EXISTS BcRebind_Test");
        }
    }
}
