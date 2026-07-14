using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The read-path subquery fuzz passes against real servers: the same seeded
/// grouped-First/correlated-aggregate and string-comparison-closure sequences
/// that hold on SQLite run through each provider's correlated-subquery SQL
/// (TOP 1 vs LIMIT 1, OFFSET/FETCH ElementAt, EXISTS/COUNT rewrites), decimal
/// order-key coercions, and collation-sensitive string comparisons — with the
/// closure slot alignment exercised by per-case re-randomized captures.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderSubqueryFuzzParityTests
{
    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_MYSQL not set");
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                var csMysql = cs;
                return (() => Open(t, csMysql), new MySqlProvider(new SqliteParameterFactory()), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_POSTGRES not set");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "NORM_TEST_SQLSERVER not set");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider(), null);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static DbConnection Open(Type connectionType, string cs)
    {
        var cn = (DbConnection)Activator.CreateInstance(connectionType, cs)!;
        cn.Open();
        return cn;
    }

    private static void Exec(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static string CreateTablesSql(string kind) => kind switch
    {
        "mysql" => """
            CREATE TABLE FuzzRow_Test (Id INT PRIMARY KEY, IntVal INT NOT NULL, NullableInt INT NULL, Name VARCHAR(64) CHARACTER SET utf8mb4 NOT NULL, Nick VARCHAR(64) CHARACTER SET utf8mb4 NULL, Amount DECIMAL(18,6) NOT NULL, Price DOUBLE NOT NULL, Flag TINYINT(1) NOT NULL, Created DATETIME(6) NOT NULL);
            CREATE TABLE FuzzChild_Test (Id INT PRIMARY KEY, ParentId INT NOT NULL, ChildVal INT NOT NULL, Tag VARCHAR(16) CHARACTER SET utf8mb4 NOT NULL)
            """,
        "postgres" => """
            CREATE TABLE "FuzzRow_Test" ("Id" INT PRIMARY KEY, "IntVal" INT NOT NULL, "NullableInt" INT NULL, "Name" TEXT NOT NULL, "Nick" TEXT NULL, "Amount" DECIMAL(18,6) NOT NULL, "Price" DOUBLE PRECISION NOT NULL, "Flag" BOOLEAN NOT NULL, "Created" TIMESTAMP NOT NULL);
            CREATE TABLE "FuzzChild_Test" ("Id" INT PRIMARY KEY, "ParentId" INT NOT NULL, "ChildVal" INT NOT NULL, "Tag" TEXT NOT NULL)
            """,
        _ => """
            CREATE TABLE FuzzRow_Test (Id INT PRIMARY KEY, IntVal INT NOT NULL, NullableInt INT NULL, Name NVARCHAR(64) NOT NULL, Nick NVARCHAR(64) NULL, Amount DECIMAL(18,6) NOT NULL, Price FLOAT NOT NULL, Flag BIT NOT NULL, Created DATETIME2 NOT NULL);
            CREATE TABLE FuzzChild_Test (Id INT PRIMARY KEY, ParentId INT NOT NULL, ChildVal INT NOT NULL, Tag NVARCHAR(16) NOT NULL)
            """,
    };

    private static string DropTablesSql(string kind) => kind switch
    {
        "postgres" => """
            DROP TABLE IF EXISTS "FuzzRow_Test"; DROP TABLE IF EXISTS "FuzzChild_Test"
            """,
        "sqlserver" => """
            IF OBJECT_ID('FuzzRow_Test') IS NOT NULL DROP TABLE FuzzRow_Test;
            IF OBJECT_ID('FuzzChild_Test') IS NOT NULL DROP TABLE FuzzChild_Test
            """,
        _ => """
            DROP TABLE IF EXISTS FuzzRow_Test; DROP TABLE IF EXISTS FuzzChild_Test
            """,
    };

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Read_path_subquery_fuzz_holds_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        Exec(factory!, DropTablesSql(kind));
        Exec(factory!, CreateTablesSql(kind));
        try
        {
            using var seedCtx = new DbContext(factory!(), provider!);
            await LinqParityFuzzTests.SeedAsync(seedCtx);
            await LinqParityFuzzTests.SeedChildrenAsync(seedCtx);

            foreach (var seed in new[] { 20260714, 42 })
            {
                using var ctx = new DbContext(factory!(), provider!);
                LinqParityFuzzTests.RunGroupedFirstAndCorrelatedAggregateFuzz(ctx, seed, cases: 80);
                LinqParityFuzzTests.RunStringComparisonClosureFuzz(ctx, seed, cases: 60);
            }
        }
        finally
        {
            Exec(factory!, DropTablesSql(kind));
        }
    }
}
