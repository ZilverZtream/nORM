using System;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// The parity fuzz harness against real servers: the same generated query
/// shapes that validate the translator core on SQLite here compose each
/// dialect's hooks (ordinal string wraps, integer division, decimal and
/// temporal handling, paging emission) with LINQ-to-Objects as the oracle.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
[Collection(LiveFuzzTableCollection.Name)]
public class LinqParityFuzzLiveTests
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
                return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()), null);
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

    private static string CreateTableSql(string kind) => kind switch
    {
        "mysql" => """
            CREATE TABLE FuzzRow_Test (
                Id INT PRIMARY KEY,
                IntVal INT NOT NULL,
                NullableInt INT NULL,
                Name VARCHAR(64) CHARACTER SET utf8mb4 NOT NULL,
                Nick VARCHAR(64) CHARACTER SET utf8mb4 NULL,
                Amount DECIMAL(18,6) NOT NULL,
                Price DOUBLE NOT NULL,
                Flag TINYINT(1) NOT NULL,
                Created DATETIME(6) NOT NULL)
            """ + "; CREATE TABLE FuzzChild_Test (Id INT PRIMARY KEY, ParentId INT NOT NULL, ChildVal INT NOT NULL, Tag VARCHAR(16) NOT NULL)",
        "postgres" => """
            CREATE TABLE "FuzzRow_Test" (
                "Id" INT PRIMARY KEY,
                "IntVal" INT NOT NULL,
                "NullableInt" INT NULL,
                "Name" TEXT NOT NULL,
                "Nick" TEXT NULL,
                "Amount" DECIMAL(18,6) NOT NULL,
                "Price" DOUBLE PRECISION NOT NULL,
                "Flag" BOOLEAN NOT NULL,
                "Created" TIMESTAMP NOT NULL)
            """ + "; CREATE TABLE \"FuzzChild_Test\" (\"Id\" INT PRIMARY KEY, \"ParentId\" INT NOT NULL, \"ChildVal\" INT NOT NULL, \"Tag\" TEXT NOT NULL)",
        _ => """
            CREATE TABLE FuzzRow_Test (
                Id INT PRIMARY KEY,
                IntVal INT NOT NULL,
                NullableInt INT NULL,
                Name NVARCHAR(64) NOT NULL,
                Nick NVARCHAR(64) NULL,
                Amount DECIMAL(18,6) NOT NULL,
                Price FLOAT NOT NULL,
                Flag BIT NOT NULL,
                Created DATETIME2 NOT NULL)
            """ + "; CREATE TABLE FuzzChild_Test (Id INT PRIMARY KEY, ParentId INT NOT NULL, ChildVal INT NOT NULL, Tag NVARCHAR(16) NOT NULL)",
    };

    private static string DropTableSql(string kind) => kind switch
    {
        "postgres" => "DROP TABLE IF EXISTS \"FuzzRow_Test\"; DROP TABLE IF EXISTS \"FuzzChild_Test\"",
        "sqlserver" => "IF OBJECT_ID('FuzzRow_Test') IS NOT NULL DROP TABLE FuzzRow_Test; IF OBJECT_ID('FuzzChild_Test') IS NOT NULL DROP TABLE FuzzChild_Test",
        _ => "DROP TABLE IF EXISTS FuzzRow_Test; DROP TABLE IF EXISTS FuzzChild_Test",
    };

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Generated_query_shapes_match_linq_to_objects_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        Exec(factory!, DropTableSql(kind));
        Exec(factory!, CreateTableSql(kind));
        try
        {
            await using var ctx = new DbContext(factory!(), provider!, LinqParityFuzzTests.CreateFuzzOptions());
            await LinqParityFuzzTests.SeedAsync(ctx);
            await LinqParityFuzzTests.SeedChildrenAsync(ctx);
            LinqParityFuzzTests.RunFuzz(ctx, seed: 20260713, cases: 250);
            LinqParityFuzzTests.RunFuzz(ctx, seed: 42, cases: 250);
            LinqParityFuzzTests.RunJoinFuzz(ctx, seed: 20260713, cases: 100);
            LinqParityFuzzTests.RunJoinFuzz(ctx, seed: 42, cases: 100);
            LinqParityFuzzTests.RunSelectManyFuzz(ctx, seed: 20260713, cases: 80);
            LinqParityFuzzTests.RunSelectManyFuzz(ctx, seed: 42, cases: 80);
            LinqParityFuzzTests.RunNavFlattenFuzz(ctx, seed: 20260713, cases: 80);
            LinqParityFuzzTests.RunNavFlattenFuzz(ctx, seed: 42, cases: 80);
            LinqParityFuzzTests.RunSetOpFuzz(ctx, seed: 20260713, cases: 80);
            LinqParityFuzzTests.RunSetOpFuzz(ctx, seed: 42, cases: 80);
            LinqParityFuzzTests.RunKeyedOpFuzz(ctx, seed: 20260713, cases: 70);
            LinqParityFuzzTests.RunKeyedOpFuzz(ctx, seed: 42, cases: 70);
            LinqParityFuzzTests.RunWindowFuzz(ctx, seed: 20260713, cases: 70);
            LinqParityFuzzTests.RunWindowFuzz(ctx, seed: 42, cases: 70);
        }
        finally
        {
            Exec(factory!, DropTableSql(kind));
        }
    }
}
