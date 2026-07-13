using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Boolean-valued projections — null tests (x == null) and comparisons (x > 5) as
/// projected members — must materialize correct booleans on every provider. T-SQL
/// rejects a bare predicate in the SELECT list, so those wrap through the
/// CASE-to-BIT hook; the other dialects select predicates directly. The null-test
/// shape previously emitted `col = NULL` and silently returned false for every row.
/// </summary>

[Trait("Category", TestCategory.LiveProvider)]
public class BooleanProjectionParityLiveTests
{
    [Table("BoolProj_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int? Score { get; set; }
    }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, "skip");
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "skip");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "skip");
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

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Null_test_and_comparison_projections_on_live(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var scoreCol = kind == "postgres" ? "\"Score\" INT NULL" : "Score INT NULL";
        var insertCols = kind == "postgres" ? "(\"Score\")" : "(Score)";
        var table = kind == "postgres" ? "\"BoolProj_Test\"" : "BoolProj_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {scoreCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES (10),(NULL),(3)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            var rows = ctx.Query<Row>().OrderBy(r => r.Id)
                .Select(r => new { r.Id, NoScore = r.Score == null, Big = r.Score > 5 }).ToList();
            Assert.Equal(new[] { false, true, false }, rows.Select(r => r.NoScore).ToArray());
            Assert.Equal(new[] { true, false, false }, rows.Select(r => r.Big).ToArray());
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }
}
