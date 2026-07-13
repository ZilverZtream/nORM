using System;
using System.Collections.Generic;
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
/// A C# explicit cast from floating point to an integer type TRUNCATES toward zero
/// ((int)2.7 is 2, (int)-2.7 is -2). SQL CAST semantics diverge: MySQL rounds half
/// away from zero, PostgreSQL's ::int rounds half to even — both silently produce
/// different values than the C# expression. Parity against LINQ-to-Objects on every
/// configured live server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class ExplicitCastTruncationLiveTests
{
    [Table("CastTrunc_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public double Val { get; set; }
    }

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

    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Val = 2.7 }, new Row { Id = 2, Val = -2.7 },
        new Row { Id = 3, Val = 2.5 }, new Row { Id = 4, Val = 3.5 },
    };

    private void RunParity<T>(string kind, Func<IQueryable<Row>, List<T>> query)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var valCol = kind switch
        {
            "mysql" => "Val DOUBLE NOT NULL",
            "postgres" => "\"Val\" DOUBLE PRECISION NOT NULL",
            _ => "Val FLOAT NOT NULL",
        };
        var insertCols = kind == "postgres" ? "(\"Val\")" : "(Val)";
        var table = kind == "postgres" ? "\"CastTrunc_Test\"" : "CastTrunc_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {valCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES (2.7),(-2.7),(2.5),(3.5)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            var expected = query(Reference.AsQueryable());
            var actual = query(ctx.Query<Row>());
            Assert.Equal(expected, actual);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Explicit_int_cast_truncates_toward_zero_on_live_server(string kind)
        // C#: 2, -2, 2, 3.
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => (int)x.Val).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Explicit_long_cast_truncates_toward_zero_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => (long)x.Val).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Explicit_cast_predicate_uses_truncation_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => (int)x.Val == 2).Select(x => x.Id).OrderBy(i => i).ToList());
}
