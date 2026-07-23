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
/// DefaultIfEmpty over a correlated value aggregate lowers to COALESCE((SELECT agg ...), fallback). That was
/// verified against LINQ-to-Objects on SQLite; this pins the same parity on every configured live server so
/// a dialect-specific correlated-subquery or COALESCE defect can't slip through. Exercises an always-empty
/// correlated set (fallback wins), a non-empty set (aggregate wins), a Sum fallback, and a no-argument
/// DefaultIfEmpty (default(int) = 0).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class DefaultIfEmptyCrossProviderLiveTests
{
    [Table("DiexRow")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int Grade { get; set; }
    }

    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Grade = 1 }, new Row { Id = 2, Grade = 1 },
        new Row { Id = 3, Grade = 2 }, new Row { Id = 4, Grade = 3 },
    };

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

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void DefaultIfEmpty_correlated_aggregate_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var gradeCol = kind == "postgres" ? "\"Grade\" INT NOT NULL" : "Grade INT NOT NULL";
        var insertCols = kind == "postgres" ? "(\"Grade\")" : "(Grade)";
        var table = kind == "postgres" ? "\"DiexRow\"" : "DiexRow";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {gradeCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES (1),(1),(2),(3)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);

            var actual = ctx.Query<Row>()
                .Select(x => new
                {
                    x.Id,
                    // Grade + 100 never matches -> empty -> fallback -1
                    EmptyMax = ctx.Query<Row>().Where(o => o.Grade == x.Grade + 100).Select(o => o.Grade).DefaultIfEmpty(-1).Max(),
                    // Grade matches at least x itself -> aggregate wins
                    NonEmptyMax = ctx.Query<Row>().Where(o => o.Grade == x.Grade).Select(o => o.Grade).DefaultIfEmpty(-1).Max(),
                    EmptySum = ctx.Query<Row>().Where(o => o.Grade == x.Grade + 100).Select(o => o.Grade).DefaultIfEmpty(777).Sum(),
                    EmptyNoArg = ctx.Query<Row>().Where(o => o.Grade == x.Grade + 100).Select(o => o.Grade).DefaultIfEmpty().Max(),
                })
                .ToList().OrderBy(r => r.Id).Select(r => $"{r.Id}:{r.EmptyMax}:{r.NonEmptyMax}:{r.EmptySum}:{r.EmptyNoArg}").ToList();

            var expected = Reference
                .Select(x => new
                {
                    x.Id,
                    EmptyMax = Reference.Where(o => o.Grade == x.Grade + 100).Select(o => o.Grade).DefaultIfEmpty(-1).Max(),
                    NonEmptyMax = Reference.Where(o => o.Grade == x.Grade).Select(o => o.Grade).DefaultIfEmpty(-1).Max(),
                    EmptySum = Reference.Where(o => o.Grade == x.Grade + 100).Select(o => o.Grade).DefaultIfEmpty(777).Sum(),
                    EmptyNoArg = Reference.Where(o => o.Grade == x.Grade + 100).Select(o => o.Grade).DefaultIfEmpty().Max(),
                })
                .OrderBy(r => r.Id).Select(r => $"{r.Id}:{r.EmptyMax}:{r.NonEmptyMax}:{r.EmptySum}:{r.EmptyNoArg}").ToList();

            Assert.Equal(expected, actual);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }
}
