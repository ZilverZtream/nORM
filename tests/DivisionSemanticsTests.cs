using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// C# integer division truncates toward zero (5/2 = 2, -7/2 = -3), and SQLite / SQL Server /
/// PostgreSQL <c>/</c> agrees for integer operands — but MySQL's <c>/</c> always produces a
/// DECIMAL (5/2 = 2.5), silently changing which rows a division predicate matches and what value
/// a division projection materializes. Integral-typed division must therefore lower to the
/// provider's integer-division operator (MySQL <c>DIV</c>, whose truncation toward zero matches
/// C#). Floating/decimal division keeps <c>/</c> everywhere.
/// </summary>
[Trait("Category", "Fast")]
public class DivisionSemanticsTests
{
    [Table("Dv")]
    private class Dv
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    //  1 (5,2)   2 (4,2)   3 (-7,2)
    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Dv (Id INTEGER PRIMARY KEY AUTOINCREMENT, A INTEGER NOT NULL, B INTEGER NOT NULL);" +
                "INSERT INTO Dv (A, B) VALUES (5,2),(4,2),(-7,2);";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static readonly Dv[] Reference =
    {
        new Dv { Id = 1, A = 5, B = 2 },
        new Dv { Id = 2, A = 4, B = 2 },
        new Dv { Id = 3, A = -7, B = 2 },
    };

    [Fact]
    public void Where_integer_division_matches_linq()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = Reference.Where(x => x.A / x.B == 2).Select(x => x.Id).ToList();
        var actual = ctx.Query<Dv>().Where(x => x.A / x.B == 2).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Projected_integer_division_truncates_like_csharp()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = Reference.OrderBy(x => x.Id).Select(x => x.A / x.B).ToList();
        var actual = ctx.Query<Dv>().OrderBy(x => x.Id).Select(x => x.A / x.B).ToList();
        Assert.Equal(expected, actual); // 2, 2, -3 — truncation toward zero, including negatives
    }

    [Fact]
    public void Where_modulo_matches_linq()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;
        var expected = Reference.Where(x => x.A % x.B == 1).Select(x => x.Id).ToList();
        var actual = ctx.Query<Dv>().Where(x => x.A % x.B == 1).Select(x => x.Id).OrderBy(i => i).ToList();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void MySql_dialect_emits_DIV_for_integer_division()
    {
        // MySQL's / on integers yields a DECIMAL, so its dialect must emit DIV. The SQL shape is
        // asserted here (a SQLite backend can't execute DIV); end-to-end filtering is covered by
        // the live parity test below.
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new MySqlProvider(new SqliteParameterFactory()));
        var sql = ctx.Query<Dv>().Where(x => x.A / x.B == 2).ToString();
        Assert.Contains(" DIV ", sql);
    }

    [Fact]
    public void Other_dialects_keep_slash_for_integer_division()
    {
        foreach (DatabaseProvider provider in new DatabaseProvider[]
                 { new SqliteProvider(), new SqlServerProvider(new SqliteParameterFactory()), new PostgresProvider(new SqliteParameterFactory()) })
        {
            using var cn = new SqliteConnection("Data Source=:memory:");
            using var ctx = new DbContext(cn, provider);
            var sql = ctx.Query<Dv>().Where(x => x.A / x.B == 2).ToString();
            Assert.DoesNotContain(" DIV ", sql);
            Assert.Contains("/", sql);
        }
    }

    [Fact]
    public void Double_division_keeps_slash_on_mysql_dialect()
    {
        // Floating division is real division in C# too — the DIV lowering must not apply.
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new MySqlProvider(new SqliteParameterFactory()));
        var sql = ctx.Query<Dv>().Where(x => (double)x.A / x.B > 2.4).ToString();
        Assert.DoesNotContain(" DIV ", sql);
    }
}

/// <summary>
/// End-to-end integer-division parity on the real servers: the same LINQ query must return the
/// same rows and values on every configured provider that LINQ-to-Objects computes.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class DivisionSemanticsLiveTests
{
    [Table("DivParity_Test")]
    private class DivRow
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
    }

    // Returns a factory (DbContext disposal closes the connection it was handed, so DDL and the
    // context each need their own), the provider, and a skip reason when unconfigured.
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
    public void Integer_division_parity_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var cols = kind == "postgres" ? "\"A\" INT NOT NULL, \"B\" INT NOT NULL" : "A INT NOT NULL, B INT NOT NULL";
        var insertCols = kind == "postgres" ? "(\"A\", \"B\")" : "(A, B)";
        var table = kind == "postgres" ? "\"DivParity_Test\"" : "DivParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {cols})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES (5,2),(4,2),(-7,2)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);

            // WHERE: C# 5/2 = 2 and 4/2 = 2 match; -7/2 = -3 does not.
            var matched = ctx.Query<DivRow>().Where(x => x.A / x.B == 2)
                .Select(x => x.A).OrderBy(a => a).ToList();
            Assert.Equal(new[] { 4, 5 }, matched);

            // Projection: truncation toward zero, including the negative operand.
            var projected = ctx.Query<DivRow>().OrderBy(x => x.Id).Select(x => x.A / x.B).ToList();
            Assert.Equal(new[] { 2, 2, -3 }, projected);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }
}
