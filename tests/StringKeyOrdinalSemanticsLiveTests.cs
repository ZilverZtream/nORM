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
/// LINQ-to-Objects joins, groups, and distincts string keys with the ordinal comparer: "abc" and
/// "ABC" join only to themselves, group separately, and stay distinct. On MySQL / SQL Server
/// default CI collations a bare JOIN ON / GROUP BY / DISTINCT folds case — extra join matches and
/// merged groups, silently. PostgreSQL is ordinal already. Parity is asserted against
/// LINQ-to-Objects on every configured live server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class StringKeyOrdinalSemanticsLiveTests
{
    [Table("SkoLeft_Test")]
    private class L
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Code { get; set; } = "";
    }

    [Table("SkoRight_Test")]
    private class R
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Code { get; set; } = "";
        public int Val { get; set; }
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

    // Left:  1 abc   2 ABC
    // Right: 1 abc/10   2 ABC/20   3 abc/30
    private static readonly L[] Lefts =
    {
        new L { Id = 1, Code = "abc" }, new L { Id = 2, Code = "ABC" },
    };
    private static readonly R[] Rights =
    {
        new R { Id = 1, Code = "abc", Val = 10 },
        new R { Id = 2, Code = "ABC", Val = 20 },
        new R { Id = 3, Code = "abc", Val = 30 },
    };

    private void Setup(string kind, out Func<DbConnection> factory, out DatabaseProvider provider, out string leftTable, out string rightTable, out bool skip)
    {
        var (f, p, reason) = OpenLive(kind);
        skip = reason != null;
        factory = f!;
        provider = p!;
        var q = kind == "postgres" ? "\"" : "";
        leftTable = $"{q}SkoLeft_Test{q}";
        rightTable = $"{q}SkoRight_Test{q}";
        if (skip) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        string C(string name, string type) => kind == "postgres" ? $"\"{name}\" {type}" : $"{name} {type}";
        string Cols(params string[] names) => kind == "postgres"
            ? "(" + string.Join(", ", names.Select(n => $"\"{n}\"")) + ")"
            : "(" + string.Join(", ", names) + ")";

        Exec(factory, $"DROP TABLE IF EXISTS {leftTable}");
        Exec(factory, $"DROP TABLE IF EXISTS {rightTable}");
        Exec(factory, $"CREATE TABLE {leftTable} ({idCol}, {C("Code", "VARCHAR(20) NOT NULL")})");
        Exec(factory, $"CREATE TABLE {rightTable} ({idCol}, {C("Code", "VARCHAR(20) NOT NULL")}, {C("Val", "INT NOT NULL")})");
        Exec(factory, $"INSERT INTO {leftTable} {Cols("Code")} VALUES ('abc'),('ABC')");
        Exec(factory, $"INSERT INTO {rightTable} {Cols("Code", "Val")} VALUES ('abc',10),('ABC',20),('abc',30)");
    }

    private void Cleanup(Func<DbConnection> factory, string leftTable, string rightTable)
    {
        Exec(factory, $"DROP TABLE IF EXISTS {leftTable}");
        Exec(factory, $"DROP TABLE IF EXISTS {rightTable}");
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Join_on_string_key_is_ordinal(string kind)
    {
        Setup(kind, out var factory, out var provider, out var lt, out var rt, out var skip);
        if (skip) return;
        try
        {
            using var ctx = new DbContext(factory(), provider);
            // LINQ: abc→(10,30), ABC→(20). CI join would also pair abc↔ABC.
            var expected = Lefts.Join(Rights, l => l.Code, r => r.Code, (l, r) => (l.Id, r.Val))
                .OrderBy(t => t.Id).ThenBy(t => t.Val).ToList();
            var actual = ctx.Query<L>().Join(ctx.Query<R>(), l => l.Code, r => r.Code, (l, r) => new { l.Id, r.Val })
                .ToList().Select(x => (x.Id, x.Val)).OrderBy(t => t.Id).ThenBy(t => t.Val).ToList();
            Assert.Equal(expected, actual);
        }
        finally { Cleanup(factory, lt, rt); }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void GroupBy_string_key_is_ordinal(string kind)
    {
        Setup(kind, out var factory, out var provider, out var lt, out var rt, out var skip);
        if (skip) return;
        try
        {
            using var ctx = new DbContext(factory(), provider);
            // LINQ over Rights: groups "abc"(2 rows) and "ABC"(1 row) — CI would give one group of 3.
            var expected = Rights.GroupBy(r => r.Code).Select(g => new { g.Key, N = g.Count() })
                .OrderBy(x => x.Key, StringComparer.Ordinal).Select(x => (x.Key, x.N)).ToList();
            var actual = ctx.Query<R>().GroupBy(r => r.Code).Select(g => new { g.Key, N = g.Count() })
                .ToList().OrderBy(x => x.Key, StringComparer.Ordinal).Select(x => (x.Key, x.N)).ToList();
            Assert.Equal(expected, actual);
        }
        finally { Cleanup(factory, lt, rt); }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Distinct_string_projection_is_ordinal(string kind)
    {
        Setup(kind, out var factory, out var provider, out var lt, out var rt, out var skip);
        if (skip) return;
        try
        {
            using var ctx = new DbContext(factory(), provider);
            // LINQ over Rights.Code: {"abc","ABC"} — CI distinct would collapse to one value.
            var expected = Rights.Select(r => r.Code).Distinct()
                .OrderBy(x => x, StringComparer.Ordinal).ToList();
            var actual = ctx.Query<R>().Select(r => r.Code).Distinct()
                .ToList().OrderBy(x => x, StringComparer.Ordinal).ToList();
            Assert.Equal(expected, actual);
        }
        finally { Cleanup(factory, lt, rt); }
    }
}
