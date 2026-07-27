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
/// Regression: a subquery / semi-join string Contains — Where(p => ctx.Query&lt;Child&gt;().Select(c => c.Code)
/// .Contains(p.Code)) — must match ordinally, like C# Enumerable.Contains. The predicate lowers to
/// `p.Code IN (SELECT c.Code ...)`, which uses the outer value's collation; on SQL Server / MySQL that
/// default collation is case-INSENSITIVE, so 'ABC' would wrongly match a child 'abc' (extra rows). The
/// local-list Contains path already forces ordinal; the subquery path did not. SQLite / PostgreSQL default
/// to ordinal so they are unaffected (and confirm the fix does not over-filter).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class SubqueryContainsOrdinalLiveTests
{
    [Table("SubCParent")]
    private sealed class Parent { [Key] public int Id { get; set; } public string Code { get; set; } = ""; }

    [Table("SubCChild")]
    private sealed class Child { [Key] public int Id { get; set; } public string Code { get; set; } = ""; }

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
            default: throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static DbConnection Open(Type t, string cs) { var cn = (DbConnection)Activator.CreateInstance(t, cs)!; cn.Open(); return cn; }
    private static void Exec(Func<DbConnection> f, string sql) { using var cn = f(); using var cmd = cn.CreateCommand(); cmd.CommandText = sql; cmd.ExecuteNonQuery(); }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Subquery_contains_matches_ordinally_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var (pTable, cTable, idP, codeP, idC, codeC) = kind == "postgres"
            ? ("\"SubCParent\"", "\"SubCChild\"", "\"Id\" INT PRIMARY KEY", "\"Code\" VARCHAR(50) NOT NULL", "\"Id\" INT PRIMARY KEY", "\"Code\" VARCHAR(50) NOT NULL")
            : ("SubCParent", "SubCChild", "Id INT PRIMARY KEY", "Code VARCHAR(50) NOT NULL", "Id INT PRIMARY KEY", "Code VARCHAR(50) NOT NULL");
        var pCodeCol = kind == "postgres" ? "\"Code\"" : "Code";
        var cCodeCol = kind == "postgres" ? "\"Code\"" : "Code";
        var pIns = kind == "postgres" ? "(\"Id\", \"Code\")" : "(Id, Code)";
        var cIns = kind == "postgres" ? "(\"Id\", \"Code\")" : "(Id, Code)";

        Exec(factory!, $"DROP TABLE IF EXISTS {pTable}");
        Exec(factory!, $"DROP TABLE IF EXISTS {cTable}");
        Exec(factory!, $"CREATE TABLE {pTable} ({idP}, {codeP})");
        Exec(factory!, $"CREATE TABLE {cTable} ({idC}, {codeC})");
        try
        {
            // Child has 'abc' only. Parent 1 = 'abc' (ordinal match), Parent 2 = 'ABC' (must NOT match).
            Exec(factory!, $"INSERT INTO {cTable} {cIns} VALUES (1, 'abc')");
            Exec(factory!, $"INSERT INTO {pTable} {pIns} VALUES (1, 'abc'), (2, 'ABC')");

            using var ctx = new DbContext(factory!(), provider!);
            var matched = ctx.Query<Parent>()
                .Where(p => ctx.Query<Child>().Select(c => c.Code).Contains(p.Code))
                .Select(p => p.Id)
                .ToList()
                .OrderBy(x => x)
                .ToList();

            // Ordinal: only Parent 1 ('abc') matches; 'ABC' must be excluded.
            Assert.Equal(new[] { 1 }, matched);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {pTable}");
            Exec(factory!, $"DROP TABLE IF EXISTS {cTable}");
        }
    }
}
