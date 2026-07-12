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
/// C# string.IndexOf and string.Replace match ordinally (case-sensitive), but the SQL
/// functions they translate to (LOCATE / CHARINDEX / REPLACE) follow the column collation,
/// which is case-insensitive by default on MySQL and SQL Server — so a bare translation
/// finds/replaces case variants the C# semantics would not. PostgreSQL and SQLite compare
/// ordinally already. These parity tests run the same LINQ query on every configured live
/// server and compare with LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class StringFunctionOrdinalSemanticsLiveTests
{
    [Table("StrFnParity_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
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

    //  1 abc   2 ABC   3 AbC   4 xyz
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Name = "abc" }, new Row { Id = 2, Name = "ABC" },
        new Row { Id = 3, Name = "AbC" }, new Row { Id = 4, Name = "xyz" },
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
        var nameCol = kind == "postgres" ? "\"Name\" VARCHAR(50) NOT NULL" : "Name VARCHAR(50) NOT NULL";
        var insertCols = kind == "postgres" ? "(\"Name\")" : "(Name)";
        var table = kind == "postgres" ? "\"StrFnParity_Test\"" : "StrFnParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {nameCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('abc'),('ABC'),('AbC'),('xyz')");
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
    public void IndexOf_projection_is_ordinal_on_live_server(string kind)
        // Ordinal: "abc" -> -1, "ABC" -> 1, "AbC" -> -1, "xyz" -> -1. A collation-following
        // LOCATE/CHARINDEX finds the case variants too (1, 1, 1, -1).
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Name.IndexOf("B")).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void IndexOf_predicate_is_ordinal_on_live_server(string kind)
        // Threshold == 1 (not a Contains-shaped comparison): only "ABC" has ordinal 'B' at 1.
        => RunParity(kind, q => q.Where(x => x.Name.IndexOf("B") == 1).Select(x => x.Id).OrderBy(i => i).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Replace_projection_is_ordinal_on_live_server(string kind)
        // Ordinal: "abc" -> "a!c", "ABC" unchanged, "AbC" -> "A!C". A collation-following
        // REPLACE rewrites the uppercase variants too.
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Name.Replace("b", "!")).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Replace_predicate_is_ordinal_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => x.Name.Replace("b", "x") == "axc").Select(x => x.Id).OrderBy(i => i).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async System.Threading.Tasks.Task Replace_in_execute_update_is_ordinal_on_live_server(string kind)
    {
        // The SET clause writes the function result back to the table, so a collation-
        // following REPLACE would silently corrupt the case-variant rows.
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var nameCol = kind == "postgres" ? "\"Name\" VARCHAR(50) NOT NULL" : "Name VARCHAR(50) NOT NULL";
        var insertCols = kind == "postgres" ? "(\"Name\")" : "(Name)";
        var table = kind == "postgres" ? "\"StrFnParity_Test\"" : "StrFnParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {nameCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('abc'),('ABC'),('AbC'),('xyz')");
        try
        {
            using (var ctx = new DbContext(factory!(), provider!))
            {
                await ctx.Query<Row>()
                    .ExecuteUpdateAsync(s => s.SetProperty(x => x.Name, x => x.Name.Replace("b", "!")));
            }
            using (var verify = new DbContext(factory!(), provider!))
            {
                var names = verify.Query<Row>().OrderBy(x => x.Id).Select(x => x.Name).ToList();
                Assert.Equal(new[] { "a!c", "ABC", "A!C", "xyz" }, names);
            }
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }
}
