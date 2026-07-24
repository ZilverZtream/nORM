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
/// C# string equality is ordinal (case-sensitive): <c>Name == "abc"</c> must not match "ABC".
/// MySQL and SQL Server default column collations are case-insensitive, so a bare <c>=</c> or
/// <c>IN</c> silently matches extra rows there — PostgreSQL and SQLite are already ordinal.
/// These parity tests run the same LINQ query on every configured live server and compare with
/// LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class StringEqualityCaseSensitivityLiveTests
{
    [Table("StrEqParity_Test")]
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

    private void RunParity(string kind, Func<IQueryable<Row>, List<int>> query)
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
        var table = kind == "postgres" ? "\"StrEqParity_Test\"" : "StrEqParity_Test";

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
    public void Equality_is_ordinal_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => x.Name == "abc").Select(x => x.Id).OrderBy(i => i).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Inequality_is_ordinal_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => x.Name != "abc").Select(x => x.Id).OrderBy(i => i).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void In_list_is_ordinal_on_live_server(string kind)
    {
        var names = new[] { "abc", "xyz" };
        RunParity(kind, q => q.Where(x => names.Contains(x.Name)).Select(x => x.Id).OrderBy(i => i).ToList());
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Bare_where_equality_is_ordinal_on_live_server(string kind)
    {
        // No Select/OrderBy after the Where: this shape can take the SIMPLE-QUERY fast path,
        // which must apply the same ordinal wrap as the full translator.
        RunParity(kind, q => q.Where(x => x.Name == "abc").ToList().Select(x => x.Id).OrderBy(i => i).ToList());
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Closure_equality_is_ordinal_on_live_server(string kind)
    {
        var name = "abc";
        RunParity(kind, q => q.Where(x => x.Name == name).Select(x => x.Id).OrderBy(i => i).ToList());
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Count_predicate_equality_is_ordinal_on_live_server(string kind)
    {
        // Count(predicate) takes the direct-count fast path (TryBuildCountWhereClause), which must apply
        // the same ordinal wrap as the full translator and the sibling read fast paths — a bare `=`
        // counts "ABC"/"AbC" on CI-collation servers, so the count is 3 instead of the ordinal 1.
        RunParity(kind, q => new List<int> { q.Count(x => x.Name == "abc") });
    }
}
