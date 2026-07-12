using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// C# string concatenation treats a null operand as an empty string. MySQL's CONCAT
/// propagates NULL instead (SQL Server's and PostgreSQL's CONCAT already ignore NULLs);
/// these parity tests pin the null-as-empty semantics on every configured live server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class StringConcatNullSemanticsLiveTests
{
    [Table("ConcatNullParity_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string? Nick { get; set; }
        public string? Suffix { get; set; }
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

    //  1 (ann, !)   2 (NULL, !)   3 (bob, NULL)   4 (NULL, NULL)
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Nick = "ann", Suffix = "!" }, new Row { Id = 2, Nick = null, Suffix = "!" },
        new Row { Id = 3, Nick = "bob", Suffix = null }, new Row { Id = 4, Nick = null, Suffix = null },
    };

    private static string CreateTable(string kind, Func<DbConnection> factory)
    {
        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var cols = kind == "postgres"
            ? "\"Nick\" VARCHAR(50) NULL, \"Suffix\" VARCHAR(50) NULL"
            : "Nick VARCHAR(50) NULL, Suffix VARCHAR(50) NULL";
        var insertCols = kind == "postgres" ? "(\"Nick\", \"Suffix\")" : "(Nick, Suffix)";
        var table = kind == "postgres" ? "\"ConcatNullParity_Test\"" : "ConcatNullParity_Test";

        Exec(factory, $"DROP TABLE IF EXISTS {table}");
        Exec(factory, $"CREATE TABLE {table} ({idCol}, {cols})");
        Exec(factory, $"INSERT INTO {table} {insertCols} VALUES ('ann','!'),(NULL,'!'),('bob',NULL),(NULL,NULL)");
        return table;
    }

    private void RunParity<T>(string kind, Func<IQueryable<Row>, List<T>> query)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var table = CreateTable(kind, factory!);
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
    public void Projection_concat_treats_null_as_empty_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Nick + "*").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Projection_concat_of_two_nullable_columns_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Nick + x.Suffix).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Predicate_concat_treats_null_as_empty_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => x.Nick + "*" == "*").Select(x => x.Id).OrderBy(i => i).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Execute_update_concat_writes_empty_not_null_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var table = CreateTable(kind, factory!);
        try
        {
            using (var ctx = new DbContext(factory!(), provider!))
            {
                await ctx.Query<Row>()
                    .ExecuteUpdateAsync(s => s.SetProperty(x => x.Nick, x => x.Nick + "*"));
            }
            using (var verify = new DbContext(factory!(), provider!))
            {
                var nicks = verify.Query<Row>().OrderBy(x => x.Id).Select(x => x.Nick).ToList();
                Assert.Equal(new[] { "ann*", "*", "bob*", "*" }, nicks);
            }
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }
}
