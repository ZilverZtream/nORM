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
/// LINQ's Distinct and set operators compare string members ordinally: "abc" and "ABC"
/// are DIFFERENT elements. Scalar string projections already dedupe ordinally on
/// MySQL / SQL Server (whose default collations fold case); anonymous-type projections
/// with string members must not silently merge case variants either. Parity against
/// LINQ-to-Objects on every configured live server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class AnonymousDistinctOrdinalLiveTests
{
    [Table("AnonDistinct_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Grade { get; set; }
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

    //  1 abc/1   2 ABC/1   3 abc/1 (dup)   4 xyz/2
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Name = "abc", Grade = 1 }, new Row { Id = 2, Name = "ABC", Grade = 1 },
        new Row { Id = 3, Name = "abc", Grade = 1 }, new Row { Id = 4, Name = "xyz", Grade = 2 },
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
        var cols = kind == "postgres"
            ? "\"Name\" VARCHAR(50) NOT NULL, \"Grade\" INT NOT NULL"
            : "Name VARCHAR(50) NOT NULL, Grade INT NOT NULL";
        var insertCols = kind == "postgres" ? "(\"Name\", \"Grade\")" : "(Name, Grade)";
        var table = kind == "postgres" ? "\"AnonDistinct_Test\"" : "AnonDistinct_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {cols})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('abc',1),('ABC',1),('abc',1),('xyz',2)");
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
    public void Anonymous_distinct_with_string_member_is_ordinal_on_live_server(string kind)
        => RunParity(kind, q => q.Select(x => new { x.Name, x.Grade }).Distinct().ToList()
            .OrderBy(x => x.Name, StringComparer.Ordinal).ThenBy(x => x.Grade)
            .Select(x => $"{x.Name}:{x.Grade}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Anonymous_union_with_string_member_is_ordinal_on_live_server(string kind)
        => RunParity(kind, q =>
            q.Where(x => x.Grade == 1).Select(x => new { x.Name, x.Grade })
             .Union(q.Where(x => x.Grade == 2).Select(x => new { x.Name, x.Grade }))
             .ToList()
             .OrderBy(x => x.Name, StringComparer.Ordinal).ThenBy(x => x.Grade)
             .Select(x => $"{x.Name}:{x.Grade}").ToList());
}
