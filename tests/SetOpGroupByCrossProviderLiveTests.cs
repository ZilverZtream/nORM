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
/// GroupBy over a set operation (and over a Distinct-wrapped set operation), HAVING over a set-op group,
/// a computed GroupBy key, and a composite key over a set-op all rely on wrapping a compound / DISTINCT
/// SELECT as a derived table — a construct whose aliasing and GROUP BY placement differ across dialects.
/// These were verified against LINQ-to-Objects on SQLite; this pins the same parity on every configured
/// live server so a dialect-specific derived-table or HAVING defect can't slip through.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class SetOpGroupByCrossProviderLiveTests
{
    [Table("SgxRow")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Grade { get; set; }
    }

    // Grade distribution 3/2/1 so HAVING and modulo grouping are selective.
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Name = "a", Grade = 1 }, new Row { Id = 2, Name = "b", Grade = 1 }, new Row { Id = 3, Name = "c", Grade = 1 },
        new Row { Id = 4, Name = "d", Grade = 2 }, new Row { Id = 5, Name = "e", Grade = 2 },
        new Row { Id = 6, Name = "f", Grade = 3 },
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
        var table = kind == "postgres" ? "\"SgxRow\"" : "SgxRow";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {cols})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('a',1),('b',1),('c',1),('d',2),('e',2),('f',3)");
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
    public void Setop_distinct_groupby_count_on_live_server(string kind)
        => RunParity(kind, q => q.Concat(q).Distinct().GroupBy(x => x.Grade).Select(g => new { g.Key, C = g.Count() })
            .ToList().OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.C}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Setop_groupby_having_on_live_server(string kind)
        => RunParity(kind, q => q.Concat(q).GroupBy(x => x.Grade).Where(g => g.Count() > 3).Select(g => new { g.Key, C = g.Count() })
            .ToList().OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.C}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Groupby_computed_modulo_key_on_live_server(string kind)
        => RunParity(kind, q => q.GroupBy(x => x.Grade % 2).Select(g => new { g.Key, C = g.Count() })
            .ToList().OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.C}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Setop_composite_key_groupby_on_live_server(string kind)
        => RunParity(kind, q => q.Concat(q).GroupBy(x => new { x.Name, x.Grade }).Select(g => new { g.Key.Name, g.Key.Grade, C = g.Count() })
            .ToList().OrderBy(x => x.Name, StringComparer.Ordinal).Select(x => $"{x.Name}/{x.Grade}:{x.C}").ToList());
}
