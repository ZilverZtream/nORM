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
/// C# Average over ints is a double (avg(1,2) = 1.5); T-SQL AVG(int) truncates. The plain
/// aggregate path is already cast — these cover the OTHER emit paths: a grouped Average
/// (GroupBy(...).Select(g => g.Average(...))) must also return fractional values on every
/// provider.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class GroupNavigationIntAverageLiveTests
{
    [Table("GnAvg_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int Grp { get; set; }
        public int Amount { get; set; }
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

    // Grp 1: Amounts 1,2 (avg 1.5)   Grp 2: Amounts 2,3 (avg 2.5)
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Grp = 1, Amount = 1 }, new Row { Id = 2, Grp = 1, Amount = 2 },
        new Row { Id = 3, Grp = 2, Amount = 2 }, new Row { Id = 4, Grp = 2, Amount = 3 },
    };

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Grouped_int_average_is_fractional(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;
        var q = kind == "postgres" ? "\"" : "";
        var table = $"{q}GnAvg_Test{q}";
        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        string C(string n, string t2) => kind == "postgres" ? $"\"{n}\" {t2}" : $"{n} {t2}";
        var ins = kind == "postgres" ? "(\"Grp\", \"Amount\")" : "(Grp, Amount)";
        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {C("Grp", "INT NOT NULL")}, {C("Amount", "INT NOT NULL")})");
        Exec(factory!, $"INSERT INTO {table} {ins} VALUES (1,1),(1,2),(2,2),(2,3)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            var expected = Reference
                .GroupBy(x => x.Grp).Select(g => new { g.Key, Avg = g.Average(x => x.Amount) })
                .OrderBy(x => x.Key).Select(x => (x.Key, x.Avg)).ToList();
            var actual = ctx.Query<Row>()
                .GroupBy(x => x.Grp).Select(g => new { g.Key, Avg = g.Average(x => x.Amount) })
                .ToList().OrderBy(x => x.Key).Select(x => (x.Key, x.Avg)).ToList();
            Assert.Equal(expected, actual); // (1, 1.5), (2, 2.5)
        }
        finally { Exec(factory!, $"DROP TABLE IF EXISTS {table}"); }
    }
}
