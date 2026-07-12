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

    [Table("GnAvgP_Test")]
    private class Parent
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Kid> Kids { get; set; } = new();
    }

    [Table("GnAvgK_Test")]
    private class Kid
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Navigation_int_average_projection_is_fractional(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;
        var q = kind == "postgres" ? "\"" : "";
        var tp = $"{q}GnAvgP_Test{q}";
        var tk = $"{q}GnAvgK_Test{q}";
        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        string C(string n, string t2) => kind == "postgres" ? $"\"{n}\" {t2}" : $"{n} {t2}";
        Exec(factory!, $"DROP TABLE IF EXISTS {tk}");
        Exec(factory!, $"DROP TABLE IF EXISTS {tp}");
        Exec(factory!, $"CREATE TABLE {tp} ({idCol}, {C("Name", "VARCHAR(20) NOT NULL")})");
        Exec(factory!, $"CREATE TABLE {tk} ({idCol}, {C("ParentId", "INT NOT NULL")}, {C("Amount", "INT NOT NULL")})");
        Exec(factory!, $"INSERT INTO {tp} {(kind == "postgres" ? "(\"Name\")" : "(Name)")} VALUES ('a')");
        Exec(factory!, $"INSERT INTO {tk} {(kind == "postgres" ? "(\"ParentId\", \"Amount\")" : "(ParentId, Amount)")} VALUES (1,1),(1,2)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            // C#: children 1,2 → 1.5. A truncating AVG(int) returns 1.
            var rows = ctx.Query<Parent>().Select(p => new { p.Id, Avg = p.Kids.Average(k => k.Amount) }).ToList();
            Assert.Single(rows);
            Assert.Equal(1.5, rows[0].Avg, 3);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {tk}");
            Exec(factory!, $"DROP TABLE IF EXISTS {tp}");
        }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Navigation_int_average_in_where_is_fractional(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;
        var q = kind == "postgres" ? "\"" : "";
        var tp = $"{q}GnAvgP_Test{q}";
        var tk = $"{q}GnAvgK_Test{q}";
        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        string C(string n, string t2) => kind == "postgres" ? $"\"{n}\" {t2}" : $"{n} {t2}";
        Exec(factory!, $"DROP TABLE IF EXISTS {tk}");
        Exec(factory!, $"DROP TABLE IF EXISTS {tp}");
        Exec(factory!, $"CREATE TABLE {tp} ({idCol}, {C("Name", "VARCHAR(20) NOT NULL")})");
        Exec(factory!, $"CREATE TABLE {tk} ({idCol}, {C("ParentId", "INT NOT NULL")}, {C("Amount", "INT NOT NULL")})");
        Exec(factory!, $"INSERT INTO {tp} {(kind == "postgres" ? "(\"Name\")" : "(Name)")} VALUES ('a')");
        Exec(factory!, $"INSERT INTO {tk} {(kind == "postgres" ? "(\"ParentId\", \"Amount\")" : "(ParentId, Amount)")} VALUES (1,1),(1,2)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            // C#: children avg 1.5 > 1.4 → the parent matches. A truncating AVG(int)=1 would
            // silently drop it; an untranslated navigation aggregate would throw.
            var rows = ctx.Query<Parent>().Where(p => p.Kids.Average(k => k.Amount) > 1.4).ToList();
            Assert.Single(rows);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {tk}");
            Exec(factory!, $"DROP TABLE IF EXISTS {tp}");
        }
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("GnAvgPT_Test")]
    private class ParentWithTotal
    {
        [System.ComponentModel.DataAnnotations.Key,
         System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public double Total { get; set; }
        public List<KidT> KidTs { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("GnAvgKT_Test")]
    private class KidT
    {
        [System.ComponentModel.DataAnnotations.Key,
         System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int ParentWithTotalId { get; set; }
        public int Amount { get; set; }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task ExecuteUpdate_navigation_int_average_is_fractional(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;
        var q = kind == "postgres" ? "\"" : "";
        var tp = $"{q}GnAvgPT_Test{q}";
        var tk = $"{q}GnAvgKT_Test{q}";
        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        string C(string n, string t2) => kind == "postgres" ? $"\"{n}\" {t2}" : $"{n} {t2}";
        Exec(factory!, $"DROP TABLE IF EXISTS {tk}");
        Exec(factory!, $"DROP TABLE IF EXISTS {tp}");
        Exec(factory!, $"CREATE TABLE {tp} ({idCol}, {C("Total", "FLOAT NOT NULL")})");
        Exec(factory!, $"CREATE TABLE {tk} ({idCol}, {C("ParentWithTotalId", "INT NOT NULL")}, {C("Amount", "INT NOT NULL")})");
        Exec(factory!, $"INSERT INTO {tp} {(kind == "postgres" ? "(\"Total\")" : "(Total)")} VALUES (0)");
        Exec(factory!, $"INSERT INTO {tk} {(kind == "postgres" ? "(\"ParentWithTotalId\", \"Amount\")" : "(ParentWithTotalId, Amount)")} VALUES (1,1),(1,2)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            // SetProperty writing a navigation Average: kids 1,2 -> C# average 1.5; a truncating
            // AVG(int) would persist 1.
            await ctx.Query<ParentWithTotal>()
                .ExecuteUpdateAsync(s => s.SetProperty(x => x.Total, x => x.KidTs.Average(k => k.Amount)));
            var total = ctx.Query<ParentWithTotal>().Select(x => x.Total).ToList().Single();
            Assert.Equal(1.5, total, 3);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {tk}");
            Exec(factory!, $"DROP TABLE IF EXISTS {tp}");
        }
    }

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
