using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Grouping by a navigation member groups by the PARENT'S VALUE: two different
/// parents sharing a title merge into one group, and orphans group under null.
/// SQLite and PostgreSQL accept the correlated-subquery key directly in GROUP BY;
/// SQL Server and MySQL group by an applied lateral column instead (CROSS APPLY /
/// CROSS JOIN LATERAL), which also satisfies only_full_group_by's SELECT matching.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationGroupByTests
{
    [Table("NavGb_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("NavGb_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    [Fact]
    public void GroupBy_nav_member_merges_same_valued_parents()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NavGb_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE NavGb_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL);
                INSERT INTO NavGb_Dept VALUES (1, 'Eng'), (2, 'Eng'), (3, 'Ops');
                INSERT INTO NavGb_Emp VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3), (4, 'd', NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<Dept>().HasKey(d => d.Id); mb.Entity<Emp>().HasKey(e => e.Id); }
        });

        var groups = ctx.Query<Emp>()
            .GroupBy(e => e.Dept!.Title)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToList().OrderBy(g => g.Key).ToList();
        Assert.Equal(3, groups.Count);
        Assert.Equal(2, groups.Single(g => g.Key == "Eng").Count);
        Assert.Equal(1, groups.Single(g => g.Key == null).Count);

        // Composite anonymous key mixing a plain column and a navigation member.
        var composite = ctx.Query<Emp>()
            .GroupBy(e => new { e.Name, DeptTitle = e.Dept!.Title })
            .Select(g => new { g.Key.Name, g.Key.DeptTitle, Count = g.Count() })
            .ToList();
        Assert.Equal(4, composite.Count); // a/Eng, b/Eng, c/Ops, d/null — names all differ
    }
}

/// <summary>Live parity for navigation-member grouping on every configured server.</summary>
[Trait("Category", TestCategory.LiveProvider)]
public class NavigationGroupByLiveTests
{
    [Table("NavGbL_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("NavGbL_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    private static (Func<DbConnection>?, DatabaseProvider?, string?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, "skip");
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, "skip");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, "skip");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider(), null);
            }
            default: throw new ArgumentOutOfRangeException(nameof(kind));
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
    public void GroupBy_composite_key_with_nav_member_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGbL_Emp")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGbL_Dept")}");
        Exec(factory!, $"CREATE TABLE {T("NavGbL_Dept")} ({q}Id{q} INT PRIMARY KEY, {q}Title{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("NavGbL_Emp")} ({q}Id{q} INT PRIMARY KEY, {q}Name{q} VARCHAR(50) NOT NULL, {q}DeptId{q} INT NULL)");
        Exec(factory!, $"INSERT INTO {T("NavGbL_Dept")} VALUES (1, 'Eng'), (2, 'Eng'), (3, 'Ops')");
        Exec(factory!, $"INSERT INTO {T("NavGbL_Emp")} VALUES (1, 'x', 1), (2, 'x', 2), (3, 'x', 3), (4, 'y', 1)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            var groups = ctx.Query<Emp>()
                .GroupBy(e => new { e.Name, DeptTitle = e.Dept!.Title })
                .Select(g => new { g.Key.Name, g.Key.DeptTitle, Count = g.Count() })
                .ToList().OrderBy(g => g.Name).ThenBy(g => g.DeptTitle).ToList();
            // (x,Eng)=2 [two different Eng depts], (x,Ops)=1, (y,Eng)=1.
            Assert.Equal(3, groups.Count);
            Assert.Equal(2, groups.Single(g => g.Name == "x" && g.DeptTitle == "Eng").Count);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGbL_Emp")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGbL_Dept")}");
        }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void GroupBy_nav_member_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGbL_Emp")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGbL_Dept")}");
        Exec(factory!, $"CREATE TABLE {T("NavGbL_Dept")} ({q}Id{q} INT PRIMARY KEY, {q}Title{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("NavGbL_Emp")} ({q}Id{q} INT PRIMARY KEY, {q}Name{q} VARCHAR(50) NOT NULL, {q}DeptId{q} INT NULL)");
        Exec(factory!, $"INSERT INTO {T("NavGbL_Dept")} VALUES (1, 'Eng'), (2, 'Eng'), (3, 'Ops')");
        Exec(factory!, $"INSERT INTO {T("NavGbL_Emp")} VALUES (1, 'a', 1), (2, 'b', 2), (3, 'c', 3), (4, 'd', NULL)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            var groups = ctx.Query<Emp>()
                .GroupBy(e => e.Dept!.Title)
                .Select(g => new { g.Key, Count = g.Count() })
                .ToList().OrderBy(g => g.Key).ToList();
            Assert.Equal(3, groups.Count);
            Assert.Equal(2, groups.Single(g => g.Key == "Eng").Count);
            Assert.Equal(1, groups.Single(g => g.Key == null).Count);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGbL_Emp")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGbL_Dept")}");
        }
    }
}
