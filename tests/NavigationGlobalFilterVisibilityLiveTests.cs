using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live parity for global-filter visibility through reference navigations: a
/// soft-deleted principal must read as a MISSING parent on every configured server —
/// in navigation predicates, navigation projections, and whole-entity null tests
/// (which become EXISTS probes when the principal type carries filters).
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class NavigationGlobalFilterVisibilityLiveTests
{
    [Table("NavGfL_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public bool Deleted { get; set; }
    }

    [Table("NavGfL_Emp")]
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
    public void Filtered_principal_reads_as_missing_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        var boolType = kind switch
        {
            "postgres" => "BOOLEAN",
            "sqlserver" => "BIT",
            _ => "TINYINT(1)",
        };
        var f = kind == "postgres" ? "FALSE" : "0";
        var t = kind == "postgres" ? "TRUE" : "1";
        Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGfL_Emp")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGfL_Dept")}");
        Exec(factory!, $"CREATE TABLE {T("NavGfL_Dept")} ({q}Id{q} INT PRIMARY KEY, {q}Title{q} VARCHAR(50) NOT NULL, {q}Deleted{q} {boolType} NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("NavGfL_Emp")} ({q}Id{q} INT PRIMARY KEY, {q}Name{q} VARCHAR(50) NOT NULL, {q}DeptId{q} INT NULL)");
        Exec(factory!, $"INSERT INTO {T("NavGfL_Dept")} VALUES (1, 'Eng', {f}), (2, 'Eng', {t})");
        Exec(factory!, $"INSERT INTO {T("NavGfL_Emp")} VALUES (1, 'ann', 1), (2, 'bob', 2), (3, 'cid', NULL)");
        try
        {
            var opts = new DbContextOptions();
            opts.AddGlobalFilter<Dept>(d => !d.Deleted);
            using var ctx = new DbContext(factory!(), provider!, opts);

            // Navigation predicate: bob's dept is soft-deleted, must not match Eng.
            var engNames = ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng")
                .Select(e => e.Name).ToList();
            Assert.Equal(new[] { "ann" }, engNames);

            // Navigation projection: filtered parent reads as null.
            var rows = ctx.Query<Emp>().Select(e => new { e.Name, Title = e.Dept!.Title })
                .ToList().OrderBy(r => r.Name).ToList();
            Assert.Equal("Eng", rows[0].Title);
            Assert.Null(rows[1].Title);
            Assert.Null(rows[2].Title);

            // Whole-entity null test: EXISTS probe with the filter applied.
            var orphans = ctx.Query<Emp>().Where(e => e.Dept == null)
                .Select(e => e.Name).ToList().OrderBy(n => n).ToList();
            Assert.Equal(new[] { "bob", "cid" }, orphans);

            var withDept = ctx.Query<Emp>().Where(e => e.Dept != null)
                .Select(e => e.Name).ToList();
            Assert.Equal(new[] { "ann" }, withDept);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGfL_Emp")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("NavGfL_Dept")}");
        }
    }
}
