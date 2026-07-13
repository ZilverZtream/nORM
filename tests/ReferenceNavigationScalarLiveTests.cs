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
/// Reference-navigation member traversal emits a correlated scalar subquery; the
/// shape is ANSI SQL and must behave identically on every configured live server:
/// predicates filter through the parent, projections yield the parent's value or
/// NULL for a missing optional parent, and two-hop chains nest.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class ReferenceNavigationScalarLiveTests
{
    [Table("NavScalar_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("NavScalar_Emp")]
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

    private void RunOnLive(string kind, Action<DbContext> assertions)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;

        Exec(factory!, $"DROP TABLE IF EXISTS {T("NavScalar_Emp")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("NavScalar_Dept")}");
        Exec(factory!, $"CREATE TABLE {T("NavScalar_Dept")} ({q}Id{q} INT PRIMARY KEY, {q}Title{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("NavScalar_Emp")} ({q}Id{q} INT PRIMARY KEY, {q}Name{q} VARCHAR(50) NOT NULL, {q}DeptId{q} INT NULL)");
        Exec(factory!, $"INSERT INTO {T("NavScalar_Dept")} VALUES (1, 'Eng'), (2, 'Ops')");
        Exec(factory!, $"INSERT INTO {T("NavScalar_Emp")} VALUES (1, 'ann', 1), (2, 'bob', NULL), (3, 'cid', 2)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            assertions(ctx);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("NavScalar_Emp")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("NavScalar_Dept")}");
        }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Nav_member_predicate_on_live_server(string kind)
        => RunOnLive(kind, ctx =>
        {
            var ids = ctx.Query<Emp>().Where(e => e.Dept!.Title == "Eng").Select(e => e.Id).OrderBy(i => i).ToList();
            Assert.Equal(new[] { 1 }, ids);
        });

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Nav_member_projection_on_live_server(string kind)
        => RunOnLive(kind, ctx =>
        {
            var titles = ctx.Query<Emp>().OrderBy(e => e.Id).Select(e => e.Dept!.Title).ToList();
            Assert.Equal(new string?[] { "Eng", null, "Ops" }, titles);
        });

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Nav_member_anonymous_projection_on_live_server(string kind)
        => RunOnLive(kind, ctx =>
        {
            var rows = ctx.Query<Emp>().OrderBy(e => e.Id)
                .Select(e => new { e.Name, DeptTitle = e.Dept!.Title }).ToList();
            Assert.Equal(new[] { "ann:Eng", "bob:", "cid:Ops" },
                rows.Select(r => $"{r.Name}:{r.DeptTitle}").ToArray());
        });
}
