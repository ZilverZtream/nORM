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
/// Live parity for eager-loading reference navigations: the principal batch fetch
/// (WHERE pk IN ...) and the chained multi-result-set load (collection→reference)
/// must run on every configured server, with NULL and dangling FKs leaving the
/// navigation null.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class ReferenceNavigationIncludeLiveTests
{
    [Table("RefIncL_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("RefIncL_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("RefIncL_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
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
    public void Include_reference_nav_and_collection_chain_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        Exec(factory!, $"DROP TABLE IF EXISTS {T("RefIncL_Chore")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("RefIncL_Emp")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("RefIncL_Dept")}");
        Exec(factory!, $"CREATE TABLE {T("RefIncL_Dept")} ({q}Id{q} INT PRIMARY KEY, {q}Title{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("RefIncL_Emp")} ({q}Id{q} INT PRIMARY KEY, {q}Name{q} VARCHAR(50) NOT NULL, {q}DeptId{q} INT NULL)");
        Exec(factory!, $"CREATE TABLE {T("RefIncL_Chore")} ({q}Id{q} INT PRIMARY KEY, {q}EmpId{q} INT NOT NULL, {q}DeptId{q} INT NULL)");
        Exec(factory!, $"INSERT INTO {T("RefIncL_Dept")} VALUES (1, 'Eng'), (2, 'Ops')");
        Exec(factory!, $"INSERT INTO {T("RefIncL_Emp")} VALUES (1, 'ann', 1), (2, 'bob', NULL), (3, 'cid', 1), (4, 'dan', 99)");
        Exec(factory!, $"INSERT INTO {T("RefIncL_Chore")} VALUES (1, 1, 2), (2, 1, NULL), (3, 3, 1)");
        try
        {
            using (var ctx = new DbContext(factory!(), provider!))
            {
                var emps = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Dept!).AsSplitQuery()
                    .ToList().OrderBy(e => e.Id).ToList();
                Assert.Equal("Eng", emps[0].Dept?.Title);
                Assert.Null(emps[1].Dept);
                Assert.Same(emps[0].Dept, emps[2].Dept);
                Assert.Null(emps[3].Dept);
            }

            using (var ctx = new DbContext(factory!(), provider!))
            {
                var emps = ((INormQueryable<Emp>)ctx.Query<Emp>()).Include(e => e.Chores).ThenInclude(c => c.Dept!)
                    .AsSplitQuery().ToList().OrderBy(e => e.Id).ToList();
                var annChores = emps[0].Chores.OrderBy(c => c.Id).ToList();
                Assert.Equal("Ops", annChores[0].Dept?.Title);
                Assert.Null(annChores[1].Dept);
                Assert.Equal("Eng", emps[2].Chores.Single().Dept?.Title);
            }
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("RefIncL_Chore")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("RefIncL_Emp")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("RefIncL_Dept")}");
        }
    }
}
