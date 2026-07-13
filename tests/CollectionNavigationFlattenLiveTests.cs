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
/// Live parity for SelectMany over a collection navigation: the derived-table
/// flatten (child as root) with downstream Where/Select/aggregates/paging must run
/// on every configured server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class CollectionNavigationFlattenLiveTests
{
    [Table("NavFlatL_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("NavFlatL_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public string What { get; set; } = "";
        public int Effort { get; set; }
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
    public void SelectMany_collection_nav_flatten_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        Exec(factory!, $"DROP TABLE IF EXISTS {T("NavFlatL_Chore")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("NavFlatL_Emp")}");
        Exec(factory!, $"CREATE TABLE {T("NavFlatL_Emp")} ({q}Id{q} INT PRIMARY KEY, {q}Name{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("NavFlatL_Chore")} ({q}Id{q} INT PRIMARY KEY, {q}EmpId{q} INT NOT NULL, {q}What{q} VARCHAR(50) NOT NULL, {q}Effort{q} INT NOT NULL)");
        Exec(factory!, $"INSERT INTO {T("NavFlatL_Emp")} VALUES (1, 'ann'), (2, 'bob'), (3, 'cid')");
        Exec(factory!, $"INSERT INTO {T("NavFlatL_Chore")} VALUES (1, 1, 'code', 5), (2, 1, 'ship', 3), (3, 2, 'ops', 7)");
        try
        {
            using var ctx = new DbContext(factory!(), provider!, new nORM.Configuration.DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    mb.Entity<Emp>().HasKey(e => e.Id);
                    mb.Entity<Chore>().HasKey(c => c.Id);
                    mb.Entity<Emp>().HasMany(e => e.Chores).WithOne().HasForeignKey(c => c.EmpId);
                }
            });

            var whats = ctx.Query<Emp>().SelectMany(e => e.Chores).Where(c => c.Effort > 4)
                .Select(c => c.What).ToList().OrderBy(w => w).ToList();
            Assert.Equal(new[] { "code", "ops" }, whats);

            Assert.Equal(15, ctx.Query<Emp>().SelectMany(e => e.Chores).Sum(c => c.Effort));

            var paged = ctx.Query<Emp>().SelectMany(e => e.Chores)
                .OrderByDescending(c => c.Effort).Take(2).Select(c => c.What).ToList();
            Assert.Equal(new[] { "ops", "code" }, paged);

            var filteredNav = ctx.Query<Emp>().Where(e => e.Name == "ann").SelectMany(e => e.Chores)
                .Select(c => c.What).ToList().OrderBy(w => w).ToList();
            Assert.Equal(new[] { "code", "ship" }, filteredNav);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("NavFlatL_Chore")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("NavFlatL_Emp")}");
        }
    }
}
