using System;
using System.Collections.Generic;
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
/// Live parity for global filters inside SelectMany flatten joins: the inner
/// entity's filter renders provider-specific boolean SQL in the ON clause, so all
/// three arms (navigation, correlated query source, cross join) must exclude
/// filtered-out rows on every configured server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class SelectManyGlobalFilterLiveTests
{
    [Table("SmGfL_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Chore> Chores { get; set; } = new();
    }

    [Table("SmGfL_Chore")]
    private class Chore
    {
        [Key] public int Id { get; set; }
        public int EmpId { get; set; }
        public string What { get; set; } = "";
        public bool Done { get; set; }
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
    public void SelectMany_arms_exclude_filtered_children_on_live_server(string kind)
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
        Exec(factory!, $"DROP TABLE IF EXISTS {T("SmGfL_Chore")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("SmGfL_Emp")}");
        Exec(factory!, $"CREATE TABLE {T("SmGfL_Emp")} ({q}Id{q} INT PRIMARY KEY, {q}Name{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("SmGfL_Chore")} ({q}Id{q} INT PRIMARY KEY, {q}EmpId{q} INT NOT NULL, {q}What{q} VARCHAR(50) NOT NULL, {q}Done{q} {boolType} NOT NULL)");
        Exec(factory!, $"INSERT INTO {T("SmGfL_Emp")} VALUES (1, 'ann')");
        Exec(factory!, $"INSERT INTO {T("SmGfL_Chore")} VALUES (1, 1, 'code', {f}), (2, 1, 'ship', {t})");
        try
        {
            var opts = new DbContextOptions();
            opts.AddGlobalFilter<Chore>(c => !c.Done);
            opts.OnModelCreating = mb =>
            {
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Chore>().HasKey(c => c.Id);
                mb.Entity<Emp>().HasMany(e => e.Chores).WithOne().HasForeignKey(c => c.EmpId);
            };
            using var ctx = new DbContext(factory!(), provider!, opts);

            var navArm = ctx.Query<Emp>().SelectMany(e => e.Chores, (e, c) => new { e.Name, c.What })
                .ToList();
            Assert.Single(navArm);
            Assert.Equal("code", navArm[0].What);

            var correlatedArm = ctx.Query<Emp>()
                .SelectMany(e => ctx.Query<Chore>().Where(c => c.EmpId == e.Id), (e, c) => new { e.Name, c.What })
                .ToList();
            Assert.Single(correlatedArm);
            Assert.Equal("code", correlatedArm[0].What);

            var crossArm = ctx.Query<Emp>().SelectMany(e => ctx.Query<Chore>(), (e, c) => new { e.Name, c.What })
                .ToList();
            Assert.Single(crossArm);
            Assert.Equal("code", crossArm[0].What);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("SmGfL_Chore")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("SmGfL_Emp")}");
        }
    }
}
