using System;
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
/// Live parity for reference-navigation writes under REAL foreign-key constraints and
/// identity columns: a graph add (dependent → principal, both new) must insert the
/// principal first and stamp the dependent's FK with the database-generated key, and
/// a navigation reassignment must update the FK — on every configured server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class ReferenceNavigationSaveChangesLiveTests
{
    [Table("RefSaveL_Dept")]
    private class Dept
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("RefSaveL_Emp")]
    private class Emp
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
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

    private static object? Scalar(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Graph_add_and_nav_reassignment_under_fk_constraints(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        var identity = kind switch
        {
            "mysql" => "INT AUTO_INCREMENT PRIMARY KEY",
            "postgres" => "SERIAL PRIMARY KEY",
            _ => "INT IDENTITY(1,1) PRIMARY KEY",
        };
        Exec(factory!, $"DROP TABLE IF EXISTS {T("RefSaveL_Emp")}");
        Exec(factory!, $"DROP TABLE IF EXISTS {T("RefSaveL_Dept")}");
        Exec(factory!, $"CREATE TABLE {T("RefSaveL_Dept")} ({q}Id{q} {identity}, {q}Title{q} VARCHAR(50) NOT NULL)");
        Exec(factory!, $"CREATE TABLE {T("RefSaveL_Emp")} ({q}Id{q} {identity}, {q}Name{q} VARCHAR(50) NOT NULL, "
            + $"{q}DeptId{q} INT NULL, FOREIGN KEY ({q}DeptId{q}) REFERENCES {T("RefSaveL_Dept")}({q}Id{q}))");
        try
        {
            // A brand-new dependent-with-principal graph: the FK constraint proves the
            // principal inserted first and the generated key reached the dependent.
            using (var ctx = new DbContext(factory!(), provider!))
            {
                ctx.Add(new Emp { Name = "eve", Dept = new Dept { Title = "Lab" } });
                await ctx.SaveChangesAsync();
            }
            Assert.Equal("Lab", Scalar(factory!,
                $"SELECT d.{q}Title{q} FROM {T("RefSaveL_Emp")} e JOIN {T("RefSaveL_Dept")} d ON d.{q}Id{q} = e.{q}DeptId{q} WHERE e.{q}Name{q} = 'eve'"));

            // Reassignment to another (new) principal on a freshly loaded dependent.
            using (var ctx = new DbContext(factory!(), provider!))
            {
                var emp = ctx.Query<Emp>().Single(e => e.Name == "eve");
                emp.Dept = new Dept { Title = "Ops" };
                await ctx.SaveChangesAsync();
            }
            Assert.Equal("Ops", Scalar(factory!,
                $"SELECT d.{q}Title{q} FROM {T("RefSaveL_Emp")} e JOIN {T("RefSaveL_Dept")} d ON d.{q}Id{q} = e.{q}DeptId{q} WHERE e.{q}Name{q} = 'eve'"));
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("RefSaveL_Emp")}");
            Exec(factory!, $"DROP TABLE IF EXISTS {T("RefSaveL_Dept")}");
        }
    }
}
