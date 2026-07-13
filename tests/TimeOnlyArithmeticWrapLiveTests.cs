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
/// .NET TimeOnly arithmetic wraps around midnight (23:30 + 2.5h is 02:00). MySQL's
/// ADDTIME does NOT wrap — TIME values happily exceed 24 hours — so its translation
/// must fold through a positive modulo; SQL Server's DATEADD-on-TIME and PostgreSQL's
/// TIME + INTERVAL wrap natively. Parity against LINQ-to-Objects on every configured
/// live server, in projections and predicates, both directions.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class TimeOnlyArithmeticWrapLiveTests
{
    [Table("TimeWrapParity_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public TimeOnly Start { get; set; }
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

    //  1 23:30   2 01:15
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Start = new TimeOnly(23, 30, 0) },
        new Row { Id = 2, Start = new TimeOnly(1, 15, 0) },
    };

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
        var startCol = kind == "postgres" ? "\"Start\" TIME NOT NULL" : "Start TIME NOT NULL";
        var insertCols = kind == "postgres" ? "(\"Start\")" : "(Start)";
        var table = kind == "postgres" ? "\"TimeWrapParity_Test\"" : "TimeWrapParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {startCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('23:30:00'),('01:15:00')");
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
    public void AddHours_projection_wraps_midnight_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Start.AddHours(2.5)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void AddHours_negative_projection_wraps_backwards_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Start.AddHours(-3)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void AddMinutes_predicate_wraps_on_live_server(string kind)
    {
        var cutoff = new TimeOnly(3, 0, 0);
        RunParity(kind, q => q.Where(x => x.Start.AddMinutes(45) < cutoff).Select(x => x.Id).OrderBy(i => i).ToList());
    }
}
