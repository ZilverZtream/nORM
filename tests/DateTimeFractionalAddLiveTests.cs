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
/// .NET DateTime.AddDays/AddHours/AddMinutes/AddSeconds/AddMilliseconds take a double and
/// are tick-exact (AddDays(1.5) is +36h). SQL date arithmetic disagrees per provider:
/// MySQL's INTERVAL (n) DAY rounds a fractional n to WHOLE days, SQL Server's DATEADD
/// truncates it, and PostgreSQL scales exactly but without the microsecond-grid pinning
/// the materialized comparison needs. These parity tests run the same LINQ query on every
/// configured live server and compare with LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class DateTimeFractionalAddLiveTests
{
    [Table("FracAddParity_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public DateTime Stamp { get; set; }
    }

    private static readonly DateTime Whole = new DateTime(2020, 1, 1, 6, 30, 15);
    private static readonly DateTime Fractional = new DateTime(2020, 3, 15, 23, 59, 59).AddMilliseconds(500);

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
        var stampCol = kind switch
        {
            "mysql" => "Stamp DATETIME(6) NOT NULL",
            "postgres" => "\"Stamp\" TIMESTAMP NOT NULL",
            _ => "Stamp DATETIME2 NOT NULL",
        };
        var insertCols = kind == "postgres" ? "(\"Stamp\")" : "(Stamp)";
        var table = kind == "postgres" ? "\"FracAddParity_Test\"" : "FracAddParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {stampCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('2020-01-01 06:30:15'),('2020-03-15 23:59:59.500')");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            var reference = new[] { new Row { Id = 1, Stamp = Whole }, new Row { Id = 2, Stamp = Fractional } };
            var expected = query(reference.AsQueryable());
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
    public void AddDays_fractional_is_exact_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Stamp.AddDays(1.5)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void AddHours_negative_fractional_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Stamp.AddHours(-2.5)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void AddMinutes_fractional_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Stamp.AddMinutes(90.5)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void AddSeconds_fractional_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Stamp.AddSeconds(1.25)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void AddMilliseconds_fractional_on_live_server(string kind)
        // 2.7ms = 27000 ticks = 2700µs -- representable on every provider's grid.
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Stamp.AddMilliseconds(2.7)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Whole_unit_add_preserves_subsecond_fraction_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Stamp.AddDays(1)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Fractional_add_predicate_matches_dotnet_rows_on_live_server(string kind)
    {
        var cutoff = new DateTime(2020, 1, 2, 18, 0, 0);
        RunParity(kind, q => q.Where(x => x.Stamp.AddDays(1.5) > cutoff).Select(x => x.Id).OrderBy(i => i).ToList());
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void AddTicks_on_live_server(string kind)
        // 7500000 ticks = 750ms -- on the microsecond grid everywhere.
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => x.Stamp.AddTicks(7500000)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void AddMonths_clamps_day_overflow_on_live_server(string kind)
    {
        // C# clamps Jan 31 + 1 month to the end of February; the servers' native
        // calendar arithmetic does the same -- pinned here so a translation change
        // can never regress to a normalize-forward form.
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var stampCol = kind switch
        {
            "mysql" => "Stamp DATETIME(6) NOT NULL",
            "postgres" => "\"Stamp\" TIMESTAMP NOT NULL",
            _ => "Stamp DATETIME2 NOT NULL",
        };
        var insertCols = kind == "postgres" ? "(\"Stamp\")" : "(Stamp)";
        var table = kind == "postgres" ? "\"FracAddParity_Test\"" : "FracAddParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {stampCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('2020-01-31 10:20:30'),('2020-02-29 23:59:59')");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);
            var reference = new[]
            {
                new Row { Id = 1, Stamp = new DateTime(2020, 1, 31, 10, 20, 30) },
                new Row { Id = 2, Stamp = new DateTime(2020, 2, 29, 23, 59, 59) },
            };
            var expectedMonths = reference.AsQueryable().OrderBy(x => x.Id).Select(x => x.Stamp.AddMonths(1)).ToList();
            var actualMonths = ctx.Query<Row>().OrderBy(x => x.Id).Select(x => x.Stamp.AddMonths(1)).ToList();
            Assert.Equal(expectedMonths, actualMonths);

            var expectedYears = reference.AsQueryable().OrderBy(x => x.Id).Select(x => x.Stamp.AddYears(1)).ToList();
            var actualYears = ctx.Query<Row>().OrderBy(x => x.Id).Select(x => x.Stamp.AddYears(1)).ToList();
            Assert.Equal(expectedYears, actualYears);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }
}
