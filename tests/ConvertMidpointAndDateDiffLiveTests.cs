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
/// Convert.ToInt32 over floating sources rounds half to even in .NET (2.5 -> 2,
/// 3.5 -> 4), while raw casts truncate (SQL Server) or round half away from zero
/// (MySQL). DateTime subtraction compared against a TimeSpan must convert both sides
/// to the same seconds domain — a native TIME/INTERVAL operand cannot compare against
/// difference-seconds directly. Parity against LINQ-to-Objects on every configured
/// live server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class ConvertMidpointAndDateDiffLiveTests
{
    [Table("ConvDiffParity_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public double Val { get; set; }
        public DateTime A { get; set; }
        public DateTime B { get; set; }
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

    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Val = 2.5, A = new DateTime(2020, 1, 2, 3, 0, 0), B = new DateTime(2020, 1, 1, 0, 0, 0) },
        new Row { Id = 2, Val = 3.5, A = new DateTime(2020, 6, 1, 0, 0, 30), B = new DateTime(2020, 5, 31, 23, 59, 0) },
        new Row { Id = 3, Val = -2.5, A = new DateTime(2020, 7, 1, 12, 0, 0), B = new DateTime(2020, 7, 1, 0, 0, 0) },
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
        var cols = kind switch
        {
            "mysql" => "Val DOUBLE NOT NULL, A DATETIME(6) NOT NULL, B DATETIME(6) NOT NULL",
            "postgres" => "\"Val\" DOUBLE PRECISION NOT NULL, \"A\" TIMESTAMP NOT NULL, \"B\" TIMESTAMP NOT NULL",
            _ => "Val FLOAT NOT NULL, A DATETIME2 NOT NULL, B DATETIME2 NOT NULL",
        };
        var insertCols = kind == "postgres" ? "(\"Val\", \"A\", \"B\")" : "(Val, A, B)";
        var table = kind == "postgres" ? "\"ConvDiffParity_Test\"" : "ConvDiffParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {cols})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES " +
            "(2.5, '2020-01-02 03:00:00', '2020-01-01 00:00:00')," +
            "(3.5, '2020-06-01 00:00:30', '2020-05-31 23:59:00')," +
            "(-2.5, '2020-07-01 12:00:00', '2020-07-01 00:00:00')");
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
    public void Convert_ToInt32_rounds_half_to_even_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Id).Select(x => Convert.ToInt32(x.Val)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Convert_ToInt32_predicate_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => Convert.ToInt32(x.Val) == 4).Select(x => x.Id).OrderBy(i => i).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void DateTime_difference_equality_against_timespan_on_live_server(string kind)
    {
        var span = TimeSpan.FromSeconds(90);
        RunParity(kind, q => q.Where(x => (x.A - x.B) == span).Select(x => x.Id).OrderBy(i => i).ToList());
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void DateTime_difference_greater_than_timespan_on_live_server(string kind)
    {
        var span = TimeSpan.FromHours(11);
        RunParity(kind, q => q.Where(x => (x.A - x.B) > span).Select(x => x.Id).OrderBy(i => i).ToList());
    }
}
