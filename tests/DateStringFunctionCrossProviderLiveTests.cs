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
/// DateTime component extraction (Year/Month) and arithmetic (AddDays), and string functions (Length,
/// Substring, Contains) in key positions translate to sharply different SQL per dialect — strftime vs
/// EXTRACT vs DATEPART vs YEAR(), SUBSTR vs SUBSTRING, and so on. These were verified against
/// LINQ-to-Objects on SQLite; this pins the same parity on every configured live server so a dialect
/// function defect surfaces as a failure rather than a silent-wrong result.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class DateStringFunctionCrossProviderLiveTests
{
    [Table("DsxRow")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public DateTime Created { get; set; }
    }

    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Name = "aa",     Created = new DateTime(2023, 1, 15) },
        new Row { Id = 2, Name = "b",      Created = new DateTime(2023, 6, 20) },
        new Row { Id = 3, Name = "ccc",    Created = new DateTime(2024, 1, 10) },
        new Row { Id = 4, Name = "dddd",   Created = new DateTime(2024, 6, 5)  },
        new Row { Id = 5, Name = "ee",     Created = new DateTime(2024, 6, 25) },
        new Row { Id = 6, Name = "ffffff", Created = new DateTime(2023, 1, 30) },
    };

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
        var createdType = kind switch { "mysql" => "DATETIME", "postgres" => "TIMESTAMP", _ => "DATETIME2" };
        var cols = kind == "postgres"
            ? $"\"Name\" VARCHAR(50) NOT NULL, \"Created\" {createdType} NOT NULL"
            : $"Name VARCHAR(50) NOT NULL, Created {createdType} NOT NULL";
        var insertCols = kind == "postgres" ? "(\"Name\", \"Created\")" : "(Name, Created)";
        var table = kind == "postgres" ? "\"DsxRow\"" : "DsxRow";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {cols})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES " +
            "('aa','2023-01-15 00:00:00'),('b','2023-06-20 00:00:00'),('ccc','2024-01-10 00:00:00')," +
            "('dddd','2024-06-05 00:00:00'),('ee','2024-06-25 00:00:00'),('ffffff','2023-01-30 00:00:00')");
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
    public void GroupBy_datetime_year_on_live_server(string kind)
        => RunParity(kind, q => q.GroupBy(x => x.Created.Year).Select(g => new { g.Key, C = g.Count() })
            .ToList().OrderBy(x => x.Key).Select(x => $"{x.Key}:{x.C}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Where_datetime_year_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => x.Created.Year == 2024).OrderBy(x => x.Id).Select(x => x.Id).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void GroupBy_year_month_composite_on_live_server(string kind)
        => RunParity(kind, q => q.GroupBy(x => new { x.Created.Year, x.Created.Month }).Select(g => new { g.Key.Year, g.Key.Month, C = g.Count() })
            .ToList().OrderBy(x => x.Year).ThenBy(x => x.Month).Select(x => $"{x.Year}-{x.Month}:{x.C}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void OrderBy_string_length_on_live_server(string kind)
        => RunParity(kind, q => q.OrderBy(x => x.Name.Length).ThenBy(x => x.Id).Select(x => x.Id).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void GroupBy_string_substring_on_live_server(string kind)
        => RunParity(kind, q => q.GroupBy(x => x.Name.Substring(0, 1)).Select(g => new { g.Key, C = g.Count() })
            .ToList().OrderBy(x => x.Key, StringComparer.Ordinal).Select(x => $"{x.Key}:{x.C}").ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Where_string_contains_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => x.Name.Contains("c")).OrderBy(x => x.Id).Select(x => x.Id).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Where_datetime_adddays_on_live_server(string kind)
    {
        var cutoff = new DateTime(2024, 6, 10);
        RunParity(kind, q => q.Where(x => x.Created.AddDays(10) > cutoff).OrderBy(x => x.Id).Select(x => x.Id).ToList());
    }
}
