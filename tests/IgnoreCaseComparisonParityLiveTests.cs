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
/// OrdinalIgnoreCase comparison parity against LINQ-to-Objects on every configured
/// live server: PostgreSQL's byte-exact equality needs the case-fold, and the
/// CI-collation providers must not double-apply it.
/// </summary>

[Trait("Category", TestCategory.LiveProvider)]
public class IgnoreCaseComparisonParityLiveTests
{
    [Table("IcParity_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
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
        new Row { Id = 1, Name = "abc" }, new Row { Id = 2, Name = "ABC" },
        new Row { Id = 3, Name = "AbC" }, new Row { Id = 4, Name = "xyz" },
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
        var nameCol = kind == "postgres" ? "\"Name\" VARCHAR(50) NOT NULL" : "Name VARCHAR(50) NOT NULL";
        var insertCols = kind == "postgres" ? "(\"Name\")" : "(Name)";
        var table = kind == "postgres" ? "\"IcParity_Test\"" : "IcParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {nameCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('abc'),('ABC'),('AbC'),('xyz')");
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
    public void Equals_ignore_case_on_live(string kind)
        => RunParity(kind, q => q.Where(r => r.Name.Equals("aBc", StringComparison.OrdinalIgnoreCase)).Select(r => r.Id).OrderBy(i => i).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Contains_ignore_case_on_live(string kind)
        => RunParity(kind, q => q.Where(r => r.Name.Contains("BC", StringComparison.OrdinalIgnoreCase)).Select(r => r.Id).OrderBy(i => i).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void StartsWith_ignore_case_on_live(string kind)
        => RunParity(kind, q => q.Where(r => r.Name.StartsWith("AB", StringComparison.OrdinalIgnoreCase)).Select(r => r.Id).OrderBy(i => i).ToList());
}
