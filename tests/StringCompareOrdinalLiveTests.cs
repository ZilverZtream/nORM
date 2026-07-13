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
/// string.CompareOrdinal and string.Compare(..., StringComparison.Ordinal) compare by
/// UTF-16 code units: 'M' (77) sorts before 'm' (109). On MySQL and SQL Server the
/// default case-insensitive collation would treat them as equal, so the relational
/// rewrite of ordinal comparisons must force a binary comparison. These parity tests
/// run the same LINQ query on every configured live server and compare with
/// LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class StringCompareOrdinalLiveTests
{
    [Table("CmpOrdParity_Test")]
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

    //  1 apple   2 melon   3 zebra   4 Melon
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Name = "apple" }, new Row { Id = 2, Name = "melon" },
        new Row { Id = 3, Name = "zebra" }, new Row { Id = 4, Name = "Melon" },
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
        var table = kind == "postgres" ? "\"CmpOrdParity_Test\"" : "CmpOrdParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {nameCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('apple'),('melon'),('zebra'),('Melon')");
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
    public void CompareOrdinal_relational_is_ordinal_on_live_server(string kind)
        // Ordinal: "Melon" < "melon", so >= 0 keeps melon and zebra only.
        => RunParity(kind, q => q.Where(x => string.CompareOrdinal(x.Name, "melon") >= 0).Select(x => x.Id).OrderBy(i => i).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Compare_with_ordinal_mode_relational_is_ordinal_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => string.Compare(x.Name, "melon", StringComparison.Ordinal) < 0).Select(x => x.Id).OrderBy(i => i).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void CompareOrdinal_equality_to_zero_is_ordinal_on_live_server(string kind)
        // == 0 must NOT match the case variant on CI-collation providers.
        => RunParity(kind, q => q.Where(x => string.CompareOrdinal(x.Name, "melon") == 0).Select(x => x.Id).OrderBy(i => i).ToList());
}
