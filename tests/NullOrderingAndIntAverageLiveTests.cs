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
/// Two C#-vs-SQL divergences that silently change results:
/// (1) LINQ OrderBy sorts nulls FIRST ascending and LAST descending; PostgreSQL defaults to the
///     opposite (NULLS LAST asc / NULLS FIRST desc), silently reordering any sort on a nullable
///     column. The other providers match LINQ already.
/// (2) C# Average over ints returns a double (avg(1,2) = 1.5); T-SQL AVG over an int column does
///     integer division (1) unless the operand is cast — a silently truncated aggregate.
/// Parity is asserted against LINQ-to-Objects on every configured live server.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class NullOrderingAndIntAverageLiveTests
{
    [Table("NoiA_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int? Score { get; set; }
        public int Amount { get; set; }
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

    //  1 (Score=5, Amount=1)   2 (Score=NULL, Amount=2)   3 (Score=3, Amount=0)
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Score = 5, Amount = 1 },
        new Row { Id = 2, Score = null, Amount = 2 },
        new Row { Id = 3, Score = 3, Amount = 0 },
    };

    private void Setup(string kind, out Func<DbConnection> factory, out DatabaseProvider provider, out string table, out bool skip)
    {
        var (f, p, reason) = OpenLive(kind);
        skip = reason != null;
        factory = f!; provider = p!;
        var q = kind == "postgres" ? "\"" : "";
        table = $"{q}NoiA_Test{q}";
        if (skip) return;
        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        string C(string n, string t2) => kind == "postgres" ? $"\"{n}\" {t2}" : $"{n} {t2}";
        var ins = kind == "postgres" ? "(\"Score\", \"Amount\")" : "(Score, Amount)";
        Exec(factory, $"DROP TABLE IF EXISTS {table}");
        Exec(factory, $"CREATE TABLE {table} ({idCol}, {C("Score", "INT NULL")}, {C("Amount", "INT NOT NULL")})");
        Exec(factory, $"INSERT INTO {table} {ins} VALUES (5,1),(NULL,2),(3,0)");
    }

    private void RunParity<T>(string kind, Func<IQueryable<Row>, T> query) where T : notnull
    {
        Setup(kind, out var factory, out var provider, out var table, out var skip);
        if (skip) return;
        try
        {
            using var ctx = new DbContext(factory(), provider);
            var expected = query(Reference.AsQueryable());
            var actual = query(ctx.Query<Row>());
            Assert.Equal(expected, actual);
        }
        finally { Exec(factory, $"DROP TABLE IF EXISTS {table}"); }
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void OrderBy_nullable_puts_nulls_first(string kind)
        // LINQ ascending: null(2), 3(3), 5(1) → ids [2,3,1]
        => RunParity(kind, q => q.OrderBy(x => x.Score).Select(x => x.Id).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void OrderByDescending_nullable_puts_nulls_last(string kind)
        // LINQ descending: 5(1), 3(3), null(2) → ids [1,3,2]
        => RunParity(kind, q => q.OrderByDescending(x => x.Score).Select(x => x.Id).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void ThenBy_nullable_puts_nulls_first(string kind)
        // Constant primary key, nullable secondary: same expected order as OrderBy.
        => RunParity(kind, q => q.OrderBy(x => x.Amount >= 0 ? 0 : 1).ThenBy(x => x.Score).Select(x => x.Id).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Average_of_ints_is_fractional(string kind)
        // LINQ: (1+2+0)/3.0 = 1.0 exactly; but use values yielding a fraction: filter to Amount>=1 → (1+2)/2.0 = 1.5.
        => RunParity(kind, q => q.Where(x => x.Amount >= 1).Average(x => x.Amount));
}
