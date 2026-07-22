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
/// A PROJECTED string equality — <c>Select(x =&gt; new { Eq = x.Name == x.Other })</c> — must
/// materialize the same boolean as C#'s lifted, ordinal equality on every live server. Two hazards
/// compound here and are invisible on SQLite (whose default collation is already case-sensitive):
/// (1) MySQL/SQL Server default collations fold case, so a bare <c>=</c> reports "ABC" == "abc" as
/// true; (2) SQL three-valued logic collapses <c>null == null</c> (C# true) and <c>value != null</c>
/// (C# true) to false. The projection path must compose the sargable ordinal wrap with the null-safe
/// expansion, exactly as the WHERE path does. These parity tests compare against LINQ-to-Objects.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class ProjectedStringEqualityLiveTests
{
    [Table("ProjStrEq_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string? Name { get; set; }
        public string? Other { get; set; }
    }

    //  1 abc/abc (equal)   2 ABC/abc (case differs)   3 xyz/null (one null)
    //  4 null/null (both null)   5 p/q (differ)
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Name = "abc", Other = "abc"  },
        new Row { Id = 2, Name = "ABC", Other = "abc"  },
        new Row { Id = 3, Name = "xyz", Other = null   },
        new Row { Id = 4, Name = null,  Other = null   },
        new Row { Id = 5, Name = "p",   Other = "q"    },
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

    private void RunParity(string kind, Func<IQueryable<Row>, List<(int, bool)>> query)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var q = kind == "postgres" ? "\"" : "";                       // identifier quote
        var cols = $"{q}Name{q} VARCHAR(50) NULL, {q}Other{q} VARCHAR(50) NULL";
        var insertCols = $"({q}Name{q}, {q}Other{q})";
        var table = kind == "postgres" ? "\"ProjStrEq_Test\"" : "ProjStrEq_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {cols})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES " +
                       "('abc','abc'),('ABC','abc'),('xyz',NULL),(NULL,NULL),('p','q')");
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
    public void Projected_equality_of_two_columns_is_ordinal_and_null_safe(string kind)
        => RunParity(kind, q => q.Select(x => new { x.Id, Eq = x.Name == x.Other })
                                 .OrderBy(a => a.Id).ToList()
                                 .Select(a => (a.Id, a.Eq)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Projected_inequality_of_two_columns_is_ordinal_and_null_safe(string kind)
        => RunParity(kind, q => q.Select(x => new { x.Id, Ne = x.Name != x.Other })
                                 .OrderBy(a => a.Id).ToList()
                                 .Select(a => (a.Id, a.Ne)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Projected_equality_against_constant_is_ordinal(string kind)
        => RunParity(kind, q => q.Select(x => new { x.Id, Eq = x.Name == "abc" })
                                 .OrderBy(a => a.Id).ToList()
                                 .Select(a => (a.Id, a.Eq)).ToList());

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Projected_inequality_against_constant_is_ordinal_and_null_aware(string kind)
        // Exercises the asymmetric NotEqual path (right operand a known non-null constant):
        // `(Name IS NULL OR ordinal-not-equal)`. Row 2 ("ABC" != "abc") must be true (ordinal),
        // and row 4 (null != "abc") must be true (C# lifted inequality).
        => RunParity(kind, q => q.Select(x => new { x.Id, Ne = x.Name != "abc" })
                                 .OrderBy(a => a.Id).ToList()
                                 .Select(a => (a.Id, a.Ne)).ToList());
}
