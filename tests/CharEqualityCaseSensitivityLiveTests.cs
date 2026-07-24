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
/// C# <see cref="char"/> equality is ordinal (case-sensitive): <c>Ch == 'a'</c> must not match 'A'.
/// nORM stores a char column as single-character TEXT, and MySQL / SQL Server default collations fold
/// case, so a bare <c>=</c> matches extra rows there. The read fast paths already apply a sargable
/// ordinal wrap for <see cref="string"/> columns; this pins that the same wrap is applied for
/// <see cref="char"/> columns (the simple-Where list and filtered-ordered emitters checked
/// <c>string</c> only). PostgreSQL and SQLite are already ordinal.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class CharEqualityCaseSensitivityLiveTests
{
    [Table("CharEqParity_Test")]
    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public char Ch { get; set; }
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

    //  1 a   2 A   3 b
    private static readonly Row[] Reference =
    {
        new Row { Id = 1, Ch = 'a' }, new Row { Id = 2, Ch = 'A' }, new Row { Id = 3, Ch = 'b' },
    };

    private void RunParity(string kind, Func<IQueryable<Row>, List<int>> query)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var idCol = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY AUTO_INCREMENT",
            "postgres" => "\"Id\" SERIAL PRIMARY KEY",
            _ => "Id INT IDENTITY PRIMARY KEY",
        };
        var chCol = kind == "postgres" ? "\"Ch\" CHAR(1) NOT NULL" : "Ch CHAR(1) NOT NULL";
        var insertCols = kind == "postgres" ? "(\"Ch\")" : "(Ch)";
        var table = kind == "postgres" ? "\"CharEqParity_Test\"" : "CharEqParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {chCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('a'),('A'),('b')");
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

    // Filtered-ordered fast path (Where + Select + OrderBy).
    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Char_equality_is_ordinal_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => x.Ch == 'a').Select(x => x.Id).OrderBy(i => i).ToList());

    // Simple-Where list fast path (no Select/OrderBy after the Where -> BuildEqualityPredicate).
    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public void Bare_where_char_equality_is_ordinal_on_live_server(string kind)
        => RunParity(kind, q => q.Where(x => x.Ch == 'a').ToList().Select(x => x.Id).OrderBy(i => i).ToList());
}
