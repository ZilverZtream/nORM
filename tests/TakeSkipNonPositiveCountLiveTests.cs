using System;
using System.Collections.Generic;
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
/// LINQ Take with a non-positive count returns an empty sequence and Skip with a negative
/// count skips nothing. Compiled queries carry the counts as real SQL parameters, and the
/// Take-then-Skip rewrite emits a (take - skip) window that can be negative even for valid
/// inputs — every provider must clamp instead of erroring (PostgreSQL/SQL Server reject
/// negative LIMIT/FETCH) or returning extra rows.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class TakeSkipNonPositiveCountLiveTests
{
    [Table("TakeSkipParity_Test")]
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

    private static readonly Func<DbContext, int, Task<List<Row>>> _takeN =
        Norm.CompileQuery((DbContext c, int n) => c.Query<Row>().OrderBy(x => x.Id).Take(n));

    private static readonly Func<DbContext, int, Task<List<Row>>> _skipN =
        Norm.CompileQuery((DbContext c, int n) => c.Query<Row>().OrderBy(x => x.Id).Skip(n));

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Take_and_skip_counts_follow_linq_semantics_on_live_server(string kind)
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
        var table = kind == "postgres" ? "\"TakeSkipParity_Test\"" : "TakeSkipParity_Test";

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({idCol}, {nameCol})");
        Exec(factory!, $"INSERT INTO {table} {insertCols} VALUES ('a'),('b'),('c'),('d')");
        try
        {
            using var ctx = new DbContext(factory!(), provider!);

            // Literal (closure-folded) counts.
            var negTake = -1;
            Assert.Empty(ctx.Query<Row>().OrderBy(x => x.Id).Take(negTake).ToList());
            var negSkip = -3;
            Assert.Equal(4, ctx.Query<Row>().OrderBy(x => x.Id).Skip(negSkip).ToList().Count);

            // Take window ending before Skip begins must yield no rows.
            Assert.Empty(ctx.Query<Row>().OrderBy(x => x.Id).Take(3).Skip(5).ToList());

            // Compiled queries: counts are real SQL parameters bound per call.
            Assert.Equal(2, (await _takeN(ctx, 2)).Count);
            Assert.Empty(await _takeN(ctx, -1));
            Assert.Empty(await _takeN(ctx, 0));
            Assert.Equal(2, (await _skipN(ctx, 2)).Count);
            Assert.Equal(4, (await _skipN(ctx, -3)).Count);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }
}
