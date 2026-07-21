using System;
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
/// Live validation that a set-op over a scalar projection with an IDENTITY order key
/// (`.Select(r => r.IntVal).Union(...).OrderBy(x => x)`) emits a compound query whose
/// ORDER BY references the single result column by ORDINAL — valid on SQL Server /
/// MySQL / PostgreSQL. Int keys are collation-free so the oracle holds across providers.
/// Skips providers not configured.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class SetOpScalarIdentityOrderLiveTests
{
    [Table("SoiLive")]
    private class SoiRow
    {
        [Key] public int Id { get; set; }
        public int IntVal { get; set; }
    }

    private static readonly SoiRow[] Seed = Enumerable.Range(1, 30)
        .Select(i => new SoiRow { Id = i, IntVal = ((i * 37) % 61) - 30 }).ToArray();

    private static (Func<DbConnection>?, DatabaseProvider?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null);
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()));
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null);
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()));
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null);
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider());
            }
            default: throw new ArgumentOutOfRangeException(nameof(kind));
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

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task Union_scalar_identity_order_matches_linq_live(string kind)
    {
        var (factory, provider) = OpenLive(kind);
        if (factory == null) return; // provider not configured

        // Postgres folds unquoted identifiers to lowercase while nORM quotes the mapped name
        // case-preserved — quote the DDL identifiers so the raw table matches nORM's references.
        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        Exec(factory, $"DROP TABLE IF EXISTS {T("SoiLive")}");
        Exec(factory, $"CREATE TABLE {T("SoiLive")} ({q}Id{q} INT PRIMARY KEY, {q}IntVal{q} INT NOT NULL)");
        try
        {
            using var ctx = new DbContext(factory(), provider!);
            foreach (var r in Seed) ctx.Add(new SoiRow { Id = r.Id, IntVal = r.IntVal });
            await ctx.SaveChangesAsync();

            var expectedDesc = Seed.Where(r => r.IntVal >= 0).Select(r => r.IntVal)
                .Union(Seed.Where(r => r.Id % 3 == 0).Select(r => r.IntVal))
                .OrderByDescending(x => x).ToList();
            // int scalar sequence → sync enumeration (ToListAsync has a class constraint).
            var actualDesc = ctx.Query<SoiRow>().Where(r => r.IntVal >= 0).Select(r => r.IntVal)
                .Union(ctx.Query<SoiRow>().Where(r => r.Id % 3 == 0).Select(r => r.IntVal))
                .OrderByDescending(x => x).ToList();
            Assert.Equal(expectedDesc, actualDesc);

            var expectedAsc = Seed.Where(r => r.IntVal >= 0).Select(r => r.IntVal)
                .Union(Seed.Where(r => r.Id % 3 == 0).Select(r => r.IntVal))
                .OrderBy(x => x).ToList();
            var actualAsc = ctx.Query<SoiRow>().Where(r => r.IntVal >= 0).Select(r => r.IntVal)
                .Union(ctx.Query<SoiRow>().Where(r => r.Id % 3 == 0).Select(r => r.IntVal))
                .OrderBy(x => x).ToList();
            Assert.Equal(expectedAsc, actualAsc);
        }
        finally
        {
            Exec(factory, $"DROP TABLE IF EXISTS {T("SoiLive")}");
        }
    }
}
