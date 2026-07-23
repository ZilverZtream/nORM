using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Value round-trip fidelity on live servers, where provider type systems can lose precision a CLR value
/// carried — the ledger records a real MySQL DATETIME(0) sub-second truncation on the bulk-staging path.
/// With precise columns (DATETIME(6)/TIMESTAMP/DATETIME2, DECIMAL(28,10), utf8/NVARCHAR), a microsecond
/// DateTime, a high-precision decimal, and a unicode string written through nORM's SaveChanges must read
/// back exactly. A binding-level truncation would surface as a mismatch.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class ValueFidelityCrossProviderLiveTests
{
    [Table("VfxRow")]
    private class VfxRow
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.None)] public int Id { get; set; }
        public string Txt { get; set; } = "";
        public DateTime Dt { get; set; }
        public decimal Amount { get; set; }
    }

    private static readonly VfxRow[] Cases =
    {
        new() { Id = 1, Txt = "emoji 🎉 CJK 日本語 é ñ", Dt = new DateTime(2023, 6, 15, 12, 30, 45).AddTicks(1234560), Amount = 12345678.1234567890m },
        new() { Id = 2, Txt = "quote's \"x\" back\\slash", Dt = new DateTime(1999, 12, 31, 23, 59, 59).AddTicks(9999990), Amount = 0.0000000001m },
        new() { Id = 3, Txt = "", Dt = new DateTime(2000, 1, 1, 0, 0, 0), Amount = -87654321.0000000001m },
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

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Microsecond_datetime_decimal_and_unicode_round_trip_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var table = kind == "postgres" ? "\"VfxRow\"" : "VfxRow";
        var ddl = kind switch
        {
            "mysql" => "Id INT PRIMARY KEY, Txt VARCHAR(200) CHARACTER SET utf8mb4 NOT NULL, Dt DATETIME(6) NOT NULL, Amount DECIMAL(28,10) NOT NULL",
            "postgres" => "\"Id\" INT PRIMARY KEY, \"Txt\" VARCHAR(200) NOT NULL, \"Dt\" TIMESTAMP NOT NULL, \"Amount\" DECIMAL(28,10) NOT NULL",
            _ => "Id INT PRIMARY KEY, Txt NVARCHAR(200) NOT NULL, Dt DATETIME2 NOT NULL, Amount DECIMAL(28,10) NOT NULL",
        };

        Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        Exec(factory!, $"CREATE TABLE {table} ({ddl})");
        try
        {
            await using (var ctx = new DbContext(factory!(), provider!, new DbContextOptions(), ownsConnection: true))
            {
                foreach (var c in Cases)
                    ctx.Add(new VfxRow { Id = c.Id, Txt = c.Txt, Dt = c.Dt, Amount = c.Amount });
                await ctx.SaveChangesAsync();
            }

            await using var readCtx = new DbContext(factory!(), provider!, new DbContextOptions(), ownsConnection: true);
            var read = (await readCtx.Query<VfxRow>().ToListAsync()).ToDictionary(r => r.Id);

            foreach (var c in Cases)
            {
                var r = read[c.Id];
                Assert.Equal(c.Txt, r.Txt);
                Assert.Equal(c.Dt, r.Dt);
                Assert.Equal(c.Amount, r.Amount);
            }
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {table}");
        }
    }
}
