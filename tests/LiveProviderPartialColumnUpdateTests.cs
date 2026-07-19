using System;
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
/// Live validation of partial-column UPDATEs across SQL Server / PostgreSQL / MySQL: SaveChanges writes only
/// the changed column, so a concurrent writer's change to an UNCHANGED column survives (a full-row UPDATE
/// would clobber it), and the positional @pN parameters stay aligned on every provider's SQL dialect. Skips
/// providers not configured.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderPartialColumnUpdateTests
{
    [Table("PcuLiveWidget")]
    private class Widget
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public int C { get; set; }
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

    private static (int A, int B, int C) ReadRow(Func<DbConnection> factory, string table, string q)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT {q}A{q}, {q}B{q}, {q}C{q} FROM {table} WHERE {q}Id{q} = 1";
        using var r = cmd.ExecuteReader(); r.Read();
        return (Convert.ToInt32(r.GetValue(0)), Convert.ToInt32(r.GetValue(1)), Convert.ToInt32(r.GetValue(2)));
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Partial_update_leaves_unchanged_columns_untouched_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        Exec(factory!, $"DROP TABLE IF EXISTS {T("PcuLiveWidget")}");
        Exec(factory!, $"CREATE TABLE {T("PcuLiveWidget")} ({q}Id{q} INT PRIMARY KEY, {q}A{q} INT NOT NULL, {q}B{q} INT NOT NULL, {q}C{q} INT NOT NULL)");
        Exec(factory!, $"INSERT INTO {T("PcuLiveWidget")} VALUES (1, 10, 20, 30)");
        try
        {
            var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
            using var ctx = new DbContext(factory!(), provider!, opts);

            var w = (await ctx.Query<Widget>().Where(x => x.Id == 1).ToListAsync()).Single();
            w.A = 11;   // only A changed

            // Concurrent writer (separate connection, auto-committed) changes the UNCHANGED column B.
            Exec(factory!, $"UPDATE {T("PcuLiveWidget")} SET {q}B{q} = 99 WHERE {q}Id{q} = 1");

            await ctx.SaveChangesAsync();   // partial UPDATE: SET A only

            var (a, b, c) = ReadRow(factory!, T("PcuLiveWidget"), q);
            Assert.Equal(11, a);   // my change applied
            Assert.Equal(99, b);   // concurrent change to the unchanged column SURVIVED (partial update)
            Assert.Equal(30, c);
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("PcuLiveWidget")}");
        }
    }
}
