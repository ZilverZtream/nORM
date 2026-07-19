using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Validates <c>ReleaseSavepointAsync</c> against every live provider: PostgreSQL and MySQL release via the
/// ADO.NET <c>DbTransaction.Release(string)</c> API (reflected), and SQL Server is a no-op (the engine
/// auto-releases). In all cases the work done since the savepoint is kept and the transaction commits. Guards
/// against a provider whose ADO.NET driver lacks a working <c>Release</c>. Skips providers not configured.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class LiveProviderReleaseSavepointTests
{
    [Table("SpRelLive")]
    private class Widget { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

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

    private static long Count(Func<DbConnection> factory, string table)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Theory]
    [InlineData("mysql")]
    [InlineData("postgres")]
    [InlineData("sqlserver")]
    public async Task Release_savepoint_keeps_the_work_on_live_server(string kind)
    {
        var (factory, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        var q = kind == "postgres" ? "\"" : "";
        string T(string n) => kind == "postgres" ? $"\"{n}\"" : n;
        Exec(factory!, $"DROP TABLE IF EXISTS {T("SpRelLive")}");
        Exec(factory!, $"CREATE TABLE {T("SpRelLive")} ({q}Id{q} INT PRIMARY KEY, {q}Name{q} VARCHAR(50) NOT NULL)");
        try
        {
            var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
            using var ctx = new DbContext(factory!(), provider!, opts);

            await using (var tx = await ctx.Database.BeginTransactionAsync())
            {
                await ctx.InsertAsync(new Widget { Id = 1, Name = "before" });
                await tx.CreateSavepointAsync("sp1");
                await ctx.InsertAsync(new Widget { Id = 2, Name = "after" });   // work done AFTER the savepoint
                await tx.ReleaseSavepointAsync("sp1");                          // must not throw on any provider
                await tx.CommitAsync();
            }

            Assert.Equal(2, Count(factory!, T("SpRelLive")));   // release kept the post-savepoint row
        }
        finally
        {
            Exec(factory!, $"DROP TABLE IF EXISTS {T("SpRelLive")}");
        }
    }
}
