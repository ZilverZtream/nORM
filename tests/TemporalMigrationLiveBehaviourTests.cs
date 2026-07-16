using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Live behavioural contract for temporal-aware migrations on the trigger-emulated servers:
/// after a generator-produced ADD COLUMN migration on a temporal table, versioning continues
/// IMMEDIATELY (no bootstrap), the new column's values reach the history table from the first
/// write, and AsOf still reconstructs pre-migration state. Mirrors the SQLite behavioural
/// contract on live SQL Server, PostgreSQL, and MySQL.
/// </summary>
[Trait("Category", TestCategory.LiveProvider)]
public class TemporalMigrationLiveBehaviourTests
{
    [Table("TmigLiveB")]
    private class RowV1
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }

    [Table("TmigLiveB")]
    private class RowV2
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public int? W { get; set; }
    }

    private static (Func<DbConnection>?, DatabaseProvider?, IMigrationSqlGenerator?, string?) OpenLive(string kind)
    {
        switch (kind)
        {
            case "mysql":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs)) return (null, null, null, "NORM_TEST_MYSQL not set");
                var t = Type.GetType("MySqlConnector.MySqlConnection, MySqlConnector")!;
                return (() => Open(t, cs), new MySqlProvider(new SqliteParameterFactory()), new MySqlMigrationSqlGenerator(), null);
            }
            case "postgres":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs)) return (null, null, null, "NORM_TEST_POSTGRES not set");
                var t = Type.GetType("Npgsql.NpgsqlConnection, Npgsql")!;
                return (() => Open(t, cs), new PostgresProvider(new SqliteParameterFactory()), new PostgresMigrationSqlGenerator(), null);
            }
            case "sqlserver":
            {
                var cs = LiveProviderEnvironment.GetByCanonicalName("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs)) return (null, null, null, "NORM_TEST_SQLSERVER not set");
                var t = Type.GetType("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient")!;
                return (() => Open(t, cs), new SqlServerProvider(), new SqlServerMigrationSqlGenerator(), null);
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

    private static void ExecIgnore(Func<DbConnection> factory, params string[] sqls)
    {
        foreach (var sql in sqls)
        {
            try
            {
                using var cn = factory();
                using var cmd = cn.CreateCommand();
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
            catch { /* cleanup best-effort */ }
        }
    }

    private static long Scalar(Func<DbConnection> factory, string sql)
    {
        using var cn = factory();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static TableSchema Build(bool withW)
    {
        var t = new TableSchema { Name = "TmigLiveB", IsTemporal = true };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "V", ClrType = typeof(int).FullName!, IsNullable = false });
        if (withW) t.Columns.Add(new ColumnSchema { Name = "W", ClrType = typeof(int).FullName!, IsNullable = true });
        return t;
    }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    [InlineData("mysql")]
    public async Task Add_column_migration_keeps_live_versioning_alive(string kind)
    {
        var (factory, provider, generator, skip) = OpenLive(kind);
        if (skip != null) return;
        using (var probe = factory!())
        {
            // Temporal DDL is rejected in provider-owned databases (master/postgres/mysql);
            // this contract needs the connection string to target an application database
            // such as 'normtest'. Skip honestly instead of failing on environment shape.
            var db = probe.Database;
            if (db is "master" or "postgres" or "mysql" or "sys" or "model" or "msdb" or "tempdb")
                return;
        }

        // Best-effort cleanup from previous runs (triggers/functions die with their tables).
        ExecIgnore(factory!,
            "DROP TABLE TmigLiveB_History", "DROP TABLE TmigLiveB",
            "DROP FUNCTION IF EXISTS \"TmigLiveB_TemporalFunction\"()");
        ExecIgnore(factory!, "CREATE TABLE TmigLiveB (Id INT PRIMARY KEY, V INT NOT NULL)");

        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<RowV1>() };
        opts.EnableTemporalVersioning();
        var betweenV1V2 = DateTime.UtcNow;
        await using (var ctx = new DbContext(factory!(), provider!, opts))
        {
            var row = new RowV1 { Id = 1, V = 1 };
            ctx.Add(row);
            await ctx.SaveChangesAsync();
            await Task.Delay(300);
            betweenV1V2 = DateTime.UtcNow;
            await Task.Delay(300);
            row.V = 2;
            await ctx.SaveChangesAsync();
        }

        // Generator-produced ADD COLUMN migration (history mirror + trigger regen included).
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { Build(withW: false) } },
            new SchemaSnapshot { Tables = { Build(withW: true) } });
        var sql = generator!.GenerateSql(diff);
        foreach (var statement in (sql.PreTransactionUp ?? Enumerable.Empty<string>()).Concat(sql.Up).Concat(sql.PostTransactionUp ?? Enumerable.Empty<string>()))
        {
            using var cn = factory!();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = statement;
            cmd.ExecuteNonQuery();
        }

        try
        {
            // Versioning continues IMMEDIATELY: the very next raw write is versioned and the
            // new column's value reaches history.
            using (var cn = factory!())
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = "UPDATE TmigLiveB SET V = 3, W = 42 WHERE Id = 1";
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }
            Assert.Equal(1, Scalar(factory!, "SELECT COUNT(*) FROM TmigLiveB_History WHERE V = 3 AND W = 42"));
            Assert.True(Scalar(factory!, "SELECT COUNT(*) FROM TmigLiveB_History") >= 3);

            // AsOf spans the migration through the post-migration model.
            var opts2 = new DbContextOptions { OnModelCreating = mb => mb.Entity<RowV2>() };
            opts2.EnableTemporalVersioning();
            await using var ctx2 = new DbContext(factory!(), provider!, opts2);
            var old = await ctx2.Query<RowV2>().AsOf(betweenV1V2).Where(r => r.Id == 1).ToListAsync();
            var v1 = Assert.Single(old);
            Assert.Equal(1, v1.V);
            Assert.Null(v1.W);
        }
        finally
        {
            ExecIgnore(factory!,
                "DROP TABLE TmigLiveB_History", "DROP TABLE TmigLiveB",
                "DROP FUNCTION IF EXISTS \"TmigLiveB_TemporalFunction\"()");
        }
    }
}
