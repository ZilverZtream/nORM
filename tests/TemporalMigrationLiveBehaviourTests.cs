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

    // Provider-agnostic column existence (INFORMATION_SCHEMA is supported by SQL Server, PostgreSQL, MySQL).
    private static long ColumnCount(Func<DbConnection> factory, string table, string column)
        => Scalar(factory, $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='{table}' AND COLUMN_NAME='{column}'");

    // Execute each migration statement as its own command (mirrors how DatabaseFacade applies them).
    private static void ApplyAll(Func<DbConnection> factory, System.Collections.Generic.IEnumerable<string> statements)
    {
        foreach (var statement in statements)
        {
            using var cn = factory();
            using var cmd = cn.CreateCommand();
            cmd.CommandText = statement;
            cmd.ExecuteNonQuery();
        }
    }

    private static TableSchema Build(bool withW)
    {
        var t = new TableSchema { Name = "TmigLiveB", IsTemporal = true };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "V", ClrType = typeof(int).FullName!, IsNullable = false });
        if (withW) t.Columns.Add(new ColumnSchema { Name = "W", ClrType = typeof(int).FullName!, IsNullable = true });
        return t;
    }

    // W as NOT NULL with a DEFAULT — on SQL Server the history-table mirror must emit
    // ADD ... NOT NULL DEFAULT ... WITH VALUES so existing history rows are backfilled.
    private static TableSchema BuildDefaultedW()
    {
        var t = new TableSchema { Name = "TmigLiveB", IsTemporal = true };
        t.Columns.Add(new ColumnSchema { Name = "Id", ClrType = typeof(int).FullName!, IsPrimaryKey = true });
        t.Columns.Add(new ColumnSchema { Name = "V", ClrType = typeof(int).FullName!, IsNullable = false });
        t.Columns.Add(new ColumnSchema { Name = "W", ClrType = typeof(int).FullName!, IsNullable = false, DefaultValue = "7" });
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
        // Postgres folds unquoted identifiers to lowercase, and nORM quotes the mapped name,
        // so the raw DDL must quote there; MySQL/SQL Server accept the plain form.
        var tbl = kind == "postgres" ? "\"TmigLiveB\"" : "TmigLiveB";
        var hist = kind == "postgres" ? "\"TmigLiveB_History\"" : "TmigLiveB_History";
        ExecIgnore(factory!,
            $"DROP TABLE {hist}", $"DROP TABLE {tbl}",
            "DROP FUNCTION IF EXISTS \"TmigLiveB_TemporalFunction\"()");
        ExecIgnore(factory!, kind == "postgres"
            ? "CREATE TABLE \"TmigLiveB\" (\"Id\" INT PRIMARY KEY, \"V\" INT NOT NULL)"
            : $"CREATE TABLE {tbl} (Id INT PRIMARY KEY, V INT NOT NULL)");

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
                cmd.CommandText = kind == "postgres" ? "UPDATE \"TmigLiveB\" SET \"V\" = 3, \"W\" = 42 WHERE \"Id\" = 1" : "UPDATE TmigLiveB SET V = 3, W = 42 WHERE Id = 1";
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }
            Assert.Equal(1, Scalar(factory!, kind == "postgres" ? "SELECT COUNT(*) FROM \"TmigLiveB_History\" WHERE \"V\" = 3 AND \"W\" = 42" : "SELECT COUNT(*) FROM TmigLiveB_History WHERE V = 3 AND W = 42"));
            Assert.True(Scalar(factory!, kind == "postgres" ? "SELECT COUNT(*) FROM \"TmigLiveB_History\"" : "SELECT COUNT(*) FROM TmigLiveB_History") >= 3);

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

    /// <summary>
    /// Adding a NOT NULL column WITH a default to a temporal table must backfill the pre-existing
    /// history rows. On SQL Server the history mirror emits <c>ADD ... NOT NULL DEFAULT (...) WITH
    /// VALUES</c> — a clause only produced for a non-nullable defaulted column, so the sibling
    /// (nullable) test never exercises it; PostgreSQL/MySQL backfill via plain <c>ADD ... DEFAULT</c>.
    /// This is a live-only guard: a malformed history ADD only fails against a real server.
    /// </summary>
    [Theory]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    [InlineData("mysql")]
    public async Task Add_notnull_defaulted_column_backfills_history_and_keeps_versioning(string kind)
    {
        var (factory, provider, generator, skip) = OpenLive(kind);
        if (skip != null) return;
        using (var probe = factory!())
        {
            var db = probe.Database;
            if (db is "master" or "postgres" or "mysql" or "sys" or "model" or "msdb" or "tempdb")
                return;
        }

        var tbl  = kind == "postgres" ? "\"TmigLiveB\"" : "TmigLiveB";
        var hist = kind == "postgres" ? "\"TmigLiveB_History\"" : "TmigLiveB_History";
        var histCount = kind == "postgres"
            ? "SELECT COUNT(*) FROM \"TmigLiveB_History\""
            : "SELECT COUNT(*) FROM TmigLiveB_History";
        var histWith7 = kind == "postgres"
            ? "SELECT COUNT(*) FROM \"TmigLiveB_History\" WHERE \"W\" = 7"
            : "SELECT COUNT(*) FROM TmigLiveB_History WHERE W = 7";

        ExecIgnore(factory!,
            $"DROP TABLE {hist}", $"DROP TABLE {tbl}",
            "DROP FUNCTION IF EXISTS \"TmigLiveB_TemporalFunction\"()");
        ExecIgnore(factory!, kind == "postgres"
            ? "CREATE TABLE \"TmigLiveB\" (\"Id\" INT PRIMARY KEY, \"V\" INT NOT NULL)"
            : $"CREATE TABLE {tbl} (Id INT PRIMARY KEY, V INT NOT NULL)");

        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<RowV1>() };
        opts.EnableTemporalVersioning();
        await using (var ctx = new DbContext(factory!(), provider!, opts))
        {
            ctx.Add(new RowV1 { Id = 1, V = 1 });
            await ctx.SaveChangesAsync();
            await Task.Delay(200);
            var row = await ctx.Query<RowV1>().Where(r => r.Id == 1).FirstAsync();
            row.V = 2;
            await ctx.SaveChangesAsync();   // snapshots V=1 into history
        }
        var preHist = Scalar(factory!, histCount);
        Assert.True(preHist >= 1, $"[{kind}] expected at least one pre-migration history row.");

        // ADD COLUMN W INT NOT NULL DEFAULT 7 on a temporal table: main + history mirror + trigger regen.
        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { Build(withW: false) } },
            new SchemaSnapshot { Tables = { BuildDefaultedW() } });
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
            // The pre-existing history rows must be backfilled with the new column's default (7) —
            // WITH VALUES on SQL Server, ADD ... DEFAULT on PostgreSQL/MySQL.
            Assert.True(Scalar(factory!, histWith7) >= preHist,
                $"[{kind}] pre-migration history rows must be backfilled with the new column default (7).");

            // Versioning still works: a further update snapshots another history row.
            using (var cn = factory!())
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = kind == "postgres"
                    ? "UPDATE \"TmigLiveB\" SET \"V\" = 3 WHERE \"Id\" = 1"
                    : "UPDATE TmigLiveB SET V = 3 WHERE Id = 1";
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }
            Assert.True(Scalar(factory!, histCount) > preHist,
                $"[{kind}] versioning must continue after the NOT NULL defaulted column was added.");
        }
        finally
        {
            ExecIgnore(factory!,
                "DROP TABLE TmigLiveB_History", "DROP TABLE TmigLiveB",
                "DROP FUNCTION IF EXISTS \"TmigLiveB_TemporalFunction\"()");
        }
    }

    /// <summary>
    /// The DOWN of a temporal ADD-COLUMN migration must remove the column from BOTH the main table and
    /// its history mirror and regenerate the trigger for the reverted shape — so versioning keeps working
    /// afterward. Live-only: <c>TemporalMigrationLiveBehaviourTests</c> otherwise applies only the Up path,
    /// leaving the generator's Down statements (history DROP COLUMN via the dynamic default-drop helper +
    /// trigger regen) unexecuted against a real server.
    /// </summary>
    [Theory]
    [InlineData("sqlserver")]
    [InlineData("postgres")]
    [InlineData("mysql")]
    public async Task Add_column_migration_down_removes_column_from_main_and_history(string kind)
    {
        var (factory, provider, generator, skip) = OpenLive(kind);
        if (skip != null) return;
        using (var probe = factory!())
        {
            var db = probe.Database;
            if (db is "master" or "postgres" or "mysql" or "sys" or "model" or "msdb" or "tempdb")
                return;
        }

        var tbl  = kind == "postgres" ? "\"TmigLiveB\"" : "TmigLiveB";
        var hist = kind == "postgres" ? "\"TmigLiveB_History\"" : "TmigLiveB_History";
        var histCount = kind == "postgres"
            ? "SELECT COUNT(*) FROM \"TmigLiveB_History\""
            : "SELECT COUNT(*) FROM TmigLiveB_History";

        ExecIgnore(factory!,
            $"DROP TABLE {hist}", $"DROP TABLE {tbl}",
            "DROP FUNCTION IF EXISTS \"TmigLiveB_TemporalFunction\"()");
        ExecIgnore(factory!, kind == "postgres"
            ? "CREATE TABLE \"TmigLiveB\" (\"Id\" INT PRIMARY KEY, \"V\" INT NOT NULL)"
            : $"CREATE TABLE {tbl} (Id INT PRIMARY KEY, V INT NOT NULL)");

        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<RowV1>() };
        opts.EnableTemporalVersioning();
        await using (var ctx = new DbContext(factory!(), provider!, opts))
        {
            ctx.Add(new RowV1 { Id = 1, V = 1 });
            await ctx.SaveChangesAsync();
        }

        var diff = SchemaDiffer.Diff(
            new SchemaSnapshot { Tables = { Build(withW: false) } },
            new SchemaSnapshot { Tables = { Build(withW: true) } });
        var sql = generator!.GenerateSql(diff);

        // Up: add W to main + history.
        ApplyAll(factory!, (sql.PreTransactionUp ?? Enumerable.Empty<string>()).Concat(sql.Up).Concat(sql.PostTransactionUp ?? Enumerable.Empty<string>()));
        Assert.Equal(1, ColumnCount(factory!, "TmigLiveB", "W"));
        Assert.Equal(1, ColumnCount(factory!, "TmigLiveB_History", "W"));

        try
        {
            // Down: remove W from main + history and regen the trigger for the (Id, V) shape.
            ApplyAll(factory!, (sql.PreTransactionDown ?? Enumerable.Empty<string>()).Concat(sql.Down).Concat(sql.PostTransactionDown ?? Enumerable.Empty<string>()));

            Assert.Equal(0, ColumnCount(factory!, "TmigLiveB", "W"));
            Assert.Equal(0, ColumnCount(factory!, "TmigLiveB_History", "W"));

            // Versioning still works on the reverted shape: a further update snapshots to history.
            var before = Scalar(factory!, histCount);
            using (var cn = factory!())
            using (var cmd = cn.CreateCommand())
            {
                cmd.CommandText = kind == "postgres"
                    ? "UPDATE \"TmigLiveB\" SET \"V\" = 5 WHERE \"Id\" = 1"
                    : "UPDATE TmigLiveB SET V = 5 WHERE Id = 1";
                Assert.Equal(1, cmd.ExecuteNonQuery());
            }
            Assert.True(Scalar(factory!, histCount) > before,
                $"[{kind}] versioning must continue after the column was dropped by the Down migration.");
        }
        finally
        {
            ExecIgnore(factory!,
                "DROP TABLE TmigLiveB_History", "DROP TABLE TmigLiveB",
                "DROP FUNCTION IF EXISTS \"TmigLiveB_TemporalFunction\"()");
        }
    }
}
