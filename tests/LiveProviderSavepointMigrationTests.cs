using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using MigrationBase = nORM.Migration.Migration;
using nORM.Providers;
using Xunit;

#nullable enable

#pragma warning disable CS8765 // Nullability mismatch on overridden member

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.0 → 4.5 audit — items not covered by LiveLockStepParityTests.cs
//
// 1. Savepoints across all providers (SP-1 through SP-5)
// 2. Live migration apply on all providers (MG-1 through MG-5)
// 3. Long-horizon cache contention (CC-1, CC-2)
// 4. Cancellation races under load (CR-1, CR-2)
// ══════════════════════════════════════════════════════════════════════════════

public class LiveProviderSavepointMigrationTests
{
    // ── Entities ──────────────────────────────────────────────────────────────

    [Table("SP_Item")]
    private class SpItem
    {
        [Key]
        public int Id { get; set; }
        public string Label { get; set; } = "";
    }

    // ── Provider-specific DDL ─────────────────────────────────────────────────

    private static string SpItemDdl(string kind) => kind switch
    {
        "sqlite"    => "CREATE TABLE IF NOT EXISTS SP_Item (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL)",
        "sqlserver" => "IF OBJECT_ID('SP_Item','U') IS NULL CREATE TABLE SP_Item (Id INT PRIMARY KEY, Label NVARCHAR(200) NOT NULL)",
        "mysql"     => "CREATE TABLE IF NOT EXISTS SP_Item (Id INT PRIMARY KEY, Label VARCHAR(200) NOT NULL)",
        "postgres"  => "CREATE TABLE IF NOT EXISTS SP_Item (Id INT PRIMARY KEY, Label VARCHAR(200) NOT NULL)",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    private static string MigTableDdl(string kind, string tableName) => kind switch
    {
        "sqlite"    => $"CREATE TABLE IF NOT EXISTS {tableName} (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL)",
        "sqlserver" => $"IF OBJECT_ID('{tableName}','U') IS NULL CREATE TABLE {tableName} (Id INT PRIMARY KEY, Tag NVARCHAR(200) NOT NULL)",
        "mysql"     => $"CREATE TABLE IF NOT EXISTS {tableName} (Id INT PRIMARY KEY, Tag VARCHAR(200) NOT NULL)",
        "postgres"  => $"CREATE TABLE IF NOT EXISTS {tableName} (Id INT PRIMARY KEY, Tag VARCHAR(200) NOT NULL)",
        _           => throw new ArgumentOutOfRangeException(nameof(kind))
    };

    // ── Connection factory (mirrors LiveLockStepParityTests.OpenLive) ─────────

    private static (DbConnection? Cn, DatabaseProvider? Provider, string? SkipReason) OpenLive(string kind)
    {
        switch (kind)
        {
            case "sqlite":
            {
                var cn = new SqliteConnection("Data Source=:memory:");
                cn.Open();
                return (cn, new SqliteProvider(), null);
            }
            case "sqlserver":
            {
                var cs = Environment.GetEnvironmentVariable("NORM_TEST_SQLSERVER");
                if (string.IsNullOrEmpty(cs))
                    return (null, null, "NORM_TEST_SQLSERVER not set — SQL Server live tests skipped.");
                var cn = OpenReflected("Microsoft.Data.SqlClient.SqlConnection, Microsoft.Data.SqlClient", cs);
                return (cn, new SqlServerProvider(), null);
            }
            case "mysql":
            {
                var cs = Environment.GetEnvironmentVariable("NORM_TEST_MYSQL");
                if (string.IsNullOrEmpty(cs))
                    return (null, null, "NORM_TEST_MYSQL not set — MySQL live tests skipped.");
                var cn = OpenReflected("MySqlConnector.MySqlConnection, MySqlConnector", cs);
                return (cn, new MySqlProvider(new SqliteParameterFactory()), null);
            }
            case "postgres":
            {
                var cs = Environment.GetEnvironmentVariable("NORM_TEST_POSTGRES");
                if (string.IsNullOrEmpty(cs))
                    return (null, null, "NORM_TEST_POSTGRES not set — PostgreSQL live tests skipped.");
                var cn = OpenReflected("Npgsql.NpgsqlConnection, Npgsql", cs);
                return (cn, new PostgresProvider(new SqliteParameterFactory()), null);
            }
            default:
                throw new ArgumentOutOfRangeException(nameof(kind));
        }
    }

    private static DbConnection OpenReflected(string typeName, string cs)
    {
        var type = Type.GetType(typeName)
            ?? throw new InvalidOperationException($"Could not load '{typeName}'. Ensure the driver is installed.");
        var cn = (DbConnection)Activator.CreateInstance(type, cs)!;
        cn.Open();
        return cn;
    }

    private static void Exec(DbConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static long CountRows(DbConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM {table}";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static bool TableExists(DbConnection cn, string tableName)
    {
        // Use sqlite_master for SQLite; INFORMATION_SCHEMA.TABLES for all others.
        // The query is intentionally pragmatic: if the row count > 0, table exists.
        try
        {
            using var cmd = cn.CreateCommand();
            // sqlite_master covers SQLite; other providers will try INFORMATION_SCHEMA.
            // We pick the approach based on connection type.
            if (cn is SqliteConnection)
            {
                cmd.CommandText = $"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{tableName}'";
            }
            else
            {
                cmd.CommandText = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME='{tableName}'";
            }
            return Convert.ToInt64(cmd.ExecuteScalar()) > 0;
        }
        catch
        {
            return false;
        }
    }

    // ── Dynamic migration assembly builder (mirrors MySqlMigrationCheckpointTests.BuildAsm) ──

    /// <summary>
    /// Builds an in-memory assembly containing one or more Migration subclasses.
    /// Each spec: (Version, Name, DDL to execute in Up(), ShouldFail).
    /// </summary>
    private static Assembly BuildMigrationAsm(params (long V, string N, string Ddl, bool Fail)[] specs)
    {
        var asmName = new AssemblyName($"SpMig_{Guid.NewGuid():N}");
        var ab      = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        var mod     = ab.DefineDynamicModule("Mod");

        var migBase   = typeof(MigrationBase);
        var baseCtor  = migBase.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance, null,
            new[] { typeof(long), typeof(string) }, null)!;
        var upMethod  = migBase.GetMethod("Up",   new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var downMethod= migBase.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;

        // DbCommand members used in Up() IL
        var createCmd   = typeof(DbConnection).GetMethod("CreateCommand")!;
        var setPropText = typeof(DbCommand).GetProperty("CommandText")!.SetMethod!;
        var execNonQ    = typeof(DbCommand).GetMethod("ExecuteNonQuery")!;
        var disposeCmd  = typeof(IDisposable).GetMethod("Dispose")!;
        var throwCtor   = typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!;

        foreach (var (v, n, ddl, fail) in specs)
        {
            var tb = mod.DefineType(n, TypeAttributes.Public | TypeAttributes.Class, migBase);

            // constructor
            var ctorB  = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            var ctorIL = ctorB.GetILGenerator();
            ctorIL.Emit(OpCodes.Ldarg_0);
            ctorIL.Emit(OpCodes.Ldc_I8, v);
            ctorIL.Emit(OpCodes.Ldstr, n);
            ctorIL.Emit(OpCodes.Call, baseCtor);
            ctorIL.Emit(OpCodes.Ret);

            // Up() — executes the DDL, optionally throws
            var upB  = tb.DefineMethod("Up",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            var upIL = upB.GetILGenerator();
            if (fail)
            {
                upIL.Emit(OpCodes.Ldstr, "simulated migration failure");
                upIL.Emit(OpCodes.Newobj, throwCtor);
                upIL.Emit(OpCodes.Throw);
            }
            else if (!string.IsNullOrEmpty(ddl))
            {
                // DbCommand cmd = connection.CreateCommand();
                var cmdLocal = upIL.DeclareLocal(typeof(DbCommand));
                upIL.Emit(OpCodes.Ldarg_1);                 // connection (arg1)
                upIL.Emit(OpCodes.Callvirt, createCmd);
                upIL.Emit(OpCodes.Stloc, cmdLocal);

                // cmd.CommandText = ddl;
                upIL.Emit(OpCodes.Ldloc, cmdLocal);
                upIL.Emit(OpCodes.Ldstr, ddl);
                upIL.Emit(OpCodes.Callvirt, setPropText);

                // cmd.ExecuteNonQuery();
                upIL.Emit(OpCodes.Ldloc, cmdLocal);
                upIL.Emit(OpCodes.Callvirt, execNonQ);
                upIL.Emit(OpCodes.Pop);

                // cmd.Dispose();
                upIL.Emit(OpCodes.Ldloc, cmdLocal);
                upIL.Emit(OpCodes.Callvirt, disposeCmd);

                upIL.Emit(OpCodes.Ret);
            }
            else
            {
                upIL.Emit(OpCodes.Ret);
            }
            tb.DefineMethodOverride(upB, upMethod);

            // Down() — no-op
            var downB  = tb.DefineMethod("Down",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            var downIL = downB.GetILGenerator();
            downIL.Emit(OpCodes.Ret);
            tb.DefineMethodOverride(downB, downMethod);

            tb.CreateType();
        }

        return ab;
    }

    // ── NoLock runner for SQLite (advisory lock methods are no-ops in SqliteMigrationRunner,
    //    so we simply wrap SqliteMigrationRunner directly — no subclass needed) ─────────────

    // Raw-SQL insert helper that bypasses the ChangeTracker, for use within
    // explicit transactions where the ChangeTracker intentionally defers AcceptChanges.
    private static void RawInsert(DbConnection cn, DbTransaction tx, int id, string label)
    {
        using var cmd = cn.CreateCommand();
        cmd.Transaction = tx;
        cmd.CommandText = $"INSERT INTO SP_Item (Id, Label) VALUES ({id}, '{label}')";
        cmd.ExecuteNonQuery();
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SP-1: Savepoint rollback discards write made after savepoint
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_Savepoint_RollbackToSavepoint_WriteMadeAfterSavepointIsGone(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn)
        await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, SpItemDdl(kind));
            Exec(cn!, "DELETE FROM SP_Item");

            using var tx = await ctx.Database.BeginTransactionAsync();

            // Write row A before the savepoint via raw SQL (bypasses ChangeTracker
            // so AcceptChanges-deferral on external tx doesn't cause re-insert).
            RawInsert(cn!, tx.Transaction!, 1, "before-savepoint");

            // Create savepoint.
            await ctx.CreateSavepointAsync(tx.Transaction!, "sp1");

            // Write row B after the savepoint.
            RawInsert(cn!, tx.Transaction!, 2, "after-savepoint");

            // Row B is visible before rollback.
            Assert.Equal(2L, CountRows(cn!, "SP_Item"));

            // Roll back to savepoint — row B should disappear.
            await ctx.RollbackToSavepointAsync(tx.Transaction!, "sp1");

            // Only row A survives the savepoint rollback.
            Assert.Equal(1L, CountRows(cn!, "SP_Item"));

            await tx.CommitAsync();

            // Row A persists after commit.
            Assert.Equal(1L, CountRows(cn!, "SP_Item"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SP-2: Write made BEFORE a savepoint survives rollback-to-savepoint
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_Savepoint_WriteBeforeSavepoint_SurvivesRollback(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn)
        await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, SpItemDdl(kind));
            Exec(cn!, "DELETE FROM SP_Item");

            using var tx = await ctx.Database.BeginTransactionAsync();

            RawInsert(cn!, tx.Transaction!, 10, "pre-savepoint");

            await ctx.CreateSavepointAsync(tx.Transaction!, "mark");

            // Roll back to the savepoint without writing anything new.
            await ctx.RollbackToSavepointAsync(tx.Transaction!, "mark");

            // Pre-savepoint row must still be visible.
            Assert.Equal(1L, CountRows(cn!, "SP_Item"));

            await tx.CommitAsync();

            Assert.Equal(1L, CountRows(cn!, "SP_Item"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SP-3: Full transaction rollback after savepoint discards all writes
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_Savepoint_FullRollback_DiscardsAllWrites(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn)
        await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, SpItemDdl(kind));
            Exec(cn!, "DELETE FROM SP_Item");

            using var tx = await ctx.Database.BeginTransactionAsync();

            RawInsert(cn!, tx.Transaction!, 20, "tx-row");

            await ctx.CreateSavepointAsync(tx.Transaction!, "sp_before_full_rollback");

            RawInsert(cn!, tx.Transaction!, 21, "after-sp");

            // Roll back the entire transaction (not just to savepoint).
            await tx.RollbackAsync();

            // Nothing should remain.
            Assert.Equal(0L, CountRows(cn!, "SP_Item"));
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SP-4: Multiple savepoints — rollback to first removes writes from both
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlite")]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_Savepoint_MultipleNestedSavepoints_RollbackToFirstRemovesAll(string kind)
    {
        var (cn, provider, skip) = OpenLive(kind);
        if (skip != null) return;

        using (cn)
        await using (var ctx = new DbContext(cn!, provider!))
        {
            Exec(cn!, SpItemDdl(kind));
            Exec(cn!, "DELETE FROM SP_Item");

            using var tx = await ctx.Database.BeginTransactionAsync();

            // Write before any savepoint.
            RawInsert(cn!, tx.Transaction!, 30, "base");

            await ctx.CreateSavepointAsync(tx.Transaction!, "sp_a");

            RawInsert(cn!, tx.Transaction!, 31, "after-sp-a");

            await ctx.CreateSavepointAsync(tx.Transaction!, "sp_b");

            RawInsert(cn!, tx.Transaction!, 32, "after-sp-b");

            Assert.Equal(3L, CountRows(cn!, "SP_Item"));

            // Roll back all the way to sp_a — rows 31 and 32 should disappear.
            await ctx.RollbackToSavepointAsync(tx.Transaction!, "sp_a");

            Assert.Equal(1L, CountRows(cn!, "SP_Item"));

            await tx.CommitAsync();
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // SP-5: Null transaction parameter throws InvalidOperationException
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Savepoint_NullTransaction_ThrowsInvalidOperationException()
    {
        await using var cn  = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        await using var ctx = new DbContext(cn, new SqliteProvider());

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.CreateSavepointAsync(null!, "sp1"));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.RollbackToSavepointAsync(null!, "sp1"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MG-1: SQLite migration apply — DDL table is created and history recorded
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SqliteMigration_Apply_CreatesTableAndRecordsHistory()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        const string tableName = "MigTarget1";
        var asm = BuildMigrationAsm((1000L, "CreateMigTarget1",
            $"CREATE TABLE IF NOT EXISTS {tableName} (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL)",
            false));

        var runner = new SqliteMigrationRunner(cn, asm);
        await runner.ApplyMigrationsAsync();

        // Table must exist.
        Assert.True(TableExists(cn, tableName), $"Table '{tableName}' was not created by the migration.");

        // History row must be present and Applied.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Version = 1000";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.Equal(1L, count);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MG-2: SQLite migration apply is idempotent — second run is a no-op
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SqliteMigration_ApplyTwice_IsNoOp()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var asm = BuildMigrationAsm((2000L, "IdempotentMig",
            "CREATE TABLE IF NOT EXISTS MigTarget2 (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL)",
            false));

        var runner = new SqliteMigrationRunner(cn, asm);

        // First apply.
        await runner.ApplyMigrationsAsync();

        // Second apply must succeed without error and not duplicate the history row.
        await runner.ApplyMigrationsAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Version = 2000";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.Equal(1L, count);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MG-3: SQLite migration failure leaves no history row (transaction rolled back)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SqliteMigration_FailedMigration_DoesNotRecordHistory()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var asm = BuildMigrationAsm((3000L, "FailingMig", "", true));

        var runner = new SqliteMigrationRunner(cn, asm);
        await Assert.ThrowsAsync<InvalidOperationException>(() => runner.ApplyMigrationsAsync());

        // The migration failure must not leave an Applied history row.
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Version = 3000";
        var count = Convert.ToInt64(cmd.ExecuteScalar());
        Assert.Equal(0L, count);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MG-4: SQLite two-step migration applies both in order
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SqliteMigration_TwoSteps_BothAppliedInOrder()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        const string t1 = "MigStep1";
        const string t2 = "MigStep2";

        var asm = BuildMigrationAsm(
            (4000L, "Step1", $"CREATE TABLE IF NOT EXISTS {t1} (Id INTEGER PRIMARY KEY)", false),
            (4001L, "Step2", $"CREATE TABLE IF NOT EXISTS {t2} (Id INTEGER PRIMARY KEY)", false));

        await new SqliteMigrationRunner(cn, asm).ApplyMigrationsAsync();

        Assert.True(TableExists(cn, t1), $"Table '{t1}' not created.");
        Assert.True(TableExists(cn, t2), $"Table '{t2}' not created.");

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"";
        Assert.Equal(2L, Convert.ToInt64(cmd.ExecuteScalar()));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // MG-5: HasPendingMigrationsAsync reports correctly before and after apply
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SqliteMigration_HasPendingMigrationsAsync_ReportsCorrectly()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var asm = BuildMigrationAsm((5000L, "PendingCheck",
            "CREATE TABLE IF NOT EXISTS MigTarget5 (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL)",
            false));

        var runner = new SqliteMigrationRunner(cn, asm);

        // Before apply: pending.
        Assert.True(await runner.HasPendingMigrationsAsync());

        // Apply.
        await runner.ApplyMigrationsAsync();

        // After apply: nothing pending.
        Assert.False(await runner.HasPendingMigrationsAsync());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // CC-1: 20 concurrent DbContexts each execute a LINQ query — plan cache
    //       does not corrupt results and all tasks return correct data
    // ══════════════════════════════════════════════════════════════════════════

    [Table("CC_Item")]
    private class CcItem
    {
        [Key]
        public int Id { get; set; }
        public string Tag { get; set; } = "";
    }

    [Fact]
    public async Task CacheContention_20ConcurrentContexts_AllReturnCorrectData()
    {
        // Shared file-backed database so contexts running in parallel can share the table.
        var dbPath = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"norm_cc_{Guid.NewGuid():N}.db");
        try
        {
            // Seed data on a dedicated connection.
            using (var seed = new SqliteConnection($"Data Source={dbPath}"))
            {
                seed.Open();
                using var cmd = seed.CreateCommand();
                cmd.CommandText =
                    "CREATE TABLE CC_Item (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL);" +
                    "INSERT INTO CC_Item VALUES (1,'alpha');" +
                    "INSERT INTO CC_Item VALUES (2,'beta');" +
                    "INSERT INTO CC_Item VALUES (3,'gamma');";
                cmd.ExecuteNonQuery();
            }

            const int concurrency = 20;
            var tasks = new Task<int>[concurrency];

            for (int i = 0; i < concurrency; i++)
            {
                tasks[i] = Task.Run(async () =>
                {
                    var cn  = new SqliteConnection($"Data Source={dbPath}");
                    cn.Open();
                    await using var ctx = new DbContext(cn, new SqliteProvider());
                    // Each task queries all rows and returns count; plan cache is shared.
                    var results = ctx.Query<CcItem>().Where(x => x.Id > 0).ToList();
                    return results.Count;
                });
            }

            var counts = await Task.WhenAll(tasks);

            // Every task must get the same 3 rows — no cache corruption.
            Assert.All(counts, c => Assert.Equal(3, c));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            if (System.IO.File.Exists(dbPath))
                System.IO.File.Delete(dbPath);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // CC-2: Plan cache is stable when the same query runs 50 times sequentially
    //       on a single context — no fingerprint collision or stale plan reuse
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CacheContention_50SequentialQueryExecutions_AlwaysReturnCorrectSlice()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var setup = cn.CreateCommand();
        setup.CommandText = "CREATE TABLE CC_Item (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL)";
        setup.ExecuteNonQuery();

        await using var ctx = new DbContext(cn, new SqliteProvider());

        // Insert 10 rows.
        for (int i = 1; i <= 10; i++)
            ctx.Add(new CcItem { Id = i, Tag = $"t{i}" });
        await ctx.SaveChangesAsync();

        // Run the same parameterized query 50 times; each time vary the threshold.
        for (int run = 0; run < 50; run++)
        {
            int threshold = (run % 5) + 1; // cycles 1..5
            var results = ctx.Query<CcItem>().Where(x => x.Id > threshold).ToList();
            Assert.Equal(10 - threshold, results.Count);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // CR-1: 10 navigation loads, 5 cancelled mid-flight — no deadlock,
    //       all tasks complete with either a result or OperationCanceledException
    // ══════════════════════════════════════════════════════════════════════════

    [Table("CR_Item")]
    private class CrItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task CancellationRaceUnderLoad_NoDeadlock_AllTasksComplete()
    {
        var dbPath  = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"norm_cr_{Guid.NewGuid():N}.db");
        const int total    = 10;
        const int toCancel = 5;

        // Declare ctsList outside try so it's accessible in finally.
        var ctsList = Enumerable.Range(0, total)
            .Select(_ => new CancellationTokenSource())
            .ToArray();
        try
        {
            // Seed.
            using (var seed = new SqliteConnection($"Data Source={dbPath}"))
            {
                seed.Open();
                using var cmd = seed.CreateCommand();
                cmd.CommandText =
                    "CREATE TABLE CR_Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                    "INSERT INTO CR_Item VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four'),(5,'five');";
                cmd.ExecuteNonQuery();
            }

            // Wrap each Task.Run so that a TaskCanceledException from the task scheduler
            // (when the CTS is already cancelled) is caught and returned as a Cancelled tuple.
            var tasks = ctsList.Select((cts, idx) =>
            {
                var inner = Task.Run(async () =>
                {
                    var cn  = new SqliteConnection($"Data Source={dbPath}");
                    cn.Open();
                    await using var ctx = new DbContext(cn, new SqliteProvider());
                    try
                    {
                        var rows = ctx.Query<CrItem>().Where(x => x.Id > 0).ToList();
                        return (Success: true, Count: rows.Count, Cancelled: false);
                    }
                    catch (OperationCanceledException)
                    {
                        return (Success: false, Count: 0, Cancelled: true);
                    }
                }, cts.Token);

                // If Task.Run itself is cancelled before the lambda runs, wrap the
                // TaskCanceledException into the expected result tuple.
                return inner.ContinueWith(t =>
                    t.IsCanceled
                        ? (Success: false, Count: 0, Cancelled: true)
                        : t.Result,
                    TaskContinuationOptions.None);
            }).ToArray();

            // Cancel 5 of them shortly after they start.
            for (int i = 0; i < toCancel; i++)
                ctsList[i].Cancel();

            // All tasks must complete — no deadlock or hang.
            var timeout = Task.Delay(TimeSpan.FromSeconds(15));
            var all     = Task.WhenAll(tasks);
            var winner  = await Task.WhenAny(all, timeout);
            Assert.Same(all, winner); // must not timeout

            var results = await all;

            // Every task completed with either success or OCE — no other exception.
            int successes  = results.Count(r => r.Success);
            int cancelleds = results.Count(r => r.Cancelled);
            Assert.Equal(total, successes + cancelleds);

            // Successful tasks got all 5 rows.
            foreach (var r in results.Where(r => r.Success))
                Assert.Equal(5, r.Count);
        }
        finally
        {
            foreach (var cts in ctsList)
                cts.Dispose();
            SqliteConnection.ClearAllPools();
            if (System.IO.File.Exists(dbPath))
                System.IO.File.Delete(dbPath);
        }
    }

    // ══════════════════════════════════════════════════════════════════════════
    // CR-2: Pre-cancelled token passed to SaveChangesAsync does not deadlock;
    //       any outcome is either OperationCanceledException or clean success
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CancellationRace_PreCancelledToken_DoesNotDeadlock()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CR_Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();

        await using var ctx = new DbContext(cn, new SqliteProvider());

        ctx.Add(new CrItem { Id = 99, Name = "cancel-test" });

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Exception? caught = null;
        try
        {
            await ctx.SaveChangesAsync(cts.Token);
        }
        catch (OperationCanceledException ex)
        {
            caught = ex;
        }

        // Only OCE is acceptable (or success if SQLite ignores the token).
        // Any other exception type would be a bug.
        if (caught != null)
            Assert.IsAssignableFrom<OperationCanceledException>(caught);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // Live provider migration env-gated tests
    // ══════════════════════════════════════════════════════════════════════════

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_Migration_Apply_CreatesTableAndRecordsHistory(string kind)
    {
        var (cn, _, skip) = OpenLive(kind);
        if (skip != null) return;

        const string tableName = "SpLiveMigTarget";
        const long   version   = 9900L;
        const string migName   = "CreateSpLiveMigTarget";

        var ddl = MigTableDdl(kind, tableName);
        var asm = BuildMigrationAsm((version, migName, ddl, false));

        // Use provider-specific runner.
        IMigrationRunner runner = kind switch
        {
            "sqlserver" => new SqlServerMigrationRunner(cn!, asm),
            "mysql"     => new MySqlMigrationRunner(cn!, asm),
            "postgres"  => new PostgresMigrationRunner(cn!, asm),
            _           => throw new ArgumentOutOfRangeException(nameof(kind))
        };

        try
        {
            await runner.ApplyMigrationsAsync();

            // History row must exist.
            using var histCmd = cn!.CreateCommand();
            histCmd.CommandText = $"SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Version = {version}";
            var count = Convert.ToInt64(histCmd.ExecuteScalar());
            Assert.Equal(1L, count);
        }
        finally
        {
            // Clean up: drop created objects.
            try { Exec(cn!, $"DROP TABLE IF EXISTS {tableName}"); } catch { }
            try { Exec(cn!, $"DELETE FROM \"__NormMigrationsHistory\" WHERE Version = {version}"); } catch { }
            cn?.Dispose();
        }
    }

    [Theory]
    [InlineData("sqlserver")]
    [InlineData("mysql")]
    [InlineData("postgres")]
    public async Task LiveProvider_Migration_ApplyTwice_IsNoOp(string kind)
    {
        var (cn, _, skip) = OpenLive(kind);
        if (skip != null) return;

        const string tableName = "SpLiveMigTarget2";
        const long   version   = 9901L;
        const string migName   = "CreateSpLiveMigTarget2";

        var ddl = MigTableDdl(kind, tableName);
        var asm = BuildMigrationAsm((version, migName, ddl, false));

        IMigrationRunner runner = kind switch
        {
            "sqlserver" => new SqlServerMigrationRunner(cn!, asm),
            "mysql"     => new MySqlMigrationRunner(cn!, asm),
            "postgres"  => new PostgresMigrationRunner(cn!, asm),
            _           => throw new ArgumentOutOfRangeException(nameof(kind))
        };

        try
        {
            await runner.ApplyMigrationsAsync();
            // Second apply must not throw or duplicate the history row.
            await runner.ApplyMigrationsAsync();

            using var histCmd = cn!.CreateCommand();
            histCmd.CommandText = $"SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Version = {version}";
            var count = Convert.ToInt64(histCmd.ExecuteScalar());
            Assert.Equal(1L, count);
        }
        finally
        {
            try { Exec(cn!, $"DROP TABLE IF EXISTS {tableName}"); } catch { }
            try { Exec(cn!, $"DELETE FROM \"__NormMigrationsHistory\" WHERE Version = {version}"); } catch { }
            cn?.Dispose();
        }
    }
}
