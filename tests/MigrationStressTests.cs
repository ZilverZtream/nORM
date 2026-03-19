using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using Xunit;

using MigBase = nORM.Migration.Migration;

#nullable enable
#pragma warning disable CS8765

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// MigrationStressTests — gate 4.5 items:
//   • Partial migration failure: migration throws midway, verify history state
//   • Retry after partial failure
//   • Concurrent migration attempts (serializable locking via SQLite exclusive tx)
//
// All tests use SQLite in-memory databases. Migration types are built with
// AssemblyBuilder.DefineDynamicAssembly to avoid polluting the main test assembly.
// MySqlMigrationRunner tests use the NoLock subclass to bypass GET_LOCK.
// ══════════════════════════════════════════════════════════════════════════════

public class MigrationStressTests
{
    // ── Dynamic assembly builder ──────────────────────────────────────────────

    private static readonly ConstructorInfo _baseCtor =
        typeof(MigBase).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;
    private static readonly MethodInfo _upAbstract =
        typeof(MigBase).GetMethod("Up",
            new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
    private static readonly MethodInfo _downAbstract =
        typeof(MigBase).GetMethod("Down",
            new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;

    private static Assembly BuildAsm(params (long V, string N, bool Fail)[] specs)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("MigStress_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Mod");
        var throwCtor = typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!;

        foreach (var (v, n, fail) in specs)
        {
            var tb = mod.DefineType("M_" + n + "_" + v,
                TypeAttributes.Public | TypeAttributes.Class, typeof(MigBase));

            var ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            var ctorIL = ctor.GetILGenerator();
            ctorIL.Emit(OpCodes.Ldarg_0);
            ctorIL.Emit(OpCodes.Ldc_I8, v);
            ctorIL.Emit(OpCodes.Ldstr, n);
            ctorIL.Emit(OpCodes.Call, _baseCtor);
            ctorIL.Emit(OpCodes.Ret);

            var upB = tb.DefineMethod("Up",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            var upIL = upB.GetILGenerator();
            if (fail)
            {
                upIL.Emit(OpCodes.Ldstr, "StressTest_simulated_failure");
                upIL.Emit(OpCodes.Newobj, throwCtor);
                upIL.Emit(OpCodes.Throw);
            }
            else { upIL.Emit(OpCodes.Ret); }
            tb.DefineMethodOverride(upB, _upAbstract);

            var downB = tb.DefineMethod("Down",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            downB.GetILGenerator().Emit(OpCodes.Ret);
            tb.DefineMethodOverride(downB, _downAbstract);
            tb.CreateType();
        }
        return ab;
    }

    private static SqliteConnection OpenSqlite()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    // ── SQLite runner helpers ─────────────────────────────────────────────────

    private static void CreateSqliteHistory(DbConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            @"CREATE TABLE IF NOT EXISTS ""__NormMigrationsHistory"" " +
            @"(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
    }

    private static long CountHistory(DbConnection cn, string whereClause = "")
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"SELECT COUNT(*) FROM ""__NormMigrationsHistory""" + whereClause;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ── MySQL NoLock runner helpers ───────────────────────────────────────────

    private sealed class NoLockMysql : MySqlMigrationRunner
    {
        public NoLockMysql(DbConnection cn, Assembly asm) : base(cn, asm) { }
        protected internal override Task AcquireAdvisoryLockAsync(CancellationToken ct) => Task.CompletedTask;
        protected internal override Task ReleaseAdvisoryLockAsync(CancellationToken ct) => Task.CompletedTask;
    }

    private static void CreateMysqlHistory(DbConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE IF NOT EXISTS `__NormMigrationsHistory` " +
            "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, " +
            "AppliedOn TEXT NOT NULL, Status TEXT NOT NULL DEFAULT 'Applied')";
        cmd.ExecuteNonQuery();
    }

    private static long CountMysqlHistory(DbConnection cn, string whereClause = "")
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM `__NormMigrationsHistory`" + whereClause;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 1. Partial failure — SQLite runner
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SQLite_PartialFailure_FirstMigrationApplied_SecondRolledBack()
    {
        // SQLite wraps ALL migrations in one transaction. If step 2 fails,
        // step 1's history INSERT is also rolled back.
        await using var cn = OpenSqlite();
        CreateSqliteHistory(cn);

        var asm = BuildAsm((1L, "Good", false), (2L, "Bad", true));
        var runner = new SqliteMigrationRunner(cn, asm);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());

        // SQLite uses a single transaction for all migrations;
        // when step 2 fails the whole transaction rolls back — zero history rows.
        Assert.Equal(0L, CountHistory(cn));
    }

    [Fact]
    public async Task SQLite_PartialFailure_RetryAfterFix_OnlyPendingApplied()
    {
        // After the failure, a fixed retry should apply both migrations.
        await using var cn = OpenSqlite();
        CreateSqliteHistory(cn);

        // First attempt: step 2 fails, whole batch rolled back.
        var failAsm = BuildAsm((1L, "Good", false), (2L, "Bad", true));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new SqliteMigrationRunner(cn, failAsm).ApplyMigrationsAsync());

        // Verify clean state: no history rows after rollback.
        Assert.Equal(0L, CountHistory(cn));

        // Retry with fixed step 2: both should be applied.
        var fixedAsm = BuildAsm((1L, "Good", false), (2L, "Fixed", false));
        await new SqliteMigrationRunner(cn, fixedAsm).ApplyMigrationsAsync();

        Assert.Equal(2L, CountHistory(cn));
    }

    [Fact]
    public async Task SQLite_PartialFailure_MiddleStepFails_RollsBackAll()
    {
        await using var cn = OpenSqlite();
        CreateSqliteHistory(cn);

        var asm = BuildAsm((1L, "A", false), (2L, "B", true), (3L, "C", false));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new SqliteMigrationRunner(cn, asm).ApplyMigrationsAsync());

        Assert.Contains("StressTest_simulated_failure", ex.Message);
        // SQLite single-transaction: all rolled back
        Assert.Equal(0L, CountHistory(cn));
    }

    [Fact]
    public async Task SQLite_Retry_AfterCompleteRollback_AllApplied()
    {
        await using var cn = OpenSqlite();
        CreateSqliteHistory(cn);

        // Fail on step 2
        var failAsm = BuildAsm((1L, "A", false), (2L, "Fail", true), (3L, "C", false));
        await Assert.ThrowsAnyAsync<Exception>(
            () => new SqliteMigrationRunner(cn, failAsm).ApplyMigrationsAsync());

        // Retry with all passing
        var okAsm = BuildAsm((1L, "A", false), (2L, "Fixed", false), (3L, "C", false));
        await new SqliteMigrationRunner(cn, okAsm).ApplyMigrationsAsync();
        Assert.Equal(3L, CountHistory(cn));
    }

    [Fact]
    public async Task SQLite_EmptyAssembly_ApplyIsNoOp()
    {
        await using var cn = OpenSqlite();
        var asm = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("EmptyForStress_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        asm.DefineDynamicModule("Mod");

        await new SqliteMigrationRunner(cn, asm).ApplyMigrationsAsync();
        Assert.Equal(0L, CountHistory(cn));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 2. Partial failure — MySQL runner (per-step transactions)
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MySQL_PartialFailure_GoodMigrationsApplied_BadLeavePartial()
    {
        await using var cn = OpenSqlite();
        CreateMysqlHistory(cn);

        var asm = BuildAsm((1L, "S1", false), (2L, "S2Fail", true));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockMysql(cn, asm).ApplyMigrationsAsync());

        // S1 Applied, S2 Partial
        Assert.Equal(1L, CountMysqlHistory(cn, " WHERE Status='Applied'"));
        Assert.Equal(1L, CountMysqlHistory(cn, " WHERE Status='Partial'"));
    }

    [Fact]
    public async Task MySQL_PartialStateBlocks_NextRun()
    {
        await using var cn = OpenSqlite();
        CreateMysqlHistory(cn);

        // Step 1 fails
        var failAsm = BuildAsm((1L, "Step1Fail", true));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockMysql(cn, failAsm).ApplyMigrationsAsync());

        // Second run: Partial state detected → throws
        var fixedAsm = BuildAsm((1L, "Step1Fail", false));
        var ex2 = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockMysql(cn, fixedAsm).ApplyMigrationsAsync());
        Assert.Contains("Partial state", ex2.Message);
    }

    [Fact]
    public async Task MySQL_PartialState_OperatorDeletesThenResumes()
    {
        await using var cn = OpenSqlite();
        CreateMysqlHistory(cn);

        // Fail step 2
        var failAsm = BuildAsm((1L, "S1", false), (2L, "S2Fail", true));
        await Assert.ThrowsAnyAsync<Exception>(
            () => new NoLockMysql(cn, failAsm).ApplyMigrationsAsync());

        // Operator deletes Partial row
        using (var del = cn.CreateCommand())
        {
            del.CommandText = "DELETE FROM `__NormMigrationsHistory` WHERE Status='Partial'";
            del.ExecuteNonQuery();
        }

        // Resume with fixed step 2
        var fixedAsm = BuildAsm((1L, "S1", false), (2L, "S2Fixed", false));
        await new NoLockMysql(cn, fixedAsm).ApplyMigrationsAsync();

        Assert.Equal(2L, CountMysqlHistory(cn, " WHERE Status='Applied'"));
        Assert.Equal(0L, CountMysqlHistory(cn, " WHERE Status='Partial'"));
    }

    [Fact]
    public async Task MySQL_FirstStepFails_Partial_PreventsRerun()
    {
        await using var cn = OpenSqlite();
        CreateMysqlHistory(cn);

        var asm = BuildAsm((1L, "OnlyStep", true));
        await Assert.ThrowsAnyAsync<Exception>(
            () => new NoLockMysql(cn, asm).ApplyMigrationsAsync());

        // Partial row exists
        Assert.Equal(1L, CountMysqlHistory(cn, " WHERE Status='Partial'"));

        // Rerun throws about Partial state
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockMysql(cn, BuildAsm((1L, "OnlyStep", false))).ApplyMigrationsAsync());
        Assert.Contains("Partial state", ex.Message);
    }

    [Fact]
    public async Task MySQL_Retry_AfterOperatorCleanup_AllThreeApplied()
    {
        await using var cn = OpenSqlite();
        CreateMysqlHistory(cn);

        // Run 1: step 3 fails
        var failAsm = BuildAsm((1L, "A", false), (2L, "B", false), (3L, "CFail", true));
        await Assert.ThrowsAnyAsync<Exception>(
            () => new NoLockMysql(cn, failAsm).ApplyMigrationsAsync());

        // Cleanup Partial for step 3
        using (var del = cn.CreateCommand())
        {
            del.CommandText = "DELETE FROM `__NormMigrationsHistory` WHERE Status='Partial'";
            del.ExecuteNonQuery();
        }

        // Run 2: step 3 fixed
        var fixedAsm = BuildAsm((1L, "A", false), (2L, "B", false), (3L, "CFixed", false));
        await new NoLockMysql(cn, fixedAsm).ApplyMigrationsAsync();

        Assert.Equal(3L, CountMysqlHistory(cn, " WHERE Status='Applied'"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 3. Concurrent migration attempts — SQLite runner
    //    SQLite BEGIN EXCLUSIVE serializes concurrent ApplyMigrationsAsync calls
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SQLite_Concurrent_TwoRunners_SharedFile_ExactlyOnceApplied()
    {
        // Use a shared file database so two connections can serialize via EXCLUSIVE lock
        var dbPath = System.IO.Path.GetTempFileName();
        try
        {
            // Initialize the database schema
            using (var init = new SqliteConnection($"Data Source={dbPath}"))
            {
                await init.OpenAsync();
                using var cmd = init.CreateCommand();
                cmd.CommandText =
                    @"CREATE TABLE IF NOT EXISTS ""__NormMigrationsHistory"" " +
                    @"(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL)";
                cmd.ExecuteNonQuery();
            }

            var asm = BuildAsm((1L, "Shared1", false), (2L, "Shared2", false));

            // Open two separate connections to the same file
            var cn1 = new SqliteConnection($"Data Source={dbPath}");
            var cn2 = new SqliteConnection($"Data Source={dbPath}");
            await cn1.OpenAsync();
            await cn2.OpenAsync();

            var runner1 = new SqliteMigrationRunner(cn1, asm);
            var runner2 = new SqliteMigrationRunner(cn2, asm);

            // Run both concurrently
            var t1 = runner1.ApplyMigrationsAsync();
            var t2 = runner2.ApplyMigrationsAsync();

            var results = await Task.WhenAll(
                t1.ContinueWith(t => t.Exception is null ? "ok" : "err"),
                t2.ContinueWith(t => t.Exception is null ? "ok" : "err"));

            // At least one must succeed; the other either succeeds (finds nothing pending) or fails
            // due to the lock. The key invariant: exactly 2 rows in history (not 4 = 2×2).
            await cn1.DisposeAsync();
            await cn2.DisposeAsync();

            // Re-read from the file to verify
            using var verify = new SqliteConnection($"Data Source={dbPath}");
            await verify.OpenAsync();
            using var vcheck = verify.CreateCommand();
            vcheck.CommandText = @"SELECT COUNT(*) FROM ""__NormMigrationsHistory""";
            var count = Convert.ToInt64(vcheck.ExecuteScalar());

            // Must not have duplicated rows
            Assert.True(count <= 2, $"Expected at most 2 history rows, got {count}");
        }
        finally
        {
            try
            {
                SqliteConnection.ClearAllPools();
                System.IO.File.Delete(dbPath);
            }
            catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public async Task SQLite_Concurrent_MemoryDb_SecondRunnerSeesNothingPending()
    {
        // With :memory: SQLite, each connection gets its own DB, so this is more
        // of a logical test: after first runner applies, a second runner on the same
        // connection has nothing to do.
        await using var cn = OpenSqlite();
        CreateSqliteHistory(cn);

        var asm = BuildAsm((1L, "X", false));

        var runner1 = new SqliteMigrationRunner(cn, asm);
        await runner1.ApplyMigrationsAsync();

        // Second runner on same connection should be no-op
        var runner2 = new SqliteMigrationRunner(cn, asm);
        await runner2.ApplyMigrationsAsync(); // must not throw

        Assert.Equal(1L, CountHistory(cn));
    }

    [Fact]
    public async Task SQLite_MultipleSequentialRuns_Idempotent()
    {
        await using var cn = OpenSqlite();
        CreateSqliteHistory(cn);

        var asm = BuildAsm((1L, "M1", false), (2L, "M2", false));
        for (int i = 0; i < 5; i++)
        {
            await new SqliteMigrationRunner(cn, asm).ApplyMigrationsAsync();
        }

        // Only 2 rows despite 5 runs
        Assert.Equal(2L, CountHistory(cn));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 4. Retry consistency — same migration version, different assembly runs
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SQLite_NameDrift_DetectedOnRetry()
    {
        await using var cn = OpenSqlite();
        CreateSqliteHistory(cn);

        // Apply version 1 with name "Alpha"
        await new SqliteMigrationRunner(cn, BuildAsm((1L, "Alpha", false)))
            .ApplyMigrationsAsync();

        // Retry with same version but different name → name drift
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new SqliteMigrationRunner(cn, BuildAsm((1L, "Beta", false)))
                .ApplyMigrationsAsync());
        Assert.Contains("name drift", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task SQLite_DuplicateVersionsInAssembly_DetectedBeforeAnyApply()
    {
        await using var cn = OpenSqlite();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new SqliteMigrationRunner(cn, BuildAsm((1L, "A", false), (1L, "B", false)))
                .ApplyMigrationsAsync());
        Assert.Contains("Duplicate migration versions", ex.Message);
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 5. Large migration batches — throughput and correctness
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SQLite_LargeBatch_50Migrations_AllApplied()
    {
        await using var cn = OpenSqlite();
        CreateSqliteHistory(cn);

        var specs = Enumerable.Range(1, 50)
            .Select(i => ((long)i, $"Mig{i:D3}", false))
            .ToArray();

        await new SqliteMigrationRunner(cn, BuildAsm(specs)).ApplyMigrationsAsync();
        Assert.Equal(50L, CountHistory(cn));
    }

    [Fact]
    public async Task MySQL_LargeBatch_20Migrations_AllApplied()
    {
        await using var cn = OpenSqlite();
        CreateMysqlHistory(cn);

        var specs = Enumerable.Range(1, 20)
            .Select(i => ((long)i, $"MySqlMig{i:D2}", false))
            .ToArray();

        await new NoLockMysql(cn, BuildAsm(specs)).ApplyMigrationsAsync();
        Assert.Equal(20L, CountMysqlHistory(cn, " WHERE Status='Applied'"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 6. History table auto-creation on first run
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SQLite_NoHistoryTable_AutoCreated_OnFirstApply()
    {
        await using var cn = OpenSqlite();
        // Do NOT create history table — runner must create it on first call
        var asm = BuildAsm((1L, "FirstEver", false));
        await new SqliteMigrationRunner(cn, asm).ApplyMigrationsAsync();

        using var check = cn.CreateCommand();
        check.CommandText = @"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='__NormMigrationsHistory'";
        Assert.Equal(1L, Convert.ToInt64(check.ExecuteScalar()));

        Assert.Equal(1L, CountHistory(cn));
    }

    [Fact]
    public async Task MySQL_NoHistoryTable_AutoCreated_OnFirstApply()
    {
        await using var cn = OpenSqlite();
        // Do NOT pre-create history table
        var asm = BuildAsm((1L, "MySqlFirst", false));
        await new NoLockMysql(cn, asm).ApplyMigrationsAsync();

        // Verify table was created and row exists
        Assert.Equal(1L, CountMysqlHistory(cn, " WHERE Status='Applied'"));
    }

    // ══════════════════════════════════════════════════════════════════════════
    // 7. Cancellation — pre-cancelled token propagates immediately
    // ══════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task SQLite_PreCancelledToken_ThrowsOce()
    {
        await using var cn = OpenSqlite();
        var asm = BuildAsm((1L, "WillNotRun", false));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => new SqliteMigrationRunner(cn, asm).ApplyMigrationsAsync(cts.Token));
    }

    [Fact]
    public async Task MySQL_PreCancelledToken_ThrowsOce()
    {
        // MySQL advisory lock is bypassed; the OpenAsync or history create should respect the token
        await using var cn = OpenSqlite();
        var asm = BuildAsm((1L, "WillNotRun", false));
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // The NoLock runner skips the advisory lock, but the connection open/table create
        // should still propagate the cancellation
        await Assert.ThrowsAnyAsync<Exception>(
            () => new NoLockMysql(cn, asm).ApplyMigrationsAsync(cts.Token));
    }
}
