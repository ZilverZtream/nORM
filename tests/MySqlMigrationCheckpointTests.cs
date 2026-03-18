using System;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using MigrationBase = nORM.Migration.Migration;
using Xunit;

#pragma warning disable CS8765 // Nullability mismatch on overridden member

namespace nORM.Tests;

/// <summary>
/// M1 checkpoint tests: verifies the durable partial-state mechanism added to
/// <see cref="MySqlMigrationRunner"/> to survive DDL auto-commit.
///
/// Key invariants:
///   1. Checkpoint (Status='Partial') is written BEFORE the transaction begins.
///   2. Checkpoint survives transaction rollback.
///   3. A second run with Partial rows throws with an actionable error message.
///   4. Error message contains the version number and DELETE instruction.
///   5. Multiple Partial rows all appear in the error.
///   6. Checkpoint status upgrades to Applied after a successful commit.
/// </summary>
public class MySqlMigrationCheckpointTests
{
    // ── Shared runner that bypasses MySQL advisory locking ────────────────────

    private sealed class NoLockRunner : MySqlMigrationRunner
    {
        public NoLockRunner(DbConnection cn, Assembly asm)
            : base(cn, asm) { }

        protected internal override Task AcquireAdvisoryLockAsync(CancellationToken ct) => Task.CompletedTask;
        protected internal override Task ReleaseAdvisoryLockAsync(CancellationToken ct) => Task.CompletedTask;
    }

    // ── History table helper ──────────────────────────────────────────────────

    private static SqliteConnection OpenSqlite()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    /// <summary>Ensures the history table does NOT have the Status column (legacy layout).</summary>
    private static void CreateLegacyHistoryTable(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE \"__NormMigrationsHistory\" " +
            "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
    }

    private static long CountRows(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ── Dynamic migration assembly builder ────────────────────────────────────

    private static Assembly BuildAsm(params (long V, string N, bool Fail)[] specs)
    {
        var asmName = new AssemblyName($"M1Ckpt_{Guid.NewGuid():N}");
        var ab = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        var mb = ab.DefineDynamicModule("Mod");

        var migBase = typeof(MigrationBase);
        var baseCtor = migBase.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance, null,
            new[] { typeof(long), typeof(string) }, null)!;
        var upMethod   = migBase.GetMethod("Up",   new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var downMethod = migBase.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var throwCtor  = typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!;

        foreach (var (v, n, fail) in specs)
        {
            var tb = mb.DefineType(n, TypeAttributes.Public | TypeAttributes.Class, migBase);

            var ctorB = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            var ctorIL = ctorB.GetILGenerator();
            ctorIL.Emit(OpCodes.Ldarg_0);
            ctorIL.Emit(OpCodes.Ldc_I8, v);
            ctorIL.Emit(OpCodes.Ldstr, n);
            ctorIL.Emit(OpCodes.Call, baseCtor);
            ctorIL.Emit(OpCodes.Ret);

            var upB = tb.DefineMethod("Up",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            var upIL = upB.GetILGenerator();
            if (fail)
            {
                upIL.Emit(OpCodes.Ldstr, "M1 simulated failure");
                upIL.Emit(OpCodes.Newobj, throwCtor);
                upIL.Emit(OpCodes.Throw);
            }
            else { upIL.Emit(OpCodes.Ret); }
            tb.DefineMethodOverride(upB, upMethod);

            var downB = tb.DefineMethod("Down",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            var downIL = downB.GetILGenerator();
            downIL.Emit(OpCodes.Ret);
            tb.DefineMethodOverride(downB, downMethod);

            tb.CreateType();
        }
        return ab;
    }

    // ── M1-CK-1: Partial row is present after Up() failure ───────────────────

    [Fact]
    public async Task FailedMigration_leaves_Partial_row_in_history()
    {
        await using var cn = OpenSqlite();
        CreateLegacyHistoryTable(cn);

        var asm = BuildAsm((2000L, "Ckpt_Fail", true));
        var runner = new NoLockRunner(cn, asm);

        await Assert.ThrowsAsync<InvalidOperationException>(() => runner.ApplyMigrationsAsync());

        // M1: Partial row must exist even though the transaction was rolled back.
        var partial = CountRows(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Status = 'Partial'");
        Assert.Equal(1L, partial);
    }

    // ── M1-CK-2: Partial row survives transaction rollback ────────────────────

    [Fact]
    public async Task Checkpoint_row_survives_rollback()
    {
        // The checkpoint INSERT is made OUTSIDE the per-step transaction.
        // Even after the transaction is rolled back (failure path), the Partial row must remain.
        await using var cn = OpenSqlite();
        CreateLegacyHistoryTable(cn);

        var asm = BuildAsm((3000L, "Ckpt_RollbackSurvive", true));
        var runner = new NoLockRunner(cn, asm);

        await Assert.ThrowsAsync<InvalidOperationException>(() => runner.ApplyMigrationsAsync());

        // Total row count = 1 (the Partial row).
        var total = CountRows(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"");
        Assert.Equal(1L, total);

        var partialCount = CountRows(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Status = 'Partial'");
        Assert.Equal(1L, partialCount);
    }

    // ── M1-CK-3: Second run throws with actionable error for Partial row ──────

    [Fact]
    public async Task Partial_state_on_rerun_throws_InvalidOperationException()
    {
        await using var cn = OpenSqlite();
        CreateLegacyHistoryTable(cn);

        // Run 1: migration fails → Partial row created.
        var failAsm = BuildAsm((4000L, "Ckpt_Rerun", true));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockRunner(cn, failAsm).ApplyMigrationsAsync());

        // Run 2: same (or fixed) migration, Partial row still present → must throw.
        var fixedAsm = BuildAsm((4000L, "Ckpt_Rerun", false));
        var ex2 = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockRunner(cn, fixedAsm).ApplyMigrationsAsync());

        Assert.Contains("Partial state", ex2.Message);
    }

    // ── M1-CK-4: Error message contains the failing migration version ─────────

    [Fact]
    public async Task Partial_state_error_message_contains_version_number()
    {
        await using var cn = OpenSqlite();
        CreateLegacyHistoryTable(cn);

        var asm = BuildAsm((5555L, "Ckpt_VersionInError", true));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockRunner(cn, asm).ApplyMigrationsAsync());

        var fixedAsm = BuildAsm((5555L, "Ckpt_VersionInError", false));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockRunner(cn, fixedAsm).ApplyMigrationsAsync());

        Assert.Contains("5555", ex.Message);
    }

    // ── M1-CK-5: Error message contains operator DELETE instruction ───────────

    [Fact]
    public async Task Partial_state_error_message_contains_delete_instruction()
    {
        await using var cn = OpenSqlite();
        CreateLegacyHistoryTable(cn);

        var asm = BuildAsm((6000L, "Ckpt_DeleteInstruction", true));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockRunner(cn, asm).ApplyMigrationsAsync());

        var fixedAsm = BuildAsm((6000L, "Ckpt_DeleteInstruction", false));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockRunner(cn, fixedAsm).ApplyMigrationsAsync());

        // Error must tell the operator what to do (DELETE instruction).
        Assert.Contains("DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("__NormMigrationsHistory", ex.Message);
    }

    // ── M1-CK-6: Multiple Partial rows listed in error ────────────────────────

    [Fact]
    public async Task Multiple_partial_rows_all_listed_in_error_message()
    {
        await using var cn = OpenSqlite();
        CreateLegacyHistoryTable(cn);

        // Manually insert two Partial rows (simulating two successive partial failures).
        using (var cmd = cn.CreateCommand())
        {
            // Must first add Status column (simulate EnsureHistoryTableAsync having run once).
            cmd.CommandText = "ALTER TABLE \"__NormMigrationsHistory\" ADD COLUMN Status TEXT NOT NULL DEFAULT 'Applied'";
            cmd.ExecuteNonQuery();

            cmd.CommandText =
                "INSERT INTO \"__NormMigrationsHistory\" (Version, Name, AppliedOn, Status) VALUES " +
                "(7000, 'MultiStep1', '2025-01-01', 'Partial');" +
                "INSERT INTO \"__NormMigrationsHistory\" (Version, Name, AppliedOn, Status) VALUES " +
                "(7001, 'MultiStep2', '2025-01-01', 'Partial');";
            cmd.ExecuteNonQuery();
        }

        var asm = BuildAsm(
            (7000L, "MultiStep1", false),
            (7001L, "MultiStep2", false));

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockRunner(cn, asm).ApplyMigrationsAsync());

        Assert.Contains("7000", ex.Message);
        Assert.Contains("7001", ex.Message);
        Assert.Contains("Partial state", ex.Message);
    }

    // ── M1-CK-7: Successful step upgrades Partial to Applied ─────────────────

    [Fact]
    public async Task Successful_migration_upgrades_checkpoint_to_Applied()
    {
        await using var cn = OpenSqlite();
        CreateLegacyHistoryTable(cn);

        var asm = BuildAsm((8000L, "Ckpt_Success", false));
        var runner = new NoLockRunner(cn, asm);
        await runner.ApplyMigrationsAsync();

        // Must have exactly 1 Applied row, 0 Partial rows.
        var applied = CountRows(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Status = 'Applied'");
        var partial  = CountRows(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Status = 'Partial'");

        Assert.Equal(1L, applied);
        Assert.Equal(0L, partial);
    }

    // ── M1-CK-8: EnsureHistoryTableAsync upgrades legacy table (no Status) ────

    [Fact]
    public async Task Legacy_table_without_status_column_is_upgraded()
    {
        // Pre-seed a legacy history table (no Status column) with applied rows.
        await using var cn = OpenSqlite();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE \"__NormMigrationsHistory\" " +
                "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL);" +
                "INSERT INTO \"__NormMigrationsHistory\" VALUES (9000, 'LegacyStep1', '2025-01-01');";
            cmd.ExecuteNonQuery();
        }

        // Run a new migration. EnsureHistoryTableAsync must add Status column and upgrade existing rows.
        var asm = BuildAsm((9001L, "NewStep", false));
        var runner = new NoLockRunner(cn, asm);
        await runner.ApplyMigrationsAsync();

        // Both rows must now have a Status value.
        var noStatus = CountRows(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Status IS NULL");
        Assert.Equal(0L, noStatus);

        // Legacy row defaults to 'Applied'; new step is also Applied.
        var appliedCount = CountRows(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Status = 'Applied'");
        Assert.Equal(2L, appliedCount);
    }

    // ── M1-CK-9: Operator deletes Partial row → resume succeeds ─────────────

    [Fact]
    public async Task After_operator_deletes_partial_row_resume_succeeds()
    {
        await using var cn = OpenSqlite();
        CreateLegacyHistoryTable(cn);

        // Fail step 2.
        var failAsm = BuildAsm((10000L, "Grp1Step1", false), (10001L, "Grp1Step2", true));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockRunner(cn, failAsm).ApplyMigrationsAsync());

        // Operator cleanup.
        using (var del = cn.CreateCommand())
        {
            del.CommandText = "DELETE FROM \"__NormMigrationsHistory\" WHERE Version = 10001";
            del.ExecuteNonQuery();
        }

        // Resume with fixed step 2.
        var fixedAsm = BuildAsm((10000L, "Grp1Step1", false), (10001L, "Grp1Step2Fixed", false));
        await new NoLockRunner(cn, fixedAsm).ApplyMigrationsAsync();

        var applied = CountRows(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Status = 'Applied'");
        Assert.Equal(2L, applied);
    }
}
