using System;
using System.Data.Common;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Migration;
using MigrationBase = nORM.Migration.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.0 → 4.5 — Fault-injected migration replay
// ══════════════════════════════════════════════════════════════════════════════

/// <summary>
/// Verifies migration recovery after faults at different points in the apply sequence.
/// Uses dynamically-built assemblies (Reflection.Emit) so that fault-injectable
/// migration types do NOT pollute the main test assembly and interfere with the
/// SqliteMigrationRunnerTests assembly-scan tests.
/// </summary>
public class FaultInjectedMigrationReplayTests
{
    // ── Dynamic assembly builder ──────────────────────────────────────────────

    private static readonly Type[] _upDownParams =
        { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) };

    private static readonly ConstructorInfo _baseCtor =
        typeof(MigrationBase).GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;

    private static readonly MethodInfo _upAbstract =
        typeof(MigrationBase).GetMethod("Up", _upDownParams)!;
    private static readonly MethodInfo _downAbstract =
        typeof(MigrationBase).GetMethod("Down", _upDownParams)!;

    private static readonly ConstructorInfo _ioExCtor =
        typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!;

    /// <summary>Builds a dynamic assembly with no-op migrations at the given versions.</summary>
    private static Assembly NoOpAssembly(params (string Name, long Version)[] specs)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("FiMig_NoOp_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");

        foreach (var (name, version) in specs)
        {
            var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
            EmitCtor(tb, version, name);
            EmitNoOpUp(tb);
            EmitNoOpDown(tb);
            tb.CreateType();
        }
        return ab;
    }

    /// <summary>
    /// Builds a dynamic assembly with two migrations. The first (v1) is a no-op;
    /// the second (v2) throws InvalidOperationException from Up().
    /// </summary>
    private static Assembly AssemblyWithThrowingSecond(long v1, string n1, long v2, string n2)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("FiMig_Throw_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");

        // First: no-op
        var tb1 = mod.DefineType(n1, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb1, v1, n1);
        EmitNoOpUp(tb1);
        EmitNoOpDown(tb1);
        tb1.CreateType();

        // Second: throws from Up()
        var tb2 = mod.DefineType(n2, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb2, v2, n2);
        EmitThrowingUp(tb2);
        EmitNoOpDown(tb2);
        tb2.CreateType();

        return ab;
    }

    /// <summary>Single-migration assembly whose only migration throws from Up().</summary>
    private static Assembly SingleThrowingAssembly(long version, string name)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("FiMig_Single_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");
        var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb, version, name);
        EmitThrowingUp(tb);
        EmitNoOpDown(tb);
        tb.CreateType();
        return ab;
    }

    // ── Emit helpers ──────────────────────────────────────────────────────────

    private static void EmitCtor(TypeBuilder tb, long version, string name)
    {
        var ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
        var il = ctor.GetILGenerator();
        il.Emit(OpCodes.Ldarg_0);
        il.Emit(OpCodes.Ldc_I8, version);
        il.Emit(OpCodes.Ldstr, name);
        il.Emit(OpCodes.Call, _baseCtor);
        il.Emit(OpCodes.Ret);
    }

    private static void EmitNoOpUp(TypeBuilder tb)
    {
        var m = tb.DefineMethod("Up",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
            typeof(void), _upDownParams);
        m.GetILGenerator().Emit(OpCodes.Ret);
        tb.DefineMethodOverride(m, _upAbstract);
    }

    private static void EmitNoOpDown(TypeBuilder tb)
    {
        var m = tb.DefineMethod("Down",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
            typeof(void), _upDownParams);
        m.GetILGenerator().Emit(OpCodes.Ret);
        tb.DefineMethodOverride(m, _downAbstract);
    }

    private static void EmitThrowingUp(TypeBuilder tb)
    {
        var m = tb.DefineMethod("Up",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
            typeof(void), _upDownParams);
        var il = m.GetILGenerator();
        il.Emit(OpCodes.Ldstr, "Simulated DDL failure in migration Up()");
        il.Emit(OpCodes.Newobj, _ioExCtor);
        il.Emit(OpCodes.Throw);
        tb.DefineMethodOverride(m, _upAbstract);
    }

    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection OpenConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static async Task<long> HistoryCountAsync(SqliteConnection cn)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    // ── Tests ─────────────────────────────────────────────────────────────────

    [Fact]
    public async Task FaultInMigrationUp_SingleMigration_HistoryRemainsEmpty()
    {
        // When the only migration throws from Up(), the transaction is rolled back
        // and the history table must remain empty.
        using var cn = OpenConnection();
        var runner = new SqliteMigrationRunner(cn, SingleThrowingAssembly(1L, "ThrowingMig"));

        var ex = await Record.ExceptionAsync(() => runner.ApplyMigrationsAsync());

        Assert.NotNull(ex);
        Assert.IsType<InvalidOperationException>(ex);
        Assert.Equal(0L, await HistoryCountAsync(cn));
    }

    [Fact]
    public async Task FaultInMigrationUp_TransactionRolledBack_BothMigrationsRemainPending()
    {
        // SQLite runner wraps all migrations in a single transaction.
        // If migration 2 throws, migration 1's history INSERT is also rolled back.
        using var cn = OpenConnection();
        var runner = new SqliteMigrationRunner(cn, AssemblyWithThrowingSecond(10L, "Good", 11L, "Bad"));

        var ex = await Record.ExceptionAsync(() => runner.ApplyMigrationsAsync());
        Assert.NotNull(ex);

        // History must be empty — both migrations rolled back.
        Assert.Equal(0L, await HistoryCountAsync(cn));

        // Both still pending.
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Equal(2, pending.Length);
        Assert.Contains("10_Good", pending);
        Assert.Contains("11_Bad", pending);
    }

    [Fact]
    public async Task FaultInMigrationUp_AfterFix_ReplaySucceedsAndHistoryComplete()
    {
        using var cn = OpenConnection();

        // First run: v2 throws → both rolled back.
        var failAsm = AssemblyWithThrowingSecond(20L, "MigPass", 21L, "MigFail");
        await Record.ExceptionAsync(() => new SqliteMigrationRunner(cn, failAsm).ApplyMigrationsAsync());
        Assert.Equal(0L, await HistoryCountAsync(cn));

        // Second run: both no-ops → succeeds.
        var fixedAsm = NoOpAssembly(("MigPass", 20L), ("MigFixed", 21L));
        await new SqliteMigrationRunner(cn, fixedAsm).ApplyMigrationsAsync();

        Assert.Equal(2L, await HistoryCountAsync(cn));
        Assert.False(await new SqliteMigrationRunner(cn, fixedAsm).HasPendingMigrationsAsync());
    }

    [Fact]
    public async Task ApplyMigrations_Idempotent_SecondRunIsNoOp()
    {
        using var cn = OpenConnection();
        var asm = NoOpAssembly(("IdempotentA", 30L), ("IdempotentB", 31L));
        var runner = new SqliteMigrationRunner(cn, asm);

        await runner.ApplyMigrationsAsync();
        var countAfterFirst = await HistoryCountAsync(cn);

        await runner.ApplyMigrationsAsync(); // must be a no-op

        Assert.Equal(countAfterFirst, await HistoryCountAsync(cn));
    }

    [Fact]
    public async Task HasPendingMigrations_AfterSuccessfulApply_ReturnsFalse()
    {
        using var cn = OpenConnection();
        var asm = NoOpAssembly(("PendingA", 40L), ("PendingB", 41L));
        var runner = new SqliteMigrationRunner(cn, asm);

        Assert.True(await runner.HasPendingMigrationsAsync());
        await runner.ApplyMigrationsAsync();
        Assert.False(await runner.HasPendingMigrationsAsync());
    }

    [Fact]
    public async Task GetPendingMigrations_AfterPartialFault_ListsAllPending()
    {
        using var cn = OpenConnection();
        var failAsm = AssemblyWithThrowingSecond(50L, "GoodFirst", 51L, "BadSecond");
        var runner = new SqliteMigrationRunner(cn, failAsm);

        await Record.ExceptionAsync(() => runner.ApplyMigrationsAsync());

        // Both migrations still pending (fault rolled back all).
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Contains("50_GoodFirst", pending);
        Assert.Contains("51_BadSecond", pending);
    }

    [Fact]
    public async Task ReplayAfterFault_OnlyRunsPendingMigrations_NotAlreadyApplied()
    {
        // Use 3 migrations: first 2 succeed in run 1, then a third is added in run 2.
        using var cn = OpenConnection();

        var run1Asm = NoOpAssembly(("Run1A", 60L), ("Run1B", 61L));
        var runner1 = new SqliteMigrationRunner(cn, run1Asm);
        await runner1.ApplyMigrationsAsync();
        Assert.Equal(2L, await HistoryCountAsync(cn));

        // Run 2: same two + a new third.
        var run2Asm = NoOpAssembly(("Run1A", 60L), ("Run1B", 61L), ("Run2C", 62L));
        var runner2 = new SqliteMigrationRunner(cn, run2Asm);
        await runner2.ApplyMigrationsAsync();

        // Only 3 total entries — the first two were not re-applied.
        Assert.Equal(3L, await HistoryCountAsync(cn));
    }
}
