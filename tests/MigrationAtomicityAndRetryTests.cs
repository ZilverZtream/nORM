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

namespace nORM.Tests;

/// <summary>
/// Verifies that the migration runner's transaction wrapping guarantees atomicity:
/// a failure inside any migration's Up() rolls back all pending changes for the batch,
/// including previously-applied migrations in the same run and their history entries.
/// Also verifies that a failed batch leaves the database in a state where a corrected
/// retry can succeed.
/// </summary>
public class MigrationAtomicityAndRetryTests
{
    // ── Dynamic assembly helpers ──────────────────────────────────────────

    /// <summary>
    /// Builds a dynamic assembly containing no-op migration classes with the given versions.
    /// </summary>
    private static Assembly BuildNoOpMigrationsAssembly(params (string Name, long Version)[] migrations)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("DynMig_NoOp_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");
        var baseMigType = typeof(MigrationBase);
        var baseCtor = baseMigType.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;
        var upMethod = baseMigType.GetMethod("Up",   new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var downMethod = baseMigType.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;

        foreach (var (name, version) in migrations)
        {
            var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, baseMigType);
            var ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            var il = ctor.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I8, version);
            il.Emit(OpCodes.Ldstr, name);
            il.Emit(OpCodes.Call, baseCtor);
            il.Emit(OpCodes.Ret);

            var up = tb.DefineMethod("Up", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            up.GetILGenerator().Emit(OpCodes.Ret);
            tb.DefineMethodOverride(up, upMethod);

            var down = tb.DefineMethod("Down", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            down.GetILGenerator().Emit(OpCodes.Ret);
            tb.DefineMethodOverride(down, downMethod);

            tb.CreateType();
        }
        return ab;
    }

    /// <summary>
    /// Builds a dynamic assembly where the first migration is a no-op and the second
    /// throws an exception from Up().
    /// </summary>
    private static Assembly BuildAssemblyWithThrowingSecond(string firstVersion, long v1, string secondName, long v2)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("DynMig_Throw_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");
        var baseMigType = typeof(MigrationBase);
        var baseCtor = baseMigType.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;
        var upMethod   = baseMigType.GetMethod("Up",   new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var downMethod = baseMigType.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var exCtor = typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!;

        // First migration: no-op Up
        {
            var tb = mod.DefineType(firstVersion, TypeAttributes.Public | TypeAttributes.Class, baseMigType);
            var ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            var il = ctor.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Ldc_I8, v1); il.Emit(OpCodes.Ldstr, firstVersion);
            il.Emit(OpCodes.Call, baseCtor); il.Emit(OpCodes.Ret);
            var up = tb.DefineMethod("Up", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            up.GetILGenerator().Emit(OpCodes.Ret);
            tb.DefineMethodOverride(up, upMethod);
            var down = tb.DefineMethod("Down", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            down.GetILGenerator().Emit(OpCodes.Ret);
            tb.DefineMethodOverride(down, downMethod);
            tb.CreateType();
        }

        // Second migration: Up throws
        {
            var tb = mod.DefineType(secondName, TypeAttributes.Public | TypeAttributes.Class, baseMigType);
            var ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            var il = ctor.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0); il.Emit(OpCodes.Ldc_I8, v2); il.Emit(OpCodes.Ldstr, secondName);
            il.Emit(OpCodes.Call, baseCtor); il.Emit(OpCodes.Ret);
            var up = tb.DefineMethod("Up", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            var upIl = up.GetILGenerator();
            upIl.Emit(OpCodes.Ldstr, "Simulated migration failure");
            upIl.Emit(OpCodes.Newobj, exCtor);
            upIl.Emit(OpCodes.Throw);
            tb.DefineMethodOverride(up, upMethod);
            var down = tb.DefineMethod("Down", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            down.GetILGenerator().Emit(OpCodes.Ret);
            tb.DefineMethodOverride(down, downMethod);
            tb.CreateType();
        }

        return ab;
    }

    // ── Tests ─────────────────────────────────────────────────────────────

    /// <summary>
    /// When a single-migration batch throws from Up(), the history table must remain
    /// empty — the transaction rollback prevents partial history recording.
    /// </summary>
    [Fact]
    public async Task SingleMigration_UpThrows_HistoryTableRemainsEmpty()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // Assembly with one throwing migration
        var asm = BuildAssemblyWithThrowingSecond("NoOpFirst", 1L, "ThrowingSecond", 2L);
        // Use only the throwing migration by restricting to v2 — easier: use an assembly
        // with just one throwing migration
        var throwingOnly = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("DynMig_ThrowOnly_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var throwMod = throwingOnly.DefineDynamicModule("Main");
        var baseMigType = typeof(MigrationBase);
        var baseCtor = baseMigType.GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;
        var upMethod = baseMigType.GetMethod("Up", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var downMethod = baseMigType.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var exCtor = typeof(InvalidOperationException).GetConstructor(new[] { typeof(string) })!;
        {
            var tb = throwMod.DefineType("ThrowMig", TypeAttributes.Public | TypeAttributes.Class, baseMigType);
            var ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            var ilC = ctor.GetILGenerator();
            ilC.Emit(OpCodes.Ldarg_0); ilC.Emit(OpCodes.Ldc_I8, 99L); ilC.Emit(OpCodes.Ldstr, "ThrowMig");
            ilC.Emit(OpCodes.Call, baseCtor); ilC.Emit(OpCodes.Ret);
            var up = tb.DefineMethod("Up", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            var upIl = up.GetILGenerator();
            upIl.Emit(OpCodes.Ldstr, "Intentional test failure");
            upIl.Emit(OpCodes.Newobj, exCtor);
            upIl.Emit(OpCodes.Throw);
            tb.DefineMethodOverride(up, upMethod);
            var down = tb.DefineMethod("Down", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            down.GetILGenerator().Emit(OpCodes.Ret);
            tb.DefineMethodOverride(down, downMethod);
            tb.CreateType();
        }

        var runner = new SqliteMigrationRunner(cn, throwingOnly);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());

        // History table may not exist if EnsureHistoryTable was skipped, or may exist empty
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\";";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(0L, count);
    }

    /// <summary>
    /// When migration 2 of 2 throws, migration 1's work and history entry must also be
    /// rolled back — the entire batch is atomic.
    /// </summary>
    [Fact]
    public async Task TwoMigrations_SecondThrows_BothRolledBack()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var asm = BuildAssemblyWithThrowingSecond("PassFirst", 10L, "FailSecond", 11L);
        var runner = new SqliteMigrationRunner(cn, asm);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());

        // Neither migration should be recorded in history
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\";";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(0L, count);

        // Both migrations must still show as pending after the rollback
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Equal(2, pending.Length);
    }

    /// <summary>
    /// After a failed migration batch, replacing the failing assembly with a passing one
    /// and re-running the runner succeeds and records all history entries.
    /// </summary>
    [Fact]
    public async Task FailedMigration_Retry_WithFixedAssembly_Succeeds()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // First attempt: second migration throws
        var failAsm = BuildAssemblyWithThrowingSecond("MigA", 20L, "MigBFail", 21L);
        var failRunner = new SqliteMigrationRunner(cn, failAsm);
        await Assert.ThrowsAsync<InvalidOperationException>(() => failRunner.ApplyMigrationsAsync());

        // Second attempt: both migrations are no-ops → succeeds
        var passAsm = BuildNoOpMigrationsAssembly(("MigA", 20L), ("MigBFixed", 21L));
        var passRunner = new SqliteMigrationRunner(cn, passAsm);
        await passRunner.ApplyMigrationsAsync();

        Assert.False(await passRunner.HasPendingMigrationsAsync());

        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\";";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(2L, count);
    }

    /// <summary>
    /// After a successful migration run, calling ApplyMigrationsAsync again is a no-op:
    /// already-applied versions are not re-applied and the history count stays the same.
    /// </summary>
    [Fact]
    public async Task AppliedMigrations_RerunIsIdempotent()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var asm = BuildNoOpMigrationsAssembly(("IdempotentMig1", 30L), ("IdempotentMig2", 31L));
        var runner = new SqliteMigrationRunner(cn, asm);

        await runner.ApplyMigrationsAsync();
        await runner.ApplyMigrationsAsync(); // second call must be no-op

        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\";";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(2L, count);

        Assert.False(await runner.HasPendingMigrationsAsync());
    }
}
