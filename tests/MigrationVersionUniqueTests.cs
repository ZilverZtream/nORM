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
/// Verify that duplicate version numbers and name drift cause fail-fast exceptions.
/// Uses dynamically-built assemblies to avoid polluting the main test assembly with
/// conflicting migration types.
/// </summary>
public class MigrationVersionUniqueTests
{
    // ── Dynamic assembly helpers ─────────────────────────────────────────────

    private static Assembly BuildMigrationsAssembly(params (string Name, long Version)[] migrations)
    {
        var asmName = new AssemblyName("DynMig_" + Guid.NewGuid().ToString("N"));
        var ab = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        var mb = ab.DefineDynamicModule("MainModule");

        var baseMigType = typeof(MigrationBase);
        // Migration has a protected ctor(long version, string name)
        var baseCtor = baseMigType.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;

        var upAbstract  = baseMigType.GetMethod("Up",   new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var downAbstract = baseMigType.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;

        foreach (var (typeName, version) in migrations)
        {
            var tb = mb.DefineType(typeName,
                TypeAttributes.Public | TypeAttributes.Class, baseMigType);

            // public constructor: calls base(version, typeName)
            var ctor = tb.DefineConstructor(MethodAttributes.Public,
                CallingConventions.Standard, Type.EmptyTypes);
            var il = ctor.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I8, version);
            il.Emit(OpCodes.Ldstr, typeName);
            il.Emit(OpCodes.Call, baseCtor);
            il.Emit(OpCodes.Ret);

            // Up override (no-op)
            var up = tb.DefineMethod("Up",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            up.GetILGenerator().Emit(OpCodes.Ret);
            tb.DefineMethodOverride(up, upAbstract);

            // Down override (no-op)
            var down = tb.DefineMethod("Down",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            down.GetILGenerator().Emit(OpCodes.Ret);
            tb.DefineMethodOverride(down, downAbstract);

            tb.CreateType();
        }

        return ab;
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    [Fact]
    public async Task DuplicateVersions_InAssembly_Throws()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // Two migrations sharing Version=5 → must throw before any DB access
        var asm = BuildMigrationsAssembly(("MigAlpha", 5L), ("MigBeta", 5L));
        var runner = new SqliteMigrationRunner(cn, asm);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.GetPendingMigrationsAsync());

        Assert.Contains("Duplicate migration versions", ex.Message);
        Assert.Contains("v5", ex.Message);
    }

    [Fact]
    public async Task AppliedMigrationWithDifferentName_Throws_OnDrift()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // Seed history: Version=10 applied under name "OldName"
        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE \"__NormMigrationsHistory\" " +
                "(\"Version\" INTEGER PRIMARY KEY, \"Name\" TEXT NOT NULL, \"AppliedOn\" TEXT NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }
        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO \"__NormMigrationsHistory\" VALUES (10, 'OldName', '2024-01-01')";
            await cmd.ExecuteNonQueryAsync();
        }

        // Assembly now has the same version but under "NewName" → drift
        var asm = BuildMigrationsAssembly(("NewName", 10L));
        var runner = new SqliteMigrationRunner(cn, asm);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.GetPendingMigrationsAsync());

        Assert.Contains("name drift", ex.Message);
        Assert.Contains("OldName", ex.Message);
        Assert.Contains("NewName", ex.Message);
    }

    [Fact]
    public async Task UniqueVersions_NoThrow()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // Distinct versions — should proceed without error
        var asm = BuildMigrationsAssembly(("CreateFoo", 100L), ("CreateBar", 200L));
        var runner = new SqliteMigrationRunner(cn, asm);

        // No exception expected
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Equal(2, pending.Length);
    }
}
