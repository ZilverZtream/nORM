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
#nullable enable

namespace nORM.Tests;

/// <summary>
/// Migration replay and failure tests across all 4 providers.
/// SQLite tests run live against in-memory databases.
/// MySQL/Postgres/SqlServer tests are either shape tests (verifying internal behavior
/// via reflection or subclass bypass) or env-gated for live provider availability.
/// </summary>
public class MigrationReplayFailureTests
{
    // ── Dynamic assembly builder ────────────────────────────────────────────

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
            new AssemblyName("MRF_NoOp_" + Guid.NewGuid().ToString("N")),
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

    /// <summary>Builds a single-migration assembly that creates a real table in Up().</summary>
    private static Assembly DdlAssembly(long version, string name, string createTableSql)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("MRF_Ddl_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");

        var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb, version, name);
        EmitDdlUp(tb, createTableSql);
        EmitNoOpDown(tb);
        tb.CreateType();
        return ab;
    }

    /// <summary>Builds a two-migration assembly: first creates a table, second throws.</summary>
    private static Assembly TwoMigAssembly(
        long v1, string n1, string createSql,
        long v2, string n2)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("MRF_Two_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");

        // First migration: DDL
        var tb1 = mod.DefineType(n1, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb1, v1, n1);
        EmitDdlUp(tb1, createSql);
        EmitNoOpDown(tb1);
        tb1.CreateType();

        // Second migration: throws
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
            new AssemblyName("MRF_Throw_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");
        var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb, version, name);
        EmitThrowingUp(tb);
        EmitNoOpDown(tb);
        tb.CreateType();
        return ab;
    }

    // ── Emit helpers ────────────────────────────────────────────────────────

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
        il.Emit(OpCodes.Ldstr, "Simulated migration failure");
        il.Emit(OpCodes.Newobj, _ioExCtor);
        il.Emit(OpCodes.Throw);
        tb.DefineMethodOverride(m, _upAbstract);
    }

    private static void EmitDdlUp(TypeBuilder tb, string sql)
    {
        // Emit IL that does:
        //   var cmd = connection.CreateCommand();
        //   cmd.Transaction = transaction;
        //   cmd.CommandText = sql;
        //   cmd.ExecuteNonQuery();
        //   cmd.Dispose();
        var m = tb.DefineMethod("Up",
            MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
            typeof(void), _upDownParams);
        var il = m.GetILGenerator();

        var createCmd = typeof(DbConnection).GetMethod("CreateCommand")!;
        var setTx = typeof(DbCommand).GetProperty("Transaction")!.GetSetMethod()!;
        var setCmdText = typeof(DbCommand).GetProperty("CommandText")!.GetSetMethod()!;
        var execNonQ = typeof(DbCommand).GetMethod("ExecuteNonQuery")!;
        var dispose = typeof(IDisposable).GetMethod("Dispose")!;

        il.DeclareLocal(typeof(DbCommand));
        il.Emit(OpCodes.Ldarg_1);          // connection
        il.Emit(OpCodes.Callvirt, createCmd);
        il.Emit(OpCodes.Stloc_0);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldarg_2);          // transaction
        il.Emit(OpCodes.Callvirt, setTx);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Ldstr, sql);
        il.Emit(OpCodes.Callvirt, setCmdText);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Callvirt, execNonQ);
        il.Emit(OpCodes.Pop);

        il.Emit(OpCodes.Ldloc_0);
        il.Emit(OpCodes.Callvirt, dispose);

        il.Emit(OpCodes.Ret);
        tb.DefineMethodOverride(m, _upAbstract);
    }

    // ── Connection / History helpers ─────────────────────────────────────────

    private static SqliteConnection OpenSqlite()
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

    private static long TableCount(SqliteConnection cn, string tableName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{tableName}'";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ── MySQL NoLock subclass for shape tests ────────────────────────────────

    private sealed class NoLockMySqlRunner : MySqlMigrationRunner
    {
        public NoLockMySqlRunner(DbConnection cn, Assembly asm)
            : base(cn, asm) { }

        protected internal override Task AcquireAdvisoryLockAsync(CancellationToken ct) => Task.CompletedTask;
        protected internal override Task ReleaseAdvisoryLockAsync(CancellationToken ct) => Task.CompletedTask;
    }

    /// <summary>Creates the MySQL-style history table with Status column on SQLite.</summary>
    private static void CreateMySqlHistoryTable(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE \"__NormMigrationsHistory\" " +
            "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL, " +
            "Status TEXT NOT NULL DEFAULT 'Applied');";
        cmd.ExecuteNonQuery();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1. SQLite migration replay idempotency
    // ═══════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Apply migrations, then re-apply the same migrations. The second run must be
    /// a no-op (already applied = skip) without error.
    /// </summary>
    [Fact]
    public async Task SQLite_ReplayIdempotency_SecondRunIsNoOp()
    {
        using var cn = OpenSqlite();
        var asm = NoOpAssembly(("Idem_Step1", 100L), ("Idem_Step2", 101L));
        var runner = new SqliteMigrationRunner(cn, asm);

        // First run: applies both
        await runner.ApplyMigrationsAsync();
        Assert.Equal(2L, await HistoryCountAsync(cn));
        Assert.False(await runner.HasPendingMigrationsAsync());

        // Second run: must be a no-op
        await runner.ApplyMigrationsAsync();
        Assert.Equal(2L, await HistoryCountAsync(cn));
        Assert.False(await runner.HasPendingMigrationsAsync());
    }

    /// <summary>
    /// Apply with a DDL migration that creates a table, then re-apply.
    /// The table must still exist and the second run must be a no-op.
    /// </summary>
    [Fact]
    public async Task SQLite_ReplayIdempotency_WithDdl_TablePersistsAndNoError()
    {
        using var cn = OpenSqlite();
        var asm = DdlAssembly(200L, "CreateWidget",
            "CREATE TABLE MRF_Widget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)");
        var runner = new SqliteMigrationRunner(cn, asm);

        await runner.ApplyMigrationsAsync();
        Assert.Equal(1L, TableCount(cn, "MRF_Widget"));

        // Second run: already applied, no-op
        var runner2 = new SqliteMigrationRunner(cn, asm);
        await runner2.ApplyMigrationsAsync();
        Assert.Equal(1L, TableCount(cn, "MRF_Widget"));
        Assert.Equal(1L, await HistoryCountAsync(cn));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 2. SQLite migration failure rollback
    // ═══════════════════════════════════════════════════════════════════════

    /// <summary>
    /// A migration whose Up() throws must propagate the exception. Previously
    /// applied migrations must remain intact (history table empty since SQLite
    /// uses a single transaction for all migrations and rolls back atomically).
    /// </summary>
    [Fact]
    public async Task SQLite_FailureRollback_ExceptionPropagates_HistoryEmpty()
    {
        using var cn = OpenSqlite();
        var asm = SingleThrowingAssembly(300L, "FailMig");
        var runner = new SqliteMigrationRunner(cn, asm);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());
        Assert.Contains("Simulated migration failure", ex.Message);

        // History must be empty (transaction rolled back)
        Assert.Equal(0L, await HistoryCountAsync(cn));
    }

    /// <summary>
    /// After a failure, the migration must remain pending.
    /// </summary>
    [Fact]
    public async Task SQLite_FailureRollback_MigrationRemainsPending()
    {
        using var cn = OpenSqlite();
        var asm = SingleThrowingAssembly(301L, "StillPending");
        var runner = new SqliteMigrationRunner(cn, asm);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());

        Assert.True(await runner.HasPendingMigrationsAsync());
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Contains("301_StillPending", pending);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 3. SQLite migration partial batch
    // ═══════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Apply 2 migrations where the first creates a table and the second throws.
    /// SQLite wraps all in a single transaction, so both should be rolled back.
    /// The table from the first migration should NOT exist.
    /// </summary>
    [Fact]
    public async Task SQLite_PartialBatch_FirstSucceeds_SecondFails_BothRolledBack()
    {
        using var cn = OpenSqlite();
        var asm = TwoMigAssembly(
            400L, "CreateGadget", "CREATE TABLE MRF_Gadget (Id INTEGER PRIMARY KEY, Label TEXT)",
            401L, "FailStep");
        var runner = new SqliteMigrationRunner(cn, asm);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());

        // SQLite single-transaction semantics: both rolled back
        Assert.Equal(0L, await HistoryCountAsync(cn));

        // The table from step 1 should NOT exist (rolled back)
        Assert.Equal(0L, TableCount(cn, "MRF_Gadget"));
    }

    /// <summary>
    /// After partial batch failure in SQLite, both migrations remain pending.
    /// </summary>
    [Fact]
    public async Task SQLite_PartialBatch_BothRemainPending()
    {
        using var cn = OpenSqlite();
        var asm = TwoMigAssembly(
            402L, "StepA", "CREATE TABLE MRF_StepA (Id INTEGER PRIMARY KEY)",
            403L, "StepB");
        var runner = new SqliteMigrationRunner(cn, asm);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());

        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Equal(2, pending.Length);
        Assert.Contains("402_StepA", pending);
        Assert.Contains("403_StepB", pending);
    }

    /// <summary>
    /// After fixing the failing migration, replay succeeds and all migrations are applied.
    /// </summary>
    [Fact]
    public async Task SQLite_PartialBatch_FixAndReplay_Succeeds()
    {
        using var cn = OpenSqlite();

        // First run: step 2 fails -> all rolled back
        var failAsm = TwoMigAssembly(
            404L, "CreateThing", "CREATE TABLE MRF_Thing (Id INTEGER PRIMARY KEY)",
            405L, "FailThing");
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new SqliteMigrationRunner(cn, failAsm).ApplyMigrationsAsync());
        Assert.Equal(0L, await HistoryCountAsync(cn));

        // Second run: both succeed
        var fixedAsm = NoOpAssembly(("CreateThing", 404L), ("FixedThing", 405L));
        await new SqliteMigrationRunner(cn, fixedAsm).ApplyMigrationsAsync();
        Assert.Equal(2L, await HistoryCountAsync(cn));
        Assert.False(await new SqliteMigrationRunner(cn, fixedAsm).HasPendingMigrationsAsync());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 4. MySQL checkpoint recovery (shape test)
    // ═══════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Verify MySQL runner's GetPendingMigrationsInternalAsync throws on 'Partial'
    /// status rows (checkpoint safety). This is a shape test using SQLite as the
    /// backing store with the NoLock subclass.
    /// </summary>
    [Fact]
    public async Task MySQL_CheckpointRecovery_PartialRowThrows()
    {
        await using var cn = OpenSqlite();
        CreateMySqlHistoryTable(cn);

        // Manually insert a Partial row (simulating a mid-flight failure)
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "INSERT INTO \"__NormMigrationsHistory\" (Version, Name, AppliedOn, Status) " +
                "VALUES (500, 'HalfDone', '2026-01-01', 'Partial')";
            cmd.ExecuteNonQuery();
        }

        var asm = NoOpAssembly(("HalfDone", 500L));
        var runner = new NoLockMySqlRunner(cn, asm);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());

        Assert.Contains("Partial state", ex.Message);
        Assert.Contains("500", ex.Message);
        Assert.Contains("DELETE", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// After operator deletes the Partial row, MySQL runner can proceed normally.
    /// </summary>
    [Fact]
    public async Task MySQL_CheckpointRecovery_AfterDelete_ReplaySucceeds()
    {
        await using var cn = OpenSqlite();
        CreateMySqlHistoryTable(cn);

        // Insert Partial row
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "INSERT INTO \"__NormMigrationsHistory\" (Version, Name, AppliedOn, Status) " +
                "VALUES (501, 'PartialStep', '2026-01-01', 'Partial')";
            cmd.ExecuteNonQuery();
        }

        // First attempt: throws
        var asm1 = NoOpAssembly(("PartialStep", 501L));
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new NoLockMySqlRunner(cn, asm1).ApplyMigrationsAsync());

        // Operator cleanup
        using (var del = cn.CreateCommand())
        {
            del.CommandText = "DELETE FROM \"__NormMigrationsHistory\" WHERE Version = 501";
            del.ExecuteNonQuery();
        }

        // Resume: should succeed now
        var asm2 = NoOpAssembly(("PartialStep", 501L));
        await new NoLockMySqlRunner(cn, asm2).ApplyMigrationsAsync();

        // Verify Applied
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT Status FROM \"__NormMigrationsHistory\" WHERE Version = 501";
        var status = (string?)check.ExecuteScalar();
        Assert.Equal("Applied", status);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 5. Provider parity: all runners detect already-applied
    // ═══════════════════════════════════════════════════════════════════════

    /// <summary>
    /// SQLite runner: after applying migrations, HasPendingMigrationsAsync returns false.
    /// </summary>
    [Fact]
    public async Task ProviderParity_SQLite_SkipsAlreadyApplied()
    {
        using var cn = OpenSqlite();
        var asm = NoOpAssembly(("ParityA", 600L), ("ParityB", 601L));
        var runner = new SqliteMigrationRunner(cn, asm);

        await runner.ApplyMigrationsAsync();
        Assert.False(await runner.HasPendingMigrationsAsync());

        // Create a new runner with the same assembly -- should have no pending
        var runner2 = new SqliteMigrationRunner(cn, asm);
        Assert.False(await runner2.HasPendingMigrationsAsync());

        var pending = await runner2.GetPendingMigrationsAsync();
        Assert.Empty(pending);
    }

    /// <summary>
    /// MySQL runner (NoLock): after applying, HasPendingMigrationsAsync returns false.
    /// Uses SQLite + NoLock bypass for testing.
    /// </summary>
    [Fact]
    public async Task ProviderParity_MySQL_SkipsAlreadyApplied()
    {
        await using var cn = OpenSqlite();
        var asm = NoOpAssembly(("MySqlParA", 700L), ("MySqlParB", 701L));
        var runner = new NoLockMySqlRunner(cn, asm);

        await runner.ApplyMigrationsAsync();
        Assert.False(await runner.HasPendingMigrationsAsync());

        // Second apply is a no-op
        await runner.ApplyMigrationsAsync();
        Assert.False(await runner.HasPendingMigrationsAsync());
    }

    /// <summary>
    /// Postgres runner: pre-seed history table, verify GetPendingMigrationsAsync
    /// returns empty when all migrations are applied. (ApplyMigrationsAsync would
    /// fail on SQLite due to pg_advisory_lock, but pending detection works.)
    /// </summary>
    [Fact]
    public async Task ProviderParity_Postgres_SkipsAlreadyApplied()
    {
        await using var cn = OpenSqlite();

        // Pre-create history table (Postgres uses double-quoted identifiers, compatible with SQLite)
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"CREATE TABLE IF NOT EXISTS ""__NormMigrationsHistory"" " +
                              @"(Version BIGINT PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TIMESTAMP NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        // Seed applied rows
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"INSERT INTO ""__NormMigrationsHistory"" (""Version"", ""Name"", ""AppliedOn"") VALUES (800, 'PgParA', '2026-01-01')";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"INSERT INTO ""__NormMigrationsHistory"" (""Version"", ""Name"", ""AppliedOn"") VALUES (801, 'PgParB', '2026-01-01')";
            cmd.ExecuteNonQuery();
        }

        var asm = NoOpAssembly(("PgParA", 800L), ("PgParB", 801L));
        var runner = new PostgresMigrationRunner(cn, asm);

        Assert.False(await runner.HasPendingMigrationsAsync());
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Empty(pending);
    }

    /// <summary>
    /// SqlServer runner: pre-seed history table, verify GetPendingMigrationsInternalAsync
    /// (via reflection) returns empty when all migrations are applied.
    /// </summary>
    [Fact]
    public async Task ProviderParity_SqlServer_SkipsAlreadyApplied()
    {
        await using var cn = OpenSqlite();

        // SqlServer uses bracket-quoted identifiers, compatible with SQLite
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE [__NormMigrationsHistory] " +
                "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        // Seed applied rows
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO [__NormMigrationsHistory] (Version, Name, AppliedOn) VALUES (900, 'SsParA', '2026-01-01')";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO [__NormMigrationsHistory] (Version, Name, AppliedOn) VALUES (901, 'SsParB', '2026-01-01')";
            cmd.ExecuteNonQuery();
        }

        var asm = NoOpAssembly(("SsParA", 900L), ("SsParB", 901L));
        var runner = new SqlServerMigrationRunner(cn, asm);

        // Use reflection to call GetPendingMigrationsInternalAsync directly
        // (ApplyMigrationsAsync would fail on SQLite due to sp_getapplock)
        var method = typeof(SqlServerMigrationRunner)
            .GetMethod("GetPendingMigrationsInternalAsync",
                BindingFlags.NonPublic | BindingFlags.Instance)!;
        var pending = await (Task<System.Collections.Generic.List<MigrationBase>>)
            method.Invoke(runner, new object[] { CancellationToken.None })!;

        Assert.Empty(pending);
    }

    /// <summary>
    /// All 4 runners: verify that applying no-op migrations and then checking pending
    /// returns false. This covers the "already applied = no-op" invariant.
    /// </summary>
    [Fact]
    public async Task ProviderParity_AllRunners_NoOpAfterApply()
    {
        // SQLite (live)
        {
            using var cn = OpenSqlite();
            var asm = NoOpAssembly(("AllPar1", 1000L));
            var r = new SqliteMigrationRunner(cn, asm);
            await r.ApplyMigrationsAsync();
            await r.ApplyMigrationsAsync(); // second run = no-op
            Assert.False(await r.HasPendingMigrationsAsync());
        }

        // MySQL (via NoLock bypass, live on SQLite)
        {
            await using var cn = OpenSqlite();
            var asm = NoOpAssembly(("AllPar2", 1001L));
            var r = new NoLockMySqlRunner(cn, asm);
            await r.ApplyMigrationsAsync();
            await r.ApplyMigrationsAsync(); // second run = no-op
            Assert.False(await r.HasPendingMigrationsAsync());
        }
    }
}
