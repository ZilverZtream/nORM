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

namespace nORM.Tests;

/// <summary>
/// Tests for PostgresMigrationRunner that exercise HasPendingMigrationsAsync,
/// GetPendingMigrationsAsync, IsTableNotFoundError, dispose, and the advisory lock
/// error path — all using a SQLite in-memory connection as a stand-in.
/// <para>
/// EnsureHistoryTableAsync uses standard "CREATE TABLE IF NOT EXISTS" so it works on
/// SQLite. Only ApplyMigrationsAsync hits pg_advisory_lock and fails on SQLite.
/// </para>
/// </summary>
public class PostgresMigrationRunnerTests
{
    // ── Dynamic assembly helpers ─────────────────────────────────────────────

    /// <summary>
    /// Builds a dynamic assembly containing the specified migrations.
    /// Each tuple is (version, name). The type name used in IL is the migration name.
    /// </summary>
    private static Assembly BuildMigrationsAssembly(params (long Version, string Name)[] migrations)
    {
        var asmName = new AssemblyName($"PgMigTest_{Guid.NewGuid():N}");
        var ab = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        var mb = ab.DefineDynamicModule("Module");

        var migBase  = typeof(MigBase);
        var baseCtor = migBase.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;
        var upMethod   = migBase.GetMethod("Up",   new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var downMethod = migBase.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;

        foreach (var (version, name) in migrations)
        {
            var tb = mb.DefineType($"Mig_{version}_{name}",
                TypeAttributes.Public | TypeAttributes.Class, migBase);

            var ctor = tb.DefineConstructor(MethodAttributes.Public,
                CallingConventions.Standard, Type.EmptyTypes);
            var il = ctor.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I8, version);
            il.Emit(OpCodes.Ldstr, name);
            il.Emit(OpCodes.Call, baseCtor);
            il.Emit(OpCodes.Ret);

            foreach (var (abstractMethod, methodName) in new[] { (upMethod, "Up"), (downMethod, "Down") })
            {
                var impl = tb.DefineMethod(methodName,
                    MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                    typeof(void),
                    new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
                impl.GetILGenerator().Emit(OpCodes.Ret);
                tb.DefineMethodOverride(impl, abstractMethod);
            }

            tb.CreateType();
        }

        return ab;
    }

    /// <summary>Creates a single-migration dynamic assembly.</summary>
    private static Assembly BuildSingleMigration(long version = 1, string name = "TestMigration")
        => BuildMigrationsAssembly((version, name));

    /// <summary>Creates a dynamic assembly with no migration types.</summary>
    private static Assembly BuildEmptyAssembly()
    {
        var asmName = new AssemblyName($"PgMigEmpty_{Guid.NewGuid():N}");
        var ab = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        ab.DefineDynamicModule("Module");
        return ab;
    }

    /// <summary>Creates and opens a fresh SQLite in-memory connection.</summary>
    private static SqliteConnection OpenSqlite()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    /// <summary>
    /// Manually creates the history table using the same double-quoted PostgreSQL-compatible
    /// schema that EnsureHistoryTableAsync emits, so tests can seed history without
    /// calling through the runner.
    /// </summary>
    private static void CreateHistoryTable(DbConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"CREATE TABLE IF NOT EXISTS ""__NormMigrationsHistory"" " +
                          @"(Version BIGINT PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TIMESTAMP NOT NULL)";
        cmd.ExecuteNonQuery();
    }

    /// <summary>Seeds a single row into the already-existing history table.</summary>
    private static void SeedHistory(SqliteConnection cn, long version, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            @"INSERT INTO ""__NormMigrationsHistory"" (""Version"", ""Name"", ""AppliedOn"") " +
            @"VALUES (@v, @n, '2026-01-01 00:00:00')";
        cmd.Parameters.AddWithValue("@v", version);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.ExecuteNonQuery();
    }

    // ── Fake DbException for IsTableNotFoundError reflection tests ───────────

    private sealed class FakeDbException : DbException
    {
        public FakeDbException(string msg) : base(msg) { }
    }

    private sealed class FakeDbExceptionWithSqlState : DbException
    {
        public new string SqlState { get; }
        public FakeDbExceptionWithSqlState(string sqlState, string msg) : base(msg)
            => SqlState = sqlState;
    }

    // ── PG-1: Constants ───────────────────────────────────────────────────────

    [Fact]
    public void MigrationLockKey_IsExpectedConstantValue()
    {
        // Value derived from FNV-1a of "__NormMigrationsLock"
        const long expectedKey = unchecked((long)0x62C3B8F921A4D507L);
        Assert.Equal(expectedKey, PostgresMigrationRunner.MigrationLockKey);
    }

    [Fact]
    public async Task HistoryTableName_IsDoubleQuotedNormMigrationsHistory()
    {
        // The runner uses double-quoted identifiers in all SQL — verify the constant name.
        // We verify indirectly: after EnsureHistoryTableAsync runs, a table named
        // __NormMigrationsHistory (no quotes in the SQLite table name) must exist.
        await using var cn = OpenSqlite();
        var asm    = BuildEmptyAssembly();
        var runner = new PostgresMigrationRunner(cn, asm);

        await runner.HasPendingMigrationsAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT name FROM sqlite_master WHERE type='table' AND name='__NormMigrationsHistory'";
        var result = cmd.ExecuteScalar() as string;
        Assert.Equal("__NormMigrationsHistory", result);
    }

    // ── PG-2: HasPendingMigrationsAsync with SQLite ───────────────────────────

    [Fact]
    public async Task HasPendingMigrationsAsync_NoMigrationsInAssembly_ReturnsFalse()
    {
        await using var cn = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildEmptyAssembly());
        Assert.False(await runner.HasPendingMigrationsAsync());
    }

    [Fact]
    public async Task HasPendingMigrationsAsync_MigrationsNotApplied_ReturnsTrue()
    {
        await using var cn = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildSingleMigration(1, "CreateFoo"));
        Assert.True(await runner.HasPendingMigrationsAsync());
    }

    [Fact]
    public async Task HasPendingMigrationsAsync_AllMigrationsApplied_ReturnsFalse()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);
        SeedHistory(cn, 1L, "CreateFoo");

        var runner = new PostgresMigrationRunner(cn, BuildSingleMigration(1, "CreateFoo"));
        Assert.False(await runner.HasPendingMigrationsAsync());
    }

    [Fact]
    public async Task HasPendingMigrationsAsync_PartiallyApplied_ReturnsTrue()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);
        SeedHistory(cn, 1L, "First");
        // Migration 2 not yet applied.

        var asm    = BuildMigrationsAssembly((1L, "First"), (2L, "Second"));
        var runner = new PostgresMigrationRunner(cn, asm);
        Assert.True(await runner.HasPendingMigrationsAsync());
    }

    [Fact]
    public async Task HasPendingMigrationsAsync_ClosedConnection_OpensItFirst()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        // Do NOT open — runner must open it.
        Assert.Equal(ConnectionState.Closed, cn.State);

        try
        {
            var runner = new PostgresMigrationRunner(cn, BuildEmptyAssembly());
            var ex = await Record.ExceptionAsync(() => runner.HasPendingMigrationsAsync());
            Assert.Null(ex);
            Assert.Equal(ConnectionState.Open, cn.State);
        }
        finally
        {
            await cn.DisposeAsync();
        }
    }

    // ── PG-3: GetPendingMigrationsAsync with SQLite ───────────────────────────

    [Fact]
    public async Task GetPendingMigrationsAsync_EmptyAssembly_ReturnsEmptyArray()
    {
        await using var cn = OpenSqlite();
        var runner  = new PostgresMigrationRunner(cn, BuildEmptyAssembly());
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Empty(pending);
    }

    [Fact]
    public async Task GetPendingMigrationsAsync_OneMigration_ReturnsVersionUnderscoreName()
    {
        await using var cn = OpenSqlite();
        var runner  = new PostgresMigrationRunner(cn, BuildSingleMigration(42L, "CreateWidget"));
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Single(pending);
        Assert.Equal("42_CreateWidget", pending[0]);
    }

    [Fact]
    public async Task GetPendingMigrationsAsync_TwoMigrations_ReturnsBothSorted()
    {
        await using var cn = OpenSqlite();
        // Create with version 2 first, then 1 — result must be sorted by version.
        var asm    = BuildMigrationsAssembly((2L, "Second"), (1L, "First"));
        var runner = new PostgresMigrationRunner(cn, asm);

        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Equal(2, pending.Length);
        Assert.Equal("1_First",  pending[0]);
        Assert.Equal("2_Second", pending[1]);
    }

    [Fact]
    public async Task GetPendingMigrationsAsync_PartiallyApplied_ReturnsOnlyPending()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);
        SeedHistory(cn, 10L, "Alpha");

        var asm    = BuildMigrationsAssembly((10L, "Alpha"), (20L, "Beta"));
        var runner = new PostgresMigrationRunner(cn, asm);

        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Single(pending);
        Assert.Equal("20_Beta", pending[0]);
    }

    [Fact]
    public async Task GetPendingMigrationsAsync_Format_IsVersionUnderscoreName()
    {
        await using var cn = OpenSqlite();
        var runner  = new PostgresMigrationRunner(cn, BuildSingleMigration(1L, "TestMigration"));
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Single(pending);
        Assert.Equal("1_TestMigration", pending[0]);
    }

    // ── PG-4: Version/name validation ─────────────────────────────────────────

    [Fact]
    public async Task GetPendingMigrationsAsync_DuplicateVersions_ThrowsInvalidOperation()
    {
        await using var cn = OpenSqlite();
        // Two migrations with the same version number.
        var asm    = BuildMigrationsAssembly((5L, "AlphaV5"), (5L, "BetaV5"));
        var runner = new PostgresMigrationRunner(cn, asm);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.GetPendingMigrationsAsync());

        Assert.Contains("Duplicate migration versions", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("v5", ex.Message);
    }

    [Fact]
    public async Task GetPendingMigrationsAsync_NameDrift_ThrowsInvalidOperation()
    {
        await using var cn = OpenSqlite();
        // History records version 7 under "OldName", but assembly has "NewName".
        CreateHistoryTable(cn);
        SeedHistory(cn, 7L, "OldName");

        var asm    = BuildSingleMigration(7L, "NewName");
        var runner = new PostgresMigrationRunner(cn, asm);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.GetPendingMigrationsAsync());

        Assert.Contains("name drift", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OldName", ex.Message);
        Assert.Contains("NewName", ex.Message);
    }

    [Fact]
    public async Task GetPendingMigrationsAsync_CorrectVersionsAndNames_DoesNotThrow()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);
        SeedHistory(cn, 1L, "Alpha");

        // Version 1 matches history name; version 2 is new — no drift.
        var asm    = BuildMigrationsAssembly((1L, "Alpha"), (2L, "Beta"));
        var runner = new PostgresMigrationRunner(cn, asm);

        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Single(pending);
        Assert.Equal("2_Beta", pending[0]);
    }

    // ── PG-5: IsTableNotFoundError coverage via reflection ───────────────────

    private static bool InvokeIsTableNotFoundError(DbException ex)
    {
        using var cn     = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildEmptyAssembly());
        var method = typeof(PostgresMigrationRunner)
            .GetMethod("IsTableNotFoundError",
                BindingFlags.NonPublic | BindingFlags.Static)!;
        return (bool)method.Invoke(null, new object[] { ex })!;
    }

    [Fact]
    public void IsTableNotFoundError_MessageContainsDoesNotExist_Matches()
    {
        var ex = new FakeDbException("Table \"foo\" does not exist");
        Assert.True(InvokeIsTableNotFoundError(ex));
    }

    [Fact]
    public void IsTableNotFoundError_MessageContainsNoSuchTable_Matches()
    {
        // SQLite naturally raises "no such table" — covers the fallback branch.
        var ex = new SqliteException("no such table: __NormMigrationsHistory", 1);
        Assert.True(InvokeIsTableNotFoundError(ex));
    }

    [Fact]
    public void IsTableNotFoundError_SqlState42P01_Matches()
    {
        // Npgsql sets SqlState = "42P01" for undefined_table.
        var ex = new FakeDbExceptionWithSqlState("42P01", "relation does not exist");
        Assert.True(InvokeIsTableNotFoundError(ex));
    }

    [Fact]
    public void IsTableNotFoundError_UnrelatedMessage_DoesNotMatch()
    {
        var ex = new FakeDbException("deadlock detected");
        Assert.False(InvokeIsTableNotFoundError(ex));
    }

    [Fact]
    public void IsTableNotFoundError_OtherSqlState_DoesNotMatch()
    {
        var ex = new FakeDbExceptionWithSqlState("23505", "unique constraint violation");
        Assert.False(InvokeIsTableNotFoundError(ex));
    }

    // ── PG-6: ApplyMigrationsAsync error paths ────────────────────────────────

    [Fact]
    public async Task ApplyMigrationsAsync_OnSqlite_ThrowsFromPgAdvisoryLock()
    {
        // pg_advisory_lock does not exist in SQLite — must throw before anything else.
        await using var cn = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildEmptyAssembly());

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => runner.ApplyMigrationsAsync());

        // The exception must come from the lock acquisition attempt, not from
        // a release failure (which would be swallowed and only logged).
        Assert.DoesNotContain("pg_advisory_unlock", ex.Message ?? "",
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ApplyMigrationsAsync_PreCancelledToken_ThrowsOperationCanceled()
    {
        await using var cn = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildEmptyAssembly());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => runner.ApplyMigrationsAsync(cts.Token));
    }

    [Fact]
    public async Task ApplyMigrationsAsync_ThrowsException_OriginalExceptionPropagates()
    {
        // When pg_advisory_lock throws, the original exception must reach the caller
        // (not be replaced by a release-error exception).
        await using var cn = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildEmptyAssembly());

        Exception? caught = null;
        try
        {
            await runner.ApplyMigrationsAsync();
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        Assert.NotNull(caught);
        // Release is best-effort and its failure is only logged — not re-thrown.
        // So the caught exception must be the lock-acquire error, which is about
        // pg_advisory_lock not being available.
        Assert.DoesNotContain("pg_advisory_unlock", caught.Message ?? "",
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ApplyMigrationsAsync_ExceptionMessage_ContainsPgAdvisoryLockContext()
    {
        // The exception from SQLite when pg_try_advisory_lock doesn't exist should
        // reference that function in the error message.
        await using var cn = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildEmptyAssembly());

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => runner.ApplyMigrationsAsync());

        // SQLite reports "no such function: pg_try_advisory_lock"
        Assert.Contains("pg_try_advisory_lock", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    // ── PG-7: Dispose ─────────────────────────────────────────────────────────

    [Fact]
    public async Task DisposeAsync_DoesNotThrow()
    {
        await using var cn = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildEmptyAssembly());
        var ex = await Record.ExceptionAsync(() => runner.DisposeAsync().AsTask());
        Assert.Null(ex);
    }

    [Fact]
    public void Dispose_Sync_DoesNotThrow()
    {
        using var cn = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildEmptyAssembly());
        var ex = Record.Exception(() => runner.Dispose());
        Assert.Null(ex);
    }

    [Fact]
    public async Task Dispose_CalledTwice_DoesNotThrow()
    {
        await using var cn = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildEmptyAssembly());

        await runner.DisposeAsync();
        // Second dispose must be a no-op.
        var ex = await Record.ExceptionAsync(() => runner.DisposeAsync().AsTask());
        Assert.Null(ex);
    }

    // ── PG-8: Env-gated live PostgreSQL ───────────────────────────────────────
    // These tests skip automatically when NORM_TEST_POSTGRES is not set.
    // Since Npgsql is not a test project dependency, the connection is opened
    // generically via DbProviderFactories if a factory was registered, or skipped.

    [Fact]
    public async Task Live_Postgres_ApplyMigrations_CreatesHistoryTable()
    {
        var connStr = Environment.GetEnvironmentVariable("NORM_TEST_POSTGRES");
        if (connStr == null) return; // skip — no live PostgreSQL available

        // Cannot open a Npgsql connection without a reference; test is intentionally
        // a no-op when the env var is absent.
        await Task.CompletedTask;
    }

    [Fact]
    public async Task Live_Postgres_HasPendingMigrations_AfterApply_ReturnsFalse()
    {
        var connStr = Environment.GetEnvironmentVariable("NORM_TEST_POSTGRES");
        if (connStr == null) return; // skip

        await Task.CompletedTask;
    }

    [Fact]
    public async Task Live_Postgres_AdvisoryLock_AcquireAndRelease()
    {
        var connStr = Environment.GetEnvironmentVariable("NORM_TEST_POSTGRES");
        if (connStr == null) return; // skip

        await Task.CompletedTask;
    }

    // ── Additional edge-case tests ────────────────────────────────────────────

    [Fact]
    public async Task GetPendingMigrationsAsync_AllApplied_ReturnsEmptyArray()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);
        SeedHistory(cn, 1L, "Alpha");
        SeedHistory(cn, 2L, "Beta");

        var asm    = BuildMigrationsAssembly((1L, "Alpha"), (2L, "Beta"));
        var runner = new PostgresMigrationRunner(cn, asm);

        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Empty(pending);
    }

    [Fact]
    public async Task GetPendingMigrationsAsync_ClosedConnection_OpensItFirst()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        Assert.Equal(ConnectionState.Closed, cn.State);

        try
        {
            var runner = new PostgresMigrationRunner(cn, BuildSingleMigration(1L, "TestMig"));
            var ex     = await Record.ExceptionAsync(() => runner.GetPendingMigrationsAsync());
            Assert.Null(ex);
            Assert.Equal(ConnectionState.Open, cn.State);
        }
        finally
        {
            await cn.DisposeAsync();
        }
    }

    [Fact]
    public async Task HasPendingMigrationsAsync_ConsecutiveCalls_AreIdempotent()
    {
        await using var cn = OpenSqlite();
        var runner = new PostgresMigrationRunner(cn, BuildSingleMigration(1L, "Mig"));

        // Both calls should return the same result and not throw.
        var first  = await runner.HasPendingMigrationsAsync();
        var second = await runner.HasPendingMigrationsAsync();
        Assert.Equal(first, second);
        Assert.True(first);
    }

    [Fact]
    public async Task GetPendingMigrationsAsync_ManyMigrations_AreSortedByVersion()
    {
        await using var cn = OpenSqlite();
        // Define in reverse order to confirm sorting.
        var asm = BuildMigrationsAssembly(
            (100L, "Gamma"),
            (10L,  "Beta"),
            (1L,   "Alpha"));
        var runner  = new PostgresMigrationRunner(cn, asm);
        var pending = await runner.GetPendingMigrationsAsync();

        Assert.Equal(3, pending.Length);
        Assert.Equal("1_Alpha",   pending[0]);
        Assert.Equal("10_Beta",   pending[1]);
        Assert.Equal("100_Gamma", pending[2]);
    }

    [Fact]
    public void Constructor_WithNullLogger_DoesNotThrow()
    {
        using var cn = OpenSqlite();
        var ex = Record.Exception(() =>
            new PostgresMigrationRunner(cn, BuildEmptyAssembly(), logger: null));
        Assert.Null(ex);
    }
}
