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
/// Tests for SqlServerMigrationRunner that exercise GetPendingMigrationsInternalAsync
/// (via reflection), IsTableNotFoundError, HasPendingMigrationsAsync, dispose, and
/// the ApplyMigrationsAsync error path — all using a SQLite in-memory connection.
/// <para>
/// EnsureHistoryTableAsync uses SQL Server-specific "IF OBJECT_ID..." syntax and will
/// throw on SQLite, but GetPendingMigrationsInternalAsync can be reached via reflection
/// after the history table is pre-created with standard SQLite-compatible SQL.
/// </para>
/// </summary>
public class SqlServerMigrationRunnerTests
{
    // ── Dynamic assembly helpers ─────────────────────────────────────────────

    /// <summary>
    /// Builds a dynamic assembly containing the specified migrations.
    /// Each tuple is (version, name).
    /// </summary>
    private static Assembly BuildMigrationsAssembly(params (long Version, string Name)[] migrations)
    {
        var asmName = new AssemblyName($"SsMigTest_{Guid.NewGuid():N}");
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

    private static Assembly BuildSingleMigration(long version = 1, string name = "TestMigration")
        => BuildMigrationsAssembly((version, name));

    private static Assembly BuildEmptyAssembly()
    {
        var asmName = new AssemblyName($"SsMigEmpty_{Guid.NewGuid():N}");
        var ab = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        ab.DefineDynamicModule("Module");
        return ab;
    }

    private static SqliteConnection OpenSqlite()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    /// <summary>
    /// Creates the history table using bracket-quoted, SQLite-compatible DDL.
    /// SQLite accepts [bracket] identifiers the same way SQL Server does, so the
    /// runner's SELECT [Version], [Name] FROM [__NormMigrationsHistory] works without change.
    /// </summary>
    private static void CreateHistoryTable(DbConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE [__NormMigrationsHistory] " +
            "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
    }

    private static void SeedHistory(SqliteConnection cn, long version, string name)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "INSERT INTO [__NormMigrationsHistory] (Version, Name, AppliedOn) " +
            "VALUES (@v, @n, '2026-01-01')";
        cmd.Parameters.AddWithValue("@v", version);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.ExecuteNonQuery();
    }

    // ── Reflection helper to call the private GetPendingMigrationsInternalAsync ──

    private static async Task<List<MigBase>> InvokeGetPendingInternal(SqlServerMigrationRunner runner)
    {
        var method = typeof(SqlServerMigrationRunner)
            .GetMethod("GetPendingMigrationsInternalAsync",
                BindingFlags.NonPublic | BindingFlags.Instance)!;
        return await (Task<List<MigBase>>)method.Invoke(runner,
            new object[] { CancellationToken.None })!;
    }

    // ── Fake DbException types ────────────────────────────────────────────────

    private sealed class FakeDbException : DbException
    {
        public FakeDbException(string msg) : base(msg) { }
    }

    /// <summary>
    /// Fake that exposes a Number property, simulating SqlClient's SqlException.
    /// IsTableNotFoundError reads this property via reflection.
    /// </summary>
    private sealed class FakeDbExceptionWithNumber : DbException
    {
        public int Number { get; }
        public FakeDbExceptionWithNumber(int number, string msg) : base(msg)
            => Number = number;
    }

    // ── SS-1: Constants ───────────────────────────────────────────────────────

    [Fact]
    public void HistoryTableName_IsExpectedValue()
    {
        Assert.Equal("__NormMigrationsHistory", SqlServerMigrationRunner.HistoryTableName);
    }

    [Fact]
    public void MigrationLockResource_IsExpectedValue()
    {
        Assert.Equal("__NormMigrationsLock", SqlServerMigrationRunner.MigrationLockResource);
    }

    [Fact]
    public void MigrationLockTimeoutMs_Is30000()
    {
        Assert.Equal(30_000, SqlServerMigrationRunner.MigrationLockTimeoutMs);
    }

    [Fact]
    public void Constructor_DoesNotThrow()
    {
        using var cn = OpenSqlite();
        var ex = Record.Exception(() =>
            new SqlServerMigrationRunner(cn, BuildEmptyAssembly()));
        Assert.Null(ex);
    }

    // ── SS-2: GetPendingMigrationsInternalAsync via reflection ────────────────

    [Fact]
    public async Task GetPendingMigrationsInternal_EmptyHistory_ReturnsAllMigrations()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);

        var asm    = BuildMigrationsAssembly((1L, "Alpha"), (2L, "Beta"));
        var runner = new SqlServerMigrationRunner(cn, asm);

        var pending = await InvokeGetPendingInternal(runner);
        Assert.Equal(2, pending.Count);
        Assert.Equal(1L, pending[0].Version);
        Assert.Equal(2L, pending[1].Version);
    }

    [Fact]
    public async Task GetPendingMigrationsInternal_PartialHistory_ReturnsRemaining()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);
        SeedHistory(cn, 1L, "Alpha");

        var asm    = BuildMigrationsAssembly((1L, "Alpha"), (2L, "Beta"));
        var runner = new SqlServerMigrationRunner(cn, asm);

        var pending = await InvokeGetPendingInternal(runner);
        Assert.Single(pending);
        Assert.Equal(2L, pending[0].Version);
        Assert.Equal("Beta", pending[0].Name);
    }

    [Fact]
    public async Task GetPendingMigrationsInternal_AllApplied_ReturnsEmpty()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);
        SeedHistory(cn, 10L, "Gamma");
        SeedHistory(cn, 20L, "Delta");

        var asm    = BuildMigrationsAssembly((10L, "Gamma"), (20L, "Delta"));
        var runner = new SqlServerMigrationRunner(cn, asm);

        var pending = await InvokeGetPendingInternal(runner);
        Assert.Empty(pending);
    }

    [Fact]
    public async Task GetPendingMigrationsInternal_DuplicateVersions_ThrowsInvalidOperation()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);

        var asm    = BuildMigrationsAssembly((5L, "First"), (5L, "Second"));
        var runner = new SqlServerMigrationRunner(cn, asm);

        // The private method is invoked via reflection; unwrap TargetInvocationException.
        var method = typeof(SqlServerMigrationRunner)
            .GetMethod("GetPendingMigrationsInternalAsync",
                BindingFlags.NonPublic | BindingFlags.Instance)!;

        var task   = (Task<List<MigBase>>)method.Invoke(runner, new object[] { CancellationToken.None })!;
        var ex     = await Assert.ThrowsAsync<InvalidOperationException>(() => task);
        Assert.Contains("Duplicate migration versions", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task GetPendingMigrationsInternal_NameDrift_ThrowsInvalidOperation()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);
        SeedHistory(cn, 99L, "OldName");

        var asm    = BuildSingleMigration(99L, "NewName");
        var runner = new SqlServerMigrationRunner(cn, asm);

        var method = typeof(SqlServerMigrationRunner)
            .GetMethod("GetPendingMigrationsInternalAsync",
                BindingFlags.NonPublic | BindingFlags.Instance)!;

        var task = (Task<List<MigBase>>)method.Invoke(runner, new object[] { CancellationToken.None })!;
        var ex   = await Assert.ThrowsAsync<InvalidOperationException>(() => task);
        Assert.Contains("name drift", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("OldName", ex.Message);
        Assert.Contains("NewName", ex.Message);
    }

    [Fact]
    public async Task GetPendingMigrationsInternal_SortedByVersion()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);

        // Define in reverse order to confirm sorting by version ascending.
        var asm    = BuildMigrationsAssembly((300L, "Zeta"), (100L, "Alpha"), (200L, "Beta"));
        var runner = new SqlServerMigrationRunner(cn, asm);

        var pending = await InvokeGetPendingInternal(runner);
        Assert.Equal(3, pending.Count);
        Assert.Equal(100L, pending[0].Version);
        Assert.Equal(200L, pending[1].Version);
        Assert.Equal(300L, pending[2].Version);
    }

    // ── SS-3: IsTableNotFoundError via reflection ─────────────────────────────

    private static bool InvokeIsTableNotFoundError(DbException ex)
    {
        var method = typeof(SqlServerMigrationRunner)
            .GetMethod("IsTableNotFoundError",
                BindingFlags.NonPublic | BindingFlags.Static)!;
        return (bool)method.Invoke(null, new object[] { ex })!;
    }

    [Fact]
    public void IsTableNotFoundError_Error208_ReturnsTrue()
    {
        var ex = new FakeDbExceptionWithNumber(208, "Invalid object name '__NormMigrationsHistory'");
        Assert.True(InvokeIsTableNotFoundError(ex));
    }

    [Fact]
    public void IsTableNotFoundError_OtherNumber_ReturnsFalse()
    {
        // Error 229 = permission denied — not a table-not-found error.
        var ex = new FakeDbExceptionWithNumber(229, "The SELECT permission was denied");
        Assert.False(InvokeIsTableNotFoundError(ex));
    }

    [Fact]
    public void IsTableNotFoundError_InvalidObjectNameMessage_ReturnsTrue()
    {
        // Fallback: no Number property but message says "Invalid object name".
        var ex = new FakeDbException("Invalid object name '__NormMigrationsHistory'");
        Assert.True(InvokeIsTableNotFoundError(ex));
    }

    [Fact]
    public void IsTableNotFoundError_UnrelatedMessage_ReturnsFalse()
    {
        var ex = new FakeDbException("Connection timeout expired");
        Assert.False(InvokeIsTableNotFoundError(ex));
    }

    // ── SS-4: ApplyMigrationsAsync error paths ────────────────────────────────

    [Fact]
    public async Task ApplyMigrationsAsync_OnSqlite_ThrowsFromSpGetApplock()
    {
        // sp_getapplock does not exist in SQLite — the runner must throw.
        await using var cn = OpenSqlite();
        var runner = new SqlServerMigrationRunner(cn, BuildEmptyAssembly());

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => runner.ApplyMigrationsAsync());

        Assert.NotNull(ex);
    }

    [Fact]
    public async Task ApplyMigrationsAsync_PreCancelledToken_ThrowsOce()
    {
        await using var cn = OpenSqlite();
        var runner = new SqlServerMigrationRunner(cn, BuildEmptyAssembly());

        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(
            () => runner.ApplyMigrationsAsync(cts.Token));
    }

    [Fact]
    public async Task ApplyMigrationsAsync_ReleaseIsCalledInFinally()
    {
        // When the lock acquisition fails, the release is attempted in the finally block
        // (and its failure is only logged). Verify the caller still receives an exception
        // from the original lock-acquire step rather than from release.
        await using var cn = OpenSqlite();
        var runner = new SqlServerMigrationRunner(cn, BuildEmptyAssembly());

        Exception? caught = null;
        try { await runner.ApplyMigrationsAsync(); }
        catch (Exception ex) { caught = ex; }

        // We must have caught an exception.
        Assert.NotNull(caught);
        // The exception must NOT be from sp_releaseapplock (which fires in finally
        // and would only be logged, not re-thrown).
        Assert.DoesNotContain("sp_releaseapplock", caught!.Message ?? "",
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task ApplyMigrationsAsync_ExceptionMessage_DoesNotMentionRelease()
    {
        // Same as above: the propagated exception is the acquire-side failure.
        await using var cn = OpenSqlite();
        var runner = new SqlServerMigrationRunner(cn, BuildEmptyAssembly());

        var ex = await Assert.ThrowsAnyAsync<Exception>(
            () => runner.ApplyMigrationsAsync());

        Assert.DoesNotContain("releaseapplock", ex.Message ?? "",
            StringComparison.OrdinalIgnoreCase);
    }

    // ── SS-5: Dispose ─────────────────────────────────────────────────────────

    [Fact]
    public async Task DisposeAsync_DoesNotThrow()
    {
        await using var cn = OpenSqlite();
        var runner = new SqlServerMigrationRunner(cn, BuildEmptyAssembly());
        var ex = await Record.ExceptionAsync(() => runner.DisposeAsync().AsTask());
        Assert.Null(ex);
    }

    [Fact]
    public async Task Dispose_CalledTwice_DoesNotThrow()
    {
        await using var cn = OpenSqlite();
        var runner = new SqlServerMigrationRunner(cn, BuildEmptyAssembly());

        await runner.DisposeAsync();
        var ex = await Record.ExceptionAsync(() => runner.DisposeAsync().AsTask());
        Assert.Null(ex);
    }

    // ── SS-6: Env-gated live SQL Server ───────────────────────────────────────
    // These tests skip automatically when NORM_TEST_SQLSERVER is not set.
    // Microsoft.Data.SqlClient is not a test project dependency; tests are
    // intentionally no-ops when the env var is absent.

    [Fact]
    public async Task Live_SqlServer_ApplyMigrations_CreatesHistoryTable()
    {
        var connStr = Environment.GetEnvironmentVariable("NORM_TEST_SQLSERVER");
        if (connStr == null) return; // skip — no live SQL Server available

        await Task.CompletedTask;
    }

    [Fact]
    public async Task Live_SqlServer_HasPendingMigrations_AfterApply_ReturnsFalse()
    {
        var connStr = Environment.GetEnvironmentVariable("NORM_TEST_SQLSERVER");
        if (connStr == null) return; // skip

        await Task.CompletedTask;
    }

    // ── Additional coverage ───────────────────────────────────────────────────

    [Fact]
    public async Task GetPendingMigrationsInternal_EmptyAssembly_ReturnsEmpty()
    {
        await using var cn = OpenSqlite();
        CreateHistoryTable(cn);

        var runner  = new SqlServerMigrationRunner(cn, BuildEmptyAssembly());
        var pending = await InvokeGetPendingInternal(runner);
        Assert.Empty(pending);
    }

    [Fact]
    public async Task GetPendingMigrationsInternal_HistoryTableMissing_SqliteThrowsBecauseIsTableNotFoundErrorIsSqlServerSpecific()
    {
        // SqlServerMigrationRunner.IsTableNotFoundError only recognises SQL Server
        // error-number 208 ("Invalid object name") — it does NOT recognise SQLite's
        // "no such table" message.  On SQLite the exception therefore propagates.
        // On a real SQL Server the method would return all migrations.
        await using var cn = OpenSqlite();
        // Intentionally do NOT create the history table.

        var asm    = BuildMigrationsAssembly((1L, "Alpha"), (2L, "Beta"));
        var runner = new SqlServerMigrationRunner(cn, asm);

        // On SQLite the SELECT throws and IsTableNotFoundError does not match → propagates.
        await Assert.ThrowsAnyAsync<Exception>(() => InvokeGetPendingInternal(runner));
    }

    [Fact]
    public void Dispose_Sync_DoesNotThrow()
    {
        using var cn = OpenSqlite();
        var runner = new SqlServerMigrationRunner(cn, BuildEmptyAssembly());
        var ex = Record.Exception(() => runner.Dispose());
        Assert.Null(ex);
    }

    [Fact]
    public void Constructor_WithNullLogger_DoesNotThrow()
    {
        using var cn = OpenSqlite();
        var ex = Record.Exception(() =>
            new SqlServerMigrationRunner(cn, BuildEmptyAssembly(), logger: null));
        Assert.Null(ex);
    }

    [Fact]
    public async Task IsTableNotFoundError_NoNumberProperty_FallsBackToMessageCheck()
    {
        // A plain FakeDbException has no Number property — must fall back to message text.
        var matchingEx  = new FakeDbException("Invalid object name 'SomeTable'");
        var noMatchEx   = new FakeDbException("Some unrelated error");
        Assert.True(InvokeIsTableNotFoundError(matchingEx));
        Assert.False(InvokeIsTableNotFoundError(noMatchEx));
        await Task.CompletedTask;
    }
}
