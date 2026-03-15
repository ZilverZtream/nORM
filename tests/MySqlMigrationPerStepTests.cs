using System;
using System.Collections.Generic;
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
/// Verifies that MySqlMigrationRunner uses per-step transactions (one commit per
/// migration) rather than a single wrapping transaction across all migrations.
///
/// Root bug: A single transaction wrapped all migrations. MySQL DDL (ALTER TABLE,
/// CREATE TABLE, etc.) auto-commits, so a rollback on failure could not undo schema
/// changes from earlier migrations, but DID undo the history INSERT for those earlier
/// migrations — leaving schema ahead of history and causing re-application on the
/// next run.
///
/// Fix: Each migration runs in its own transaction: Up() + MarkApplied + Commit.
/// On failure, earlier migrations are already individually committed (both DDL and
/// history). Only the failing migration's history is not recorded.
///
/// Tests use a SQLite-backed counting wrapper (since MySQL is not available in CI)
/// to verify the commit-per-step invariant structurally.
/// Migration types are built in a dynamic assembly so they don't appear in the test
/// assembly scan used by SqliteMigrationRunnerTests (which would cause spurious failures).
/// </summary>
public class MySqlMigrationPerStepTests
{
    // ── Counting transaction wrapper ─────────────────────────────────────────

    /// <summary>
    /// Wraps a real SQLite DbTransaction and counts how many times Commit is called.
    /// Used to verify the per-step commit invariant without a live MySQL server.
    /// </summary>
    private sealed class CountingTransaction : DbTransaction
    {
        private readonly DbTransaction _inner;
        private readonly List<int> _commitLog;
        private readonly int _commitIndex;

        public CountingTransaction(DbTransaction inner, List<int> commitLog, int index)
        {
            _inner = inner;
            _commitLog = commitLog;
            _commitIndex = index;
        }

        public override IsolationLevel IsolationLevel => _inner.IsolationLevel;
        protected override DbConnection DbConnection => _inner.Connection!;

        // Exposes the real underlying transaction so ProxyCommand can forward it to the
        // inner SQLite command, which would throw InvalidCastException if given a CountingTransaction.
        internal DbTransaction Inner => _inner;

        public override void Commit() { _commitLog.Add(_commitIndex); _inner.Commit(); }
        public override void Rollback() { _inner.Rollback(); }

        public override Task CommitAsync(CancellationToken ct = default)
        {
            _commitLog.Add(_commitIndex);
            return _inner.CommitAsync(ct);
        }

        public override Task RollbackAsync(CancellationToken ct = default)
            => _inner.RollbackAsync(ct);

        protected override void Dispose(bool disposing) { if (disposing) _inner.Dispose(); }
    }

    /// <summary>
    /// Connection wrapper that injects CountingTransaction on BeginTransaction.
    /// Allows verifying how many times CommitAsync is called without a real MySQL server.
    /// </summary>
    private sealed class CountingConnection : DbConnection
    {
        private readonly DbConnection _inner;
        private readonly List<int> _commitLog = new();
        private int _txCount;

        public IReadOnlyList<int> CommitLog => _commitLog;

        public CountingConnection(DbConnection inner) => _inner = inner;

        public override string ConnectionString
        {
            get => _inner.ConnectionString;
            set => _inner.ConnectionString = value!;
        }
        public override string Database => _inner.Database;
        public override string DataSource => _inner.DataSource;
        public override string ServerVersion => _inner.ServerVersion;
        public override ConnectionState State => _inner.State;

        public override void Open() => _inner.Open();
        public override Task OpenAsync(CancellationToken ct) => _inner.OpenAsync(ct);
        public override void Close() => _inner.Close();
        public override void ChangeDatabase(string db) => _inner.ChangeDatabase(db);
        protected override DbCommand CreateDbCommand()
        {
            var cmd = _inner.CreateCommand();
            return new ProxyCommand(cmd, this);
        }

        protected override DbTransaction BeginDbTransaction(IsolationLevel il)
        {
            var inner = _inner.BeginTransaction(il);
            return new CountingTransaction(inner, _commitLog, _txCount++);
        }

        protected override ValueTask<DbTransaction> BeginDbTransactionAsync(
            IsolationLevel il, CancellationToken ct)
        {
            var inner = _inner.BeginTransaction(il);
            DbTransaction tx = new CountingTransaction(inner, _commitLog, _txCount++);
            return new ValueTask<DbTransaction>(tx);
        }

        // Simple command proxy to forward to real connection but bind to CountingConnection
        private sealed class ProxyCommand : DbCommand
        {
            private readonly DbCommand _inner;
            private readonly DbConnection _owner;
            public ProxyCommand(DbCommand inner, DbConnection owner) { _inner = inner; _owner = owner; }
            public override string CommandText { get => _inner.CommandText; set => _inner.CommandText = value; }
            public override int CommandTimeout { get => _inner.CommandTimeout; set => _inner.CommandTimeout = value; }
            public override CommandType CommandType { get => _inner.CommandType; set => _inner.CommandType = value; }
            public override bool DesignTimeVisible { get => _inner.DesignTimeVisible; set => _inner.DesignTimeVisible = value; }
            public override UpdateRowSource UpdatedRowSource { get => _inner.UpdatedRowSource; set => _inner.UpdatedRowSource = value; }
            protected override DbConnection? DbConnection { get => _owner; set { } }
            protected override DbParameterCollection DbParameterCollection => _inner.Parameters;
            protected override DbTransaction? DbTransaction
            {
                get => _inner.Transaction;
                // Unwrap CountingTransaction so the inner SQLite command receives the real
                // SqliteTransaction; without this, SQLite throws InvalidCastException.
                set => _inner.Transaction = (value is CountingTransaction ct ? ct.Inner : value)!;
            }
            public override void Cancel() => _inner.Cancel();
            public override int ExecuteNonQuery() => _inner.ExecuteNonQuery();
            public override Task<int> ExecuteNonQueryAsync(CancellationToken ct) => _inner.ExecuteNonQueryAsync(ct);
            public override object? ExecuteScalar() => _inner.ExecuteScalar();
            public override void Prepare() => _inner.Prepare();
            protected override DbParameter CreateDbParameter() => _inner.CreateParameter();
            protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => _inner.ExecuteReader(behavior);
            protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken ct)
                => _inner.ExecuteReaderAsync(behavior, ct);
        }
    }

    // ── Tests ────────────────────────────────────────────────────────────────

    /// <summary>
    /// P-1 structural test: MySqlMigrationRunner must issue one CommitAsync per migration,
    /// not one commit for all migrations at the end.
    ///
    /// We use a CountingConnection (SQLite-backed) to intercept BeginTransaction and
    /// track how many commits are made. With 2 migrations, we expect exactly 2 commits.
    ///
    /// Before the fix: 1 commit at the end regardless of migration count.
    /// After the fix: N commits for N migrations (per-step durability).
    /// </summary>
    [Fact]
    public async Task ApplyMigrations_TwoMigrations_IssuesTwoCommits()
    {
        var sqlite = new SqliteConnection("Data Source=:memory:");
        await sqlite.OpenAsync();

        // Create history table manually (MySqlMigrationRunner uses backtick syntax, which
        // SQLite doesn't support, so we pre-create it with standard syntax for the test).
        using (var setup = sqlite.CreateCommand())
        {
            setup.CommandText =
                "CREATE TABLE \"__NormMigrationsHistory\" " +
                "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL);";
            setup.ExecuteNonQuery();
        }

        await using var counting = new CountingConnection(sqlite);

        // Build a dynamic assembly containing exactly 2 no-op migration types.
        // Dynamic assembly avoids polluting the test assembly (which SqliteMigrationRunner
        // tests also scan), preventing FailingMigration from breaking unrelated tests.
        var testAsm = BuildDynamicMigrationAssembly(
            (1000L, "DynStep1", false),
            (1001L, "DynStep2", false));

        var runner = new MySqlMigrationRunner(counting, testAsm);

        await runner.ApplyMigrationsAsync();

        // P-1 fix: 2 migrations → 2 commits (one per step), not 1 at the end.
        Assert.Equal(2, counting.CommitLog.Count);
        // Commits must be in step order (step 0 first, step 1 second).
        Assert.Equal(new[] { 0, 1 }, counting.CommitLog);
    }

    /// <summary>
    /// When the third of three migrations fails, the first two must have been
    /// individually committed (schema applied + history recorded). Only the third is absent.
    ///
    /// Before the fix: a single wrapping rollback would remove history for all prior
    /// migrations even though their DDL had already auto-committed in MySQL.
    /// After the fix: each migration's commit is independent; failure in step 3 leaves
    /// steps 1 and 2 durably applied.
    /// </summary>
    [Fact]
    public async Task ApplyMigrations_ThirdMigrationFails_FirstTwoHistoryRecorded()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // Pre-create history table with SQLite syntax.
        using (var setup = cn.CreateCommand())
        {
            setup.CommandText =
                "CREATE TABLE \"__NormMigrationsHistory\" " +
                "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL);";
            setup.ExecuteNonQuery();
        }

        var testAsm = BuildDynamicMigrationAssembly(
            (1000L, "DynStep1", false),
            (1001L, "DynStep2", false),
            (1002L, "DynFailing", true));

        var runner = new MySqlMigrationRunner(cn, testAsm);

        // Third migration throws — runner must propagate the exception.
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());

        Assert.Contains("P-1 simulated migration failure", ex.Message);

        // P-1 fix: steps 1 and 2 were individually committed BEFORE step 3 ran.
        // History must contain exactly 2 entries (for versions 1000 and 1001).
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"";
        var historyCount = Convert.ToInt64(check.ExecuteScalar());
        Assert.Equal(2L, historyCount);
    }

    // ── P-1 replay safety, idempotency, crash-safe reconciliation ────────────

    /// <summary>
    /// P-1 idempotency: running ApplyMigrationsAsync on a database where all migrations
    /// are already recorded in history must be a no-op — zero commits, zero new history rows.
    ///
    /// Before the fix: even a no-op run might start a transaction unnecessarily.
    /// After the fix: the pending list is empty → the foreach body never executes → no commits.
    /// </summary>
    [Fact]
    public async Task ApplyMigrations_AllAlreadyApplied_IsNoOp()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        using (var setup = cn.CreateCommand())
        {
            setup.CommandText =
                "CREATE TABLE \"__NormMigrationsHistory\" " +
                "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL);";
            setup.ExecuteNonQuery();
        }

        await using var counting = new CountingConnection(cn);
        var testAsm = BuildDynamicMigrationAssembly(
            (1000L, "DynStep1", false),
            (1001L, "DynStep2", false));

        // First run: apply both migrations.
        var runner1 = new MySqlMigrationRunner(counting, testAsm);
        await runner1.ApplyMigrationsAsync();
        var firstRunCommits = counting.CommitLog.Count;

        // Second run: all migrations already recorded → must be a no-op.
        var runner2 = new MySqlMigrationRunner(counting, testAsm);
        await runner2.ApplyMigrationsAsync();

        // No new commits since the first run ended.
        Assert.Equal(firstRunCommits, counting.CommitLog.Count);

        // History must still have exactly 2 rows — not duplicated.
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"";
        Assert.Equal(2L, Convert.ToInt64(check.ExecuteScalar()));
    }

    /// <summary>
    /// P-1 replay safety: after a partial failure (third migration fails, only 2 history rows),
    /// a second run with a fixed (non-failing) assembly for the third migration must apply
    /// ONLY the third migration — not reapply the first two.
    ///
    /// This validates the per-step history model: once a migration is committed to history,
    /// it is skipped on the next run regardless of failure in later steps.
    /// </summary>
    [Fact]
    public async Task ApplyMigrations_AfterPartialFailure_ReplaySkipsAlreadyApplied()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        using (var setup = cn.CreateCommand())
        {
            setup.CommandText =
                "CREATE TABLE \"__NormMigrationsHistory\" " +
                "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL);";
            setup.ExecuteNonQuery();
        }

        // Run 1: third migration fails. Steps 1 and 2 committed individually to history.
        var failingAsm = BuildDynamicMigrationAssembly(
            (1000L, "DynStep1", false),
            (1001L, "DynStep2", false),
            (1002L, "DynFailing", true));

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => new MySqlMigrationRunner(cn, failingAsm).ApplyMigrationsAsync());

        // Run 2: same 3 migration versions, but step 3 now succeeds.
        await using var counting = new CountingConnection(cn);
        var fixedAsm = BuildDynamicMigrationAssembly(
            (1000L, "DynStep1", false),
            (1001L, "DynStep2", false),
            (1002L, "DynStep3Fixed", false));

        var runner2 = new MySqlMigrationRunner(counting, fixedAsm);
        await runner2.ApplyMigrationsAsync();

        // P-1 replay safety: only 1 commit on the second run (step 3 only, not steps 1+2).
        Assert.Single(counting.CommitLog);

        // History must now have exactly 3 rows total.
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"";
        Assert.Equal(3L, Convert.ToInt64(check.ExecuteScalar()));
    }

    /// <summary>
    /// P-1 crash-safe reconciliation: simulates a crash between Up() committing and
    /// MarkMigrationApplied completing (i.e. history write failed / process killed).
    ///
    /// Precondition: manually seed history with only steps 1 and 2 (as if step 3's
    /// history INSERT was rolled back after its DDL committed — the MySQL DDL-autocommit
    /// failure scenario). On the next run, step 3 must be retried because it is absent
    /// from history, even though its DDL may already be applied on the DB server.
    ///
    /// Idempotent Up() (CREATE TABLE IF NOT EXISTS, etc.) handles the DDL-already-present
    /// case gracefully; this test verifies the runner retries step 3.
    /// </summary>
    [Fact]
    public async Task ApplyMigrations_HistoryMissingForAppliedStep_StepRetried()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        using (var setup = cn.CreateCommand())
        {
            // Pre-create history table and manually record steps 1 and 2,
            // leaving step 3 absent (simulating crash after step 3 DDL but before history write).
            setup.CommandText =
                "CREATE TABLE \"__NormMigrationsHistory\" " +
                "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL);" +
                "INSERT INTO \"__NormMigrationsHistory\" VALUES (1000, 'DynStep1', '2025-01-01');" +
                "INSERT INTO \"__NormMigrationsHistory\" VALUES (1001, 'DynStep2', '2025-01-01');";
            setup.ExecuteNonQuery();
        }

        await using var counting = new CountingConnection(cn);
        // All three migration versions exist; only step 3 is missing from history.
        var testAsm = BuildDynamicMigrationAssembly(
            (1000L, "DynStep1", false),
            (1001L, "DynStep2", false),
            (1002L, "DynStep3", false));

        var runner = new MySqlMigrationRunner(counting, testAsm);
        await runner.ApplyMigrationsAsync();

        // P-1 crash-safe: only step 3 was pending → exactly 1 commit on this run.
        Assert.Single(counting.CommitLog);

        // History now has all 3 entries.
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"";
        Assert.Equal(3L, Convert.ToInt64(check.ExecuteScalar()));
    }

    // ── Helper: build a dynamic assembly containing the requested migration stubs ─

    /// <summary>
    /// Emits a transient in-memory assembly containing concrete Migration subclasses.
    /// Each entry specifies (version, name, throwOnUp). When throwOnUp=true, Up() throws
    /// InvalidOperationException("P-1 simulated migration failure"); otherwise Up() is a no-op.
    /// Using a dynamic assembly (not the test assembly) prevents these stub types from
    /// being discovered by SqliteMigrationRunner tests that scan the test assembly.
    /// </summary>
    private static Assembly BuildDynamicMigrationAssembly(
        params (long Version, string Name, bool ThrowOnUp)[] specs)
    {
        var asmName = new AssemblyName($"P1DynMigrations_{Guid.NewGuid():N}");
        var asmBuilder = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        var modBuilder = asmBuilder.DefineDynamicModule("Module");

        var migBase = typeof(MigrationBase);
        // protected Migration(long version, string name)
        var baseCtor = migBase.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance, null,
            new[] { typeof(long), typeof(string) }, null)!;

        var upMethod   = migBase.GetMethod("Up",   new[] { typeof(DbConnection), typeof(DbTransaction) })!;
        var downMethod = migBase.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction) })!;

        var throwCtor = typeof(InvalidOperationException)
            .GetConstructor(new[] { typeof(string) })!;

        foreach (var (version, name, throwOnUp) in specs)
        {
            var tb = modBuilder.DefineType(
                name,
                TypeAttributes.Public | TypeAttributes.Class,
                migBase);

            // ── Constructor ──────────────────────────────────────────────────
            var ctorBuilder = tb.DefineConstructor(
                MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
            var ctorIL = ctorBuilder.GetILGenerator();
            ctorIL.Emit(OpCodes.Ldarg_0);
            ctorIL.Emit(OpCodes.Ldc_I8, version);
            ctorIL.Emit(OpCodes.Ldstr, name);
            ctorIL.Emit(OpCodes.Call, baseCtor);
            ctorIL.Emit(OpCodes.Ret);

            // ── Up() ─────────────────────────────────────────────────────────
            var upBuilder = tb.DefineMethod(
                "Up",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void),
                new[] { typeof(DbConnection), typeof(DbTransaction) });
            var upIL = upBuilder.GetILGenerator();
            if (throwOnUp)
            {
                upIL.Emit(OpCodes.Ldstr, "P-1 simulated migration failure");
                upIL.Emit(OpCodes.Newobj, throwCtor);
                upIL.Emit(OpCodes.Throw);
            }
            else
            {
                upIL.Emit(OpCodes.Ret);
            }
            tb.DefineMethodOverride(upBuilder, upMethod);

            // ── Down() ───────────────────────────────────────────────────────
            var downBuilder = tb.DefineMethod(
                "Down",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void),
                new[] { typeof(DbConnection), typeof(DbTransaction) });
            var downIL = downBuilder.GetILGenerator();
            downIL.Emit(OpCodes.Ret);
            tb.DefineMethodOverride(downBuilder, downMethod);

            tb.CreateType();
        }

        return asmBuilder;
    }
}
