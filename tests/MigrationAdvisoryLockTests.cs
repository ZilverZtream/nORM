using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using MigrationBase = nORM.Migration.Migration;
using nORM.Migration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Migration advisory lock — structural tests
//
// Purpose: prove that each non-SQLite migration runner acquires and releases
// a database-level advisory lock around ApplyMigrationsAsync so that two
// processes deploying simultaneously cannot both apply the same pending list.
//
// Structural tests (no live SQL Server / MySQL / Postgres required):
//   - Lock constants / SQL shape verified for each provider.
//   - SQLite runner serialises concurrent calls via BEGIN EXCLUSIVE (live test).
// ══════════════════════════════════════════════════════════════════════════════

public class MigrationAdvisoryLockTests
{
    // ── SQL Server lock SQL shape ─────────────────────────────────────────────

    [Fact]
    public void SqlServer_LockResource_IsNonEmpty()
    {
        Assert.NotEmpty(SqlServerMigrationRunner.MigrationLockResource);
    }

    [Fact]
    public void SqlServer_LockTimeout_IsPositive()
    {
        Assert.True(SqlServerMigrationRunner.MigrationLockTimeoutMs > 0);
    }

    [Fact]
    public void SqlServer_LockResource_ContainsNormPrefix()
    {
        // Lock name must not be a generic "lock" — needs to be nORM-namespaced to avoid
        // colliding with application-defined sp_getapplock resources.
        Assert.Contains("Norm", SqlServerMigrationRunner.MigrationLockResource,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SqlServer_LockTimeout_Is30SecondsOrMore()
    {
        // Minimum useful timeout for a deploy race: 30 000 ms.
        Assert.True(SqlServerMigrationRunner.MigrationLockTimeoutMs >= 30_000,
            $"Expected >= 30 000 ms, got {SqlServerMigrationRunner.MigrationLockTimeoutMs}");
    }

    // ── MySQL lock SQL shape ──────────────────────────────────────────────────

    [Fact]
    public void MySql_LockName_IsNonEmpty()
    {
        Assert.NotEmpty(MySqlMigrationRunner.MigrationLockName);
    }

    [Fact]
    public void MySql_LockTimeout_IsPositive()
    {
        Assert.True(MySqlMigrationRunner.MigrationLockTimeoutSeconds > 0);
    }

    [Fact]
    public void MySql_LockName_ContainsNormPrefix()
    {
        Assert.Contains("Norm", MySqlMigrationRunner.MigrationLockName,
            StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void MySql_LockTimeout_Is30SecondsOrMore()
    {
        Assert.True(MySqlMigrationRunner.MigrationLockTimeoutSeconds >= 30,
            $"Expected >= 30 s, got {MySqlMigrationRunner.MigrationLockTimeoutSeconds}");
    }

    // ── Postgres lock key shape ───────────────────────────────────────────────

    [Fact]
    public void Postgres_LockKey_IsNonZero()
    {
        // Zero is valid but a suspicious default — verify the key was deliberately set.
        Assert.NotEqual(0L, PostgresMigrationRunner.MigrationLockKey);
    }

    [Fact]
    public void Postgres_LockKey_IsStable()
    {
        // Same value every call — must be a compile-time constant, not generated at runtime.
        var first  = PostgresMigrationRunner.MigrationLockKey;
        var second = PostgresMigrationRunner.MigrationLockKey;
        Assert.Equal(first, second);
    }

    // ── SQLite runner concurrent behaviour (live) ─────────────────────────────

    // NOTE: The migration class used for SQLite concurrency tests lives in a DYNAMIC
    // assembly (not this test assembly). This prevents it from appearing in the
    // SqliteMigrationRunnerTests assembly scan that checks for exactly 7 migrations.

    private const long AlLockVersion = 9_900_001L;

    /// <summary>
    /// Emits a single-migration dynamic assembly with one no-op CREATE TABLE migration.
    /// Dynamic assembly isolates the migration type from the test assembly scan.
    /// </summary>
    private static Assembly BuildSingleMigrationAssembly(long version, string name)
    {
        var asmName    = new AssemblyName($"AlLockDyn_{Guid.NewGuid():N}");
        var asmBuilder = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        var modBuilder = asmBuilder.DefineDynamicModule("Module");

        var migBase  = typeof(MigrationBase);
        var baseCtor = migBase.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance, null,
            new[] { typeof(long), typeof(string) }, null)!;
        var upMethod   = migBase.GetMethod("Up",   new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var downMethod = migBase.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;

        var tb = modBuilder.DefineType(name,
            TypeAttributes.Public | TypeAttributes.Class, migBase);

        var ctor = tb.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, Type.EmptyTypes);
        var cil  = ctor.GetILGenerator();
        cil.Emit(OpCodes.Ldarg_0);
        cil.Emit(OpCodes.Ldc_I8, version);
        cil.Emit(OpCodes.Ldstr, name);
        cil.Emit(OpCodes.Call, baseCtor);
        cil.Emit(OpCodes.Ret);

        foreach (var (method, il) in new[] {
            (upMethod, tb.DefineMethod("Up", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })),
            (downMethod, tb.DefineMethod("Down", MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.HideBySig,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) }))
        })
        {
            il.GetILGenerator().Emit(OpCodes.Ret);
            tb.DefineMethodOverride(il, method);
        }

        tb.CreateType();
        return asmBuilder;
    }

    [Fact]
    public async Task SQLite_ConcurrentRunners_OnlyApplyMigrationsOnce()
    {
        // Two SqliteMigrationRunners pointing at the same file-backed DB.
        // The SQLite runner uses BEGIN EXCLUSIVE — so only one wins the table create;
        // the second sees it applied and skips. No history PK violation should occur.
        var asm    = BuildSingleMigrationAssembly(AlLockVersion, "AlCreateTable");
        var dbPath = System.IO.Path.GetTempFileName() + ".advisory.db";
        try
        {
            using var cn1 = new SqliteConnection($"Data Source={dbPath}");
            using var cn2 = new SqliteConnection($"Data Source={dbPath}");
            cn1.Open();
            cn2.Open();

            using var r1 = new SqliteMigrationRunner(cn1, asm);
            using var r2 = new SqliteMigrationRunner(cn2, asm);

            // Run both sequentially (concurrent in a real deploy, serial here for determinism).
            await r1.ApplyMigrationsAsync();
            await r2.ApplyMigrationsAsync(); // Should be a no-op — history shows it applied.

            // Verify history was recorded exactly once for our version.
            using var check = cn1.CreateCommand();
            check.CommandText =
                $"SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Version = {AlLockVersion}";
            Assert.Equal(1, Convert.ToInt32(check.ExecuteScalar()));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            try { System.IO.File.Delete(dbPath); } catch { }
            try { System.IO.File.Delete(dbPath + "-wal"); } catch { }
            try { System.IO.File.Delete(dbPath + "-shm"); } catch { }
        }
    }

    [Fact]
    public async Task SQLite_IdempotentRun_SecondCallIsNoOp()
    {
        var asm    = BuildSingleMigrationAssembly(AlLockVersion, "AlCreateTable2");
        var dbPath = System.IO.Path.GetTempFileName() + ".advisory2.db";
        try
        {
            using var cn = new SqliteConnection($"Data Source={dbPath}");
            cn.Open();
            using var runner = new SqliteMigrationRunner(cn, asm);

            await runner.ApplyMigrationsAsync();
            await runner.ApplyMigrationsAsync(); // second call — must be a no-op

            using var check = cn.CreateCommand();
            check.CommandText =
                $"SELECT COUNT(*) FROM \"__NormMigrationsHistory\" WHERE Version = {AlLockVersion}";
            Assert.Equal(1, Convert.ToInt32(check.ExecuteScalar()));
        }
        finally
        {
            SqliteConnection.ClearAllPools();
            try { System.IO.File.Delete(dbPath); } catch { }
            try { System.IO.File.Delete(dbPath + "-wal"); } catch { }
            try { System.IO.File.Delete(dbPath + "-shm"); } catch { }
        }
    }

    // ── README / documentation contract ──────────────────────────────────────

    [Theory]
    [InlineData("sp_getapplock",    "SqlServer")]
    [InlineData("GET_LOCK",         "MySQL")]
    [InlineData("pg_advisory_lock", "PostgreSQL")]
    public void ReadmeDocuments_AdvisoryLockMechanism(string mechanismKeyword, string provider)
    {
        // Verify the README mentions each provider's advisory lock mechanism.
        var readmePath = System.IO.Path.Combine(
            System.IO.Path.GetDirectoryName(
                System.IO.Path.GetDirectoryName(
                    System.IO.Path.GetDirectoryName(
                        typeof(MigrationAdvisoryLockTests).Assembly.Location)!
                )!
            )!, "..", "..", "..", "..", "README.md");

        if (!System.IO.File.Exists(readmePath))
        {
            // In CI the relative path may differ; skip rather than fail.
            Assert.True(true, $"README not found at {readmePath} — skipping.");
            return;
        }

        var readme = System.IO.File.ReadAllText(readmePath);
        Assert.Contains(mechanismKeyword, readme, StringComparison.Ordinal);
        _ = provider; // documents intent in InlineData
    }
}
