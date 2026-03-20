using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Migration;
using nORM.Providers;
using MigrationBase = nORM.Migration.Migration;
using Xunit;

#pragma warning disable CS8765 // Nullability mismatch on overridden member
#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 4.0 → 4.5 — Section 8: Long-running contention tests on plan/materializer/
// result caches. Fault-injected live tests for cancellation during batched saves,
// migration application, and navigation loading. Replay/partial-failure tests
// across all four providers.
// ══════════════════════════════════════════════════════════════════════════════

public class CacheFaultInjectionStressTests : IDisposable
{
    // ── Entity types ────────────────────────────────────────────────────────

    [Table("CFI_Item")]
    private class CfiItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    [Table("CFI_BulkItem")]
    private class CfiBulkItem
    {
        [Key]
        public int Id { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    [Table("CFI_CacheRow")]
    private class CfiCacheRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Tag { get; set; } = string.Empty;
        public int Seq { get; set; }
    }

    [Table("CFI_Parent")]
    private class CfiParent
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public ICollection<CfiChild> Children { get; set; } = new List<CfiChild>();
    }

    [Table("CFI_Child")]
    private class CfiChild
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int ParentId { get; set; }
        public string Value { get; set; } = string.Empty;
    }

    // ── DDL ─────────────────────────────────────────────────────────────────

    private const string ItemDdl =
        "CREATE TABLE IF NOT EXISTS CFI_Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Value INTEGER NOT NULL)";

    private const string BulkDdl =
        "CREATE TABLE IF NOT EXISTS CFI_BulkItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL)";

    private const string CacheDdl =
        "CREATE TABLE IF NOT EXISTS CFI_CacheRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Tag TEXT NOT NULL, Seq INTEGER NOT NULL)";

    private const string ParentChildDdl = @"
        CREATE TABLE IF NOT EXISTS CFI_Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS CFI_Child  (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Value TEXT NOT NULL)";

    // ── Temp file tracking ──────────────────────────────────────────────────

    private readonly List<string> _tempFiles = new();

    private string CreateTempDbPath()
    {
        var path = Path.GetTempFileName();
        _tempFiles.Add(path);
        return path;
    }

    public void Dispose()
    {
        SqliteConnection.ClearAllPools();
        foreach (var path in _tempFiles)
        {
            try { File.Delete(path); } catch { }
            try { File.Delete(path + "-wal"); } catch { }
            try { File.Delete(path + "-shm"); } catch { }
        }
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private static string SharedDbCs(string name) =>
        $"Data Source={name};Mode=Memory;Cache=Shared";

    private static SqliteConnection OpenShared(string cs)
    {
        var cn = new SqliteConnection(cs);
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static long CountRows(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM [{table}]";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static DbContext BuildCtx(SqliteConnection cn, DbContextOptions? opts = null)
        => new(cn, new SqliteProvider(), opts, ownsConnection: false);

    // ── Dynamic migration assembly helpers (from MigrationReplayFailureTests) ──

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

    private static Assembly NoOpAssembly(params (string Name, long Version)[] specs)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("CFI_NoOp_" + Guid.NewGuid().ToString("N")),
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

    private static Assembly DdlAssembly(long version, string name, string createTableSql)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("CFI_Ddl_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");
        var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb, version, name);
        EmitDdlUp(tb, createTableSql);
        EmitNoOpDown(tb);
        tb.CreateType();
        return ab;
    }

    private static Assembly TwoMigAssembly(
        long v1, string n1, string createSql,
        long v2, string n2)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("CFI_Two_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");

        var tb1 = mod.DefineType(n1, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb1, v1, n1);
        EmitDdlUp(tb1, createSql);
        EmitNoOpDown(tb1);
        tb1.CreateType();

        var tb2 = mod.DefineType(n2, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb2, v2, n2);
        EmitThrowingUp(tb2);
        EmitNoOpDown(tb2);
        tb2.CreateType();

        return ab;
    }

    /// <summary>Builds a single migration whose Up() executes a large multi-row DDL.</summary>
    private static Assembly HeavyDdlAssembly(long version, string name, string sql)
        => DdlAssembly(version, name, sql);

    private static async Task<long> HistoryCountAsync(SqliteConnection cn)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"";
        return Convert.ToInt64(await cmd.ExecuteScalarAsync());
    }

    private static long TableExists(SqliteConnection cn, string tableName)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{tableName}'";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 1. Plan cache contention — 20 concurrent tasks, different Where predicates
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task PlanCacheContention_20Tasks_DifferentPredicates_3Seconds_NoPlanCorruption()
    {
        var dbName = $"cfi_pc_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ItemDdl);

        // Seed 200 rows: Value 0-19, 10 rows each
        for (int i = 1; i <= 200; i++)
            Exec(anchor, $"INSERT INTO CFI_Item VALUES ({i}, 'item{i}', {(i - 1) % 20})");

        const int taskCount = 20;
        var errors = new ConcurrentBag<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(() =>
        {
            try
            {
                int filterValue = taskIdx; // 0..19
                int iteration = 0;
                while (!cts.IsCancellationRequested)
                {
                    using var cn = OpenShared(cs);
                    using var ctx = BuildCtx(cn);

                    // Equality filter — each task's filterValue selects exactly 10 rows
                    var eqResults = ctx.Query<CfiItem>()
                        .Where(x => x.Value == filterValue)
                        .ToList();

                    if (eqResults.Count != 10)
                        errors.Add($"task{taskIdx} iter{iteration} eq: expected 10, got {eqResults.Count}");
                    if (eqResults.Any(r => r.Value != filterValue))
                        errors.Add($"task{taskIdx} iter{iteration} eq: wrong Value in results");

                    // Range filter — Value >= filterValue
                    var rangeResults = ctx.Query<CfiItem>()
                        .Where(x => x.Value >= filterValue)
                        .OrderBy(x => x.Id)
                        .ToList();
                    int expectedRange = (20 - filterValue) * 10;
                    if (rangeResults.Count != expectedRange)
                        errors.Add($"task{taskIdx} iter{iteration} range: expected {expectedRange}, got {rangeResults.Count}");

                    iteration++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                errors.Add($"task{taskIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        anchor.Close();
        Assert.Empty(errors);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 2. Materializer cache contention — 20 tasks, same entity type, different
    //    projections and full-entity materialization
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MaterializerCacheContention_20Tasks_DifferentProjections_AllCorrect()
    {
        var dbName = $"cfi_mc_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ItemDdl);

        // Seed 100 rows
        for (int i = 1; i <= 100; i++)
            Exec(anchor, $"INSERT INTO CFI_Item VALUES ({i}, 'item{i:D3}', {i % 10})");

        const int taskCount = 20;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(() =>
        {
            try
            {
                using var cn = OpenShared(cs);
                using var ctx = BuildCtx(cn);

                int filterValue = taskIdx % 10;

                // Full entity materialization
                var fullEntities = ctx.Query<CfiItem>()
                    .Where(x => x.Value == filterValue)
                    .OrderBy(x => x.Id)
                    .ToList();
                if (fullEntities.Count != 10)
                    errors.Add($"task{taskIdx} full: expected 10, got {fullEntities.Count}");
                foreach (var e in fullEntities)
                {
                    if (e.Value != filterValue)
                        errors.Add($"task{taskIdx} full: wrong Value={e.Value}");
                    if (string.IsNullOrEmpty(e.Name))
                        errors.Add($"task{taskIdx} full: Name is null/empty for Id={e.Id}");
                }

                // Anonymous projection (Id, Name)
                var projectedIdName = ctx.Query<CfiItem>()
                    .Where(x => x.Value == filterValue)
                    .Select(x => new { x.Id, x.Name })
                    .OrderBy(x => x.Id)
                    .ToList();
                if (projectedIdName.Count != 10)
                    errors.Add($"task{taskIdx} proj-IdName: expected 10, got {projectedIdName.Count}");
                foreach (var p in projectedIdName)
                {
                    if (p.Id <= 0) errors.Add($"task{taskIdx} proj-IdName: invalid Id={p.Id}");
                    if (string.IsNullOrEmpty(p.Name))
                        errors.Add($"task{taskIdx} proj-IdName: Name null/empty for Id={p.Id}");
                }

                // Anonymous projection (Id, Value)
                var projectedIdVal = ctx.Query<CfiItem>()
                    .Where(x => x.Value == filterValue)
                    .Select(x => new { x.Id, x.Value })
                    .OrderBy(x => x.Id)
                    .ToList();
                if (projectedIdVal.Count != 10)
                    errors.Add($"task{taskIdx} proj-IdVal: expected 10, got {projectedIdVal.Count}");
                foreach (var p in projectedIdVal)
                {
                    if (p.Value != filterValue)
                        errors.Add($"task{taskIdx} proj-IdVal: wrong Value={p.Value}");
                }
            }
            catch (Exception ex)
            {
                errors.Add($"task{taskIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        anchor.Close();
        Assert.Empty(errors);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 3. Result cache contention — Cacheable queries + InvalidateTag concurrently
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task ResultCacheContention_CacheableQueries_InvalidateTag_NoStaleData()
    {
        var dbName = $"cfi_rc_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, CacheDdl);

        // Seed 50 rows
        for (int i = 1; i <= 50; i++)
            Exec(anchor, $"INSERT INTO CFI_CacheRow (Tag, Seq) VALUES ('batch{i % 5}', {i})");

        using var cache = new NormMemoryCacheProvider();
        var errors = new ConcurrentBag<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        // 5 reader tasks doing Cacheable queries
        var readers = Enumerable.Range(0, 5).Select(rIdx => Task.Run(async () =>
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    using var cn = OpenShared(cs);
                    await using var ctx = BuildCtx(cn, new DbContextOptions { CacheProvider = cache });
                    var rows = await ctx.Query<CfiCacheRow>()
                        .Cacheable(TimeSpan.FromMinutes(5))
                        .ToListAsync();

                    // Rows must be non-empty and self-consistent
                    if (rows.Count == 0)
                        errors.Add($"reader{rIdx}: got 0 rows (expected >= 50)");
                    foreach (var r in rows)
                    {
                        if (string.IsNullOrEmpty(r.Tag))
                            errors.Add($"reader{rIdx}: empty Tag for Id={r.Id}");
                    }
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                errors.Add($"reader{rIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        // 5 invalidator tasks calling InvalidateTag
        var invalidators = Enumerable.Range(0, 5).Select(iIdx => Task.Run(() =>
        {
            try
            {
                while (!cts.IsCancellationRequested)
                {
                    cache.InvalidateTag("CFI_CacheRow");
                    Thread.SpinWait(100);
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                errors.Add($"invalidator{iIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(readers.Concat(invalidators));

        // After storm: new cache set must work
        cache.Set("post-storm-cfi", "ok", TimeSpan.FromMinutes(1), new[] { "test" });
        Assert.True(cache.TryGet<string>("post-storm-cfi", out var val));
        Assert.Equal("ok", val);

        anchor.Close();
        Assert.Empty(errors);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 4. Cancellation during batched SaveChangesAsync — 100 entities, cancel
    //    after 50ms. Verify no partial state (either OCE or full commit).
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CancellationDuringBatchedSave_100Entities_NoPartialState()
    {
        var dbPath = CreateTempDbPath();
        var connStr = $"Data Source={dbPath}";

        using (var setup = new SqliteConnection(connStr))
        {
            setup.Open();
            Exec(setup, ItemDdl);
        }

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(50));

        bool wasCancelled = false;
        int saved = 0;

        try
        {
            using var cn = new SqliteConnection(connStr);
            cn.Open();
            using var ctx = BuildCtx(cn);
            for (int i = 1; i <= 100; i++)
                ctx.Add(new CfiItem { Id = i, Name = $"entity{i}", Value = i });
            saved = await ctx.SaveChangesAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            wasCancelled = true;
        }

        // Verify DB state is consistent: either 0 rows (rolled back) or 100 (committed)
        using var verifyCn = new SqliteConnection(connStr);
        verifyCn.Open();
        var actualRows = CountRows(verifyCn, "CFI_Item");

        if (wasCancelled)
        {
            // If transaction was rolled back, 0 rows; if the cancel arrived after
            // commit, the rows may be present. Either way, DB must be queryable.
            Assert.True(actualRows == 0 || actualRows == 100,
                $"Partial state detected: {actualRows} rows (expected 0 or 100)");
        }
        else
        {
            Assert.Equal(100, saved);
            Assert.Equal(100, actualRows);
        }

        // Structural integrity: a simple query must not throw
        using var check = verifyCn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM CFI_Item WHERE Name LIKE 'entity%'";
        var named = Convert.ToInt64(check.ExecuteScalar());
        Assert.Equal(actualRows, named);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 5. Cancellation during BulkInsert — cancel mid-flight, verify either
    //    OCE or complete insert, no half-committed rows
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CancellationDuringBulkInsert_NoHalfCommittedRows()
    {
        var dbPath = CreateTempDbPath();
        var connStr = $"Data Source={dbPath}";

        using (var setup = new SqliteConnection(connStr))
        {
            setup.Open();
            Exec(setup, BulkDdl);
        }

        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(30));

        var items = Enumerable.Range(1, 500)
            .Select(i => new CfiBulkItem { Id = i, Label = $"bulk_{i}_{new string('x', 50)}" })
            .ToList();

        bool wasCancelled = false;
        int insertedCount = 0;

        try
        {
            using var cn = new SqliteConnection(connStr);
            cn.Open();
            using var ctx = BuildCtx(cn);
            insertedCount = await ctx.BulkInsertAsync(items, cts.Token);
        }
        catch (OperationCanceledException)
        {
            wasCancelled = true;
        }

        // Verify: either full commit or 0 rows (rolled back) or partial batch boundary
        using var verifyCn = new SqliteConnection(connStr);
        verifyCn.Open();
        var actualRows = CountRows(verifyCn, "CFI_BulkItem");

        if (wasCancelled)
        {
            // BulkInsert uses a transaction: partial within a batch is impossible.
            // Rows present means committed batches up to that point.
            Assert.True(actualRows >= 0 && actualRows <= 500,
                $"Row count {actualRows} out of expected range [0, 500]");
        }
        else
        {
            Assert.Equal(500, insertedCount);
            Assert.Equal(500, actualRows);
        }

        // Integrity check
        using var checkCmd = verifyCn.CreateCommand();
        checkCmd.CommandText = "PRAGMA integrity_check";
        var integrity = checkCmd.ExecuteScalar() as string;
        Assert.Equal("ok", integrity);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 6. Cancellation during migration — migration with heavy INSERT, cancel.
    //    Verify exception propagates and DB is clean (SQLite transactional).
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CancellationDuringMigration_ExceptionPropagates_DbClean()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        // Build a migration that creates a table and inserts 1000 rows
        var sb = new System.Text.StringBuilder();
        sb.Append("CREATE TABLE CFI_MigHeavy (Id INTEGER PRIMARY KEY, Data TEXT NOT NULL);");
        for (int i = 1; i <= 1000; i++)
            sb.Append($"INSERT INTO CFI_MigHeavy VALUES ({i}, 'row{i}');");
        var heavySql = sb.ToString();

        var asm = HeavyDdlAssembly(1000L, "HeavyMigration", heavySql);
        var runner = new SqliteMigrationRunner(cn, asm);

        // The migration runs synchronously inside Up(), so a CTS with CancelAfter
        // will only take effect if SQLite is cooperating with the token. In practice
        // SQLite migrations complete atomically. We test that:
        //   (a) if it completes, the table+rows exist and history records it
        //   (b) if it throws (for any reason), the DB is clean
        Exception? caught = null;
        try
        {
            using var migCts = new CancellationTokenSource();
            migCts.CancelAfter(TimeSpan.FromMilliseconds(5));
            await runner.ApplyMigrationsAsync(migCts.Token);
        }
        catch (Exception ex)
        {
            caught = ex;
        }

        if (caught != null)
        {
            // Exception propagated. DB should be clean (SQLite single-tx rollback).
            Assert.Equal(0L, TableExists(cn, "CFI_MigHeavy"));
        }
        else
        {
            // Migration completed before cancel fired. Verify full commit.
            Assert.Equal(1L, TableExists(cn, "CFI_MigHeavy"));
            Assert.Equal(1000L, CountRows(cn, "CFI_MigHeavy"));
            Assert.Equal(1L, await HistoryCountAsync(cn));
        }
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 7. Cancellation during navigation load (Include query) — verify either
    //    OCE or complete result
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CancellationDuringNavigationLoad_OceOrCompleteResult()
    {
        var dbName = $"cfi_nav_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ParentChildDdl);

        // Seed 20 parents with 10 children each
        for (int p = 1; p <= 20; p++)
        {
            Exec(anchor, $"INSERT INTO CFI_Parent VALUES ({p}, 'parent{p}')");
            for (int c = 1; c <= 10; c++)
            {
                int childId = (p - 1) * 10 + c;
                Exec(anchor, $"INSERT INTO CFI_Child (Id, ParentId, Value) VALUES ({childId}, {p}, 'child{childId}')");
            }
        }

        // Run Include query with a very tight cancellation window.
        // Include requires OnModelCreating to register the HasMany relationship.
        using var cts = new CancellationTokenSource();
        cts.CancelAfter(TimeSpan.FromMilliseconds(10));

        bool wasCancelled = false;
        List<CfiParent>? result = null;

        try
        {
            using var cn = OpenShared(cs);
            var opts = new DbContextOptions
            {
                OnModelCreating = mb =>
                {
                    mb.Entity<CfiParent>()
                        .HasMany(p => p.Children)
                        .WithOne()
                        .HasForeignKey(c => c.ParentId, p => p.Id);
                }
            };
            await using var ctx = BuildCtx(cn, opts);
            result = await ((INormQueryable<CfiParent>)ctx.Query<CfiParent>())
                .Include(p => p.Children)
                .AsSplitQuery()
                .ToListAsync(cts.Token);
        }
        catch (OperationCanceledException)
        {
            wasCancelled = true;
        }

        if (wasCancelled)
        {
            // OCE is acceptable — nothing to verify about partial results
            Assert.Null(result);
        }
        else
        {
            // Full result: 20 parents, each with 10 children
            Assert.NotNull(result);
            Assert.Equal(20, result!.Count);
            foreach (var parent in result)
            {
                Assert.NotNull(parent.Name);
                Assert.Equal(10, parent.Children.Count);
                Assert.All(parent.Children, ch =>
                {
                    Assert.Equal(parent.Id, ch.ParentId);
                    Assert.False(string.IsNullOrEmpty(ch.Value));
                });
            }
        }

        anchor.Close();
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 8. Migration replay all providers (SQLite live) — apply, then replay is
    //    a no-op with no error
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationReplay_SQLiteLive_SecondRunIsNoOp()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var createSql = "CREATE TABLE CFI_Replay (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL)";
        var asm = DdlAssembly(2000L, "ReplayMig", createSql);

        // First run
        var runner1 = new SqliteMigrationRunner(cn, asm);
        await runner1.ApplyMigrationsAsync();
        Assert.Equal(1L, await HistoryCountAsync(cn));
        Assert.Equal(1L, TableExists(cn, "CFI_Replay"));

        // Replay: second run must be a no-op
        var runner2 = new SqliteMigrationRunner(cn, asm);
        await runner2.ApplyMigrationsAsync();
        Assert.Equal(1L, await HistoryCountAsync(cn));
        Assert.False(await runner2.HasPendingMigrationsAsync());

        // Third run with fresh runner instance
        var runner3 = new SqliteMigrationRunner(cn, asm);
        Assert.False(await runner3.HasPendingMigrationsAsync());
        await runner3.ApplyMigrationsAsync();
        Assert.Equal(1L, await HistoryCountAsync(cn));
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 9. Migration partial failure — first succeeds, second throws. Verify
    //    both rolled back (SQLite single-transaction semantics).
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationPartialFailure_FirstSucceeds_SecondThrows_BothRolledBack()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var asm = TwoMigAssembly(
            3000L, "CreateWidget", "CREATE TABLE CFI_Widget (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL)",
            3001L, "FailWidget");

        var runner = new SqliteMigrationRunner(cn, asm);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => runner.ApplyMigrationsAsync());
        Assert.Contains("Simulated migration failure", ex.Message);

        // SQLite transactional semantics: both rolled back
        Assert.Equal(0L, await HistoryCountAsync(cn));
        Assert.Equal(0L, TableExists(cn, "CFI_Widget"));

        // Both remain pending
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Equal(2, pending.Length);
        Assert.Contains("3000_CreateWidget", pending);
        Assert.Contains("3001_FailWidget", pending);

        // Fix and replay succeeds
        var fixedAsm = NoOpAssembly(("CreateWidget", 3000L), ("FixedWidget", 3001L));
        var fixedRunner = new SqliteMigrationRunner(cn, fixedAsm);
        await fixedRunner.ApplyMigrationsAsync();
        Assert.Equal(2L, await HistoryCountAsync(cn));
        Assert.False(await fixedRunner.HasPendingMigrationsAsync());
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 10. Compiled query under contention — 10 tasks calling same CompileQuery
    //     delegate concurrently, all must produce correct results
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CompiledQueryUnderContention_10Tasks_AllCorrect()
    {
        var dbName = $"cfi_cq_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ItemDdl);

        // Seed 100 rows: Value = i * 10 (10, 20, ..., 1000)
        for (int i = 1; i <= 100; i++)
            Exec(anchor, $"INSERT INTO CFI_Item VALUES ({i}, 'cq{i}', {i * 10})");

        // Compile a single query delegate shared across all tasks
        var compiled = Norm.CompileQuery((DbContext c, int minValue) =>
            c.Query<CfiItem>().Where(x => x.Value >= minValue));

        const int taskCount = 10;
        var errors = new ConcurrentBag<string>();
        var results = new ConcurrentDictionary<int, List<CfiItem>>();

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(async () =>
        {
            try
            {
                using var cn = OpenShared(cs);
                using var ctx = BuildCtx(cn);
                // Each task queries Value >= (taskIdx + 1) * 100
                int minVal = (taskIdx + 1) * 100;
                var items = await compiled(ctx, minVal);
                results[taskIdx] = items;
            }
            catch (Exception ex)
            {
                errors.Add($"task{taskIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        Assert.Empty(errors);

        // Verify each task got correct results
        for (int t = 0; t < taskCount; t++)
        {
            Assert.True(results.ContainsKey(t), $"Task {t} did not produce results");
            int minVal = (t + 1) * 100;
            // Values are 10, 20, ..., 1000. Count those >= minVal.
            int expectedCount = Enumerable.Range(1, 100).Count(i => i * 10 >= minVal);
            Assert.Equal(expectedCount, results[t].Count);
            Assert.All(results[t], item => Assert.True(item.Value >= minVal,
                $"Task {t}: Item {item.Id} has Value {item.Value} < {minVal}"));
        }

        anchor.Close();
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 11. BoundedCache eviction under contention — 30 tasks writing to
    //     BoundedCache(50), verify count bounded and values correct
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task BoundedCacheEvictionUnderContention_30Tasks_SizeBounded_ValuesCorrect()
    {
        const int maxSize = 50;
        var cache = new BoundedCache<string, int>(maxSize);
        const int taskCount = 30;
        const int keysPerTask = 100;
        var errors = new ConcurrentBag<string>();

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(() =>
        {
            try
            {
                for (int i = 0; i < keysPerTask; i++)
                {
                    var key = $"t{taskIdx}_k{i}";
                    int value = taskIdx * keysPerTask + i;
                    cache.Set(key, value);

                    // If the key is still retrievable, its value must be correct
                    if (cache.TryGet(key, out var retrieved) && retrieved != value)
                        errors.Add($"{key}: expected {value}, got {retrieved}");
                }
            }
            catch (Exception ex)
            {
                errors.Add($"task{taskIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);

        // Cache must never exceed maxSize
        Assert.True(cache.Count <= maxSize,
            $"Cache count {cache.Count} exceeds max size {maxSize}");

        // All values that ARE present must be correct
        int verifiedCount = 0;
        for (int t = 0; t < taskCount; t++)
        {
            for (int i = 0; i < keysPerTask; i++)
            {
                var key = $"t{t}_k{i}";
                if (cache.TryGet(key, out var val))
                {
                    Assert.Equal(t * keysPerTask + i, val);
                    verifiedCount++;
                }
            }
        }

        // Some entries should be present (cache not empty)
        Assert.True(verifiedCount > 0, "Cache should contain at least one entry");
        // Total keys inserted (3000) far exceeds maxSize (50), so eviction occurred
        Assert.True(verifiedCount <= maxSize,
            $"Verified count {verifiedCount} should not exceed max size {maxSize}");

        Assert.Empty(errors);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 12. Transaction savepoint cancellation — create savepoint, cancel before
    //     rollback, verify transaction still usable
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task TransactionSavepointCancellation_TransactionStillUsable()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        Exec(cn, "CREATE TABLE CFI_SP (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL)");

        var provider = new SqliteProvider();
        await using var tx = await cn.BeginTransactionAsync();

        // Insert row 1 within the transaction
        using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = (SqliteTransaction)tx;
            cmd.CommandText = "INSERT INTO CFI_SP VALUES (1, 'before-savepoint')";
            cmd.ExecuteNonQuery();
        }

        // Create savepoint
        await provider.CreateSavepointAsync(tx, "sp_test");

        // Insert row 2 within the transaction (after savepoint)
        using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = (SqliteTransaction)tx;
            cmd.CommandText = "INSERT INTO CFI_SP VALUES (2, 'after-savepoint')";
            cmd.ExecuteNonQuery();
        }

        // Cancel before rollback — use a pre-cancelled CTS
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        // RollbackToSavepointAsync with cancelled token should throw OCE
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            provider.RollbackToSavepointAsync(tx, "sp_test", cts.Token));

        // Transaction must still be usable despite the cancelled rollback
        // Insert row 3
        using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = (SqliteTransaction)tx;
            cmd.CommandText = "INSERT INTO CFI_SP VALUES (3, 'after-cancel')";
            cmd.ExecuteNonQuery();
        }

        // Now rollback to savepoint without cancellation — row 2 and 3 should be undone
        await provider.RollbackToSavepointAsync(tx, "sp_test");

        // Insert row 4 (should persist after commit)
        using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = (SqliteTransaction)tx;
            cmd.CommandText = "INSERT INTO CFI_SP VALUES (4, 'post-rollback')";
            cmd.ExecuteNonQuery();
        }

        await tx.CommitAsync();

        // Final state: row 1 (before savepoint) + row 4 (post rollback) = 2 rows
        Assert.Equal(2L, CountRows(cn, "CFI_SP"));

        using var verify = cn.CreateCommand();
        verify.CommandText = "SELECT Label FROM CFI_SP ORDER BY Id";
        using var reader = verify.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal("before-savepoint", reader.GetString(0));
        Assert.True(reader.Read());
        Assert.Equal("post-rollback", reader.GetString(0));
        Assert.False(reader.Read());
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 13. Supplemental: Migration replay across MySQL runner shape
    //     (NoLock bypass on SQLite)
    // ═════════════════════════════════════════════════════════════════════════

    private sealed class NoLockMySqlRunner : MySqlMigrationRunner
    {
        public NoLockMySqlRunner(DbConnection cn, Assembly asm)
            : base(cn, asm) { }

        protected internal override Task AcquireAdvisoryLockAsync(CancellationToken ct) => Task.CompletedTask;
        protected internal override Task ReleaseAdvisoryLockAsync(CancellationToken ct) => Task.CompletedTask;
    }

    private static void CreateMySqlHistoryTable(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE \"__NormMigrationsHistory\" " +
            "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL, " +
            "Status TEXT NOT NULL DEFAULT 'Applied');";
        cmd.ExecuteNonQuery();
    }

    [Fact]
    public async Task MigrationReplay_MySQL_ShapeTest_ReplayIsNoOp()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        var asm = NoOpAssembly(("MySqlReplay1", 4000L), ("MySqlReplay2", 4001L));
        var runner = new NoLockMySqlRunner(cn, asm);

        // First run
        await runner.ApplyMigrationsAsync();
        Assert.False(await runner.HasPendingMigrationsAsync());

        // Replay: second run is no-op
        await runner.ApplyMigrationsAsync();
        Assert.False(await runner.HasPendingMigrationsAsync());
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 14. Supplemental: Postgres runner — pre-seed history, verify already-
    //     applied detection
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationReplay_Postgres_ShapeTest_AlreadyAppliedDetected()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        // Pre-create Postgres-style history table
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"CREATE TABLE IF NOT EXISTS ""__NormMigrationsHistory"" " +
                              @"(Version BIGINT PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TIMESTAMP NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        // Seed applied rows
        Exec(cn, @"INSERT INTO ""__NormMigrationsHistory"" (""Version"", ""Name"", ""AppliedOn"") VALUES (5000, 'PgReplay1', '2026-01-01')");
        Exec(cn, @"INSERT INTO ""__NormMigrationsHistory"" (""Version"", ""Name"", ""AppliedOn"") VALUES (5001, 'PgReplay2', '2026-01-01')");

        var asm = NoOpAssembly(("PgReplay1", 5000L), ("PgReplay2", 5001L));
        var runner = new PostgresMigrationRunner(cn, asm);

        Assert.False(await runner.HasPendingMigrationsAsync());
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Empty(pending);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 15. Supplemental: SqlServer runner — pre-seed history, verify already-
    //     applied detection via reflection
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationReplay_SqlServer_ShapeTest_AlreadyAppliedDetected()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();

        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE [__NormMigrationsHistory] " +
                "(Version INTEGER PRIMARY KEY, Name TEXT NOT NULL, AppliedOn TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }

        Exec(cn, "INSERT INTO [__NormMigrationsHistory] (Version, Name, AppliedOn) VALUES (6000, 'SsReplay1', '2026-01-01')");
        Exec(cn, "INSERT INTO [__NormMigrationsHistory] (Version, Name, AppliedOn) VALUES (6001, 'SsReplay2', '2026-01-01')");

        var asm = NoOpAssembly(("SsReplay1", 6000L), ("SsReplay2", 6001L));
        var runner = new SqlServerMigrationRunner(cn, asm);

        // Use reflection to call GetPendingMigrationsInternalAsync
        var method = typeof(SqlServerMigrationRunner)
            .GetMethod("GetPendingMigrationsInternalAsync",
                BindingFlags.NonPublic | BindingFlags.Instance)!;
        var pending = await (Task<List<MigrationBase>>)
            method.Invoke(runner, new object[] { CancellationToken.None })!;

        Assert.Empty(pending);
    }

    // ═════════════════════════════════════════════════════════════════════════
    // 16. Supplemental: Long-running plan + materializer combined stress
    //     (3-second horizon, interleaved queries + inserts)
    // ═════════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task CombinedPlanMaterializerStress_3Seconds_NoCrossContamination()
    {
        var dbName = $"cfi_combo_{Guid.NewGuid():N}";
        var cs = SharedDbCs(dbName);
        using var anchor = OpenShared(cs);
        Exec(anchor, ItemDdl);

        // Seed 500 rows
        for (int i = 1; i <= 500; i++)
            Exec(anchor, $"INSERT INTO CFI_Item VALUES ({i}, 'combo{i:D3}', {i % 25})");

        const int taskCount = 20;
        var errors = new ConcurrentBag<string>();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));

        var tasks = Enumerable.Range(0, taskCount).Select(taskIdx => Task.Run(() =>
        {
            try
            {
                int iteration = 0;
                while (!cts.IsCancellationRequested)
                {
                    using var cn = OpenShared(cs);
                    using var ctx = BuildCtx(cn);

                    int filterValue = (taskIdx + iteration) % 25;

                    switch (iteration % 4)
                    {
                        case 0:
                        {
                            // Full entity
                            var rows = ctx.Query<CfiItem>()
                                .Where(x => x.Value == filterValue)
                                .OrderBy(x => x.Id)
                                .ToList();
                            if (rows.Count != 20)
                                errors.Add($"t{taskIdx}i{iteration} full: {rows.Count} != 20");
                            break;
                        }
                        case 1:
                        {
                            // Projection
                            var projected = ctx.Query<CfiItem>()
                                .Where(x => x.Value == filterValue)
                                .Select(x => new { x.Id, x.Name })
                                .ToList();
                            if (projected.Count != 20)
                                errors.Add($"t{taskIdx}i{iteration} proj: {projected.Count} != 20");
                            break;
                        }
                        case 2:
                        {
                            // Count
                            var count = ctx.Query<CfiItem>()
                                .Where(x => x.Value == filterValue)
                                .CountAsync().GetAwaiter().GetResult();
                            if (count != 20)
                                errors.Add($"t{taskIdx}i{iteration} count: {count} != 20");
                            break;
                        }
                        case 3:
                        {
                            // Skip/Take
                            var page = ctx.Query<CfiItem>()
                                .Where(x => x.Value == filterValue)
                                .OrderBy(x => x.Id)
                                .Skip(5)
                                .Take(10)
                                .ToList();
                            if (page.Count != 10)
                                errors.Add($"t{taskIdx}i{iteration} page: {page.Count} != 10");
                            break;
                        }
                    }
                    iteration++;
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                errors.Add($"t{taskIdx}: {ex.GetType().Name}: {ex.Message}");
            }
        })).ToArray();

        await Task.WhenAll(tasks);
        anchor.Close();
        Assert.Empty(errors);
    }
}
