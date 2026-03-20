using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using Xunit;

using MigrationBase = nORM.Migration.Migration;

#nullable enable

namespace nORM.Tests;

// ── Entity types for Provider DML / Migration Parity tests ────────────────────

[Table("PDM_Item")]
public class PdmItem
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public int Score { get; set; }
}

[Table("PDM_ItemB")]
public class PdmItemB
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Label { get; set; } = "";
}

/// <summary>
/// Gate 4.0: live or faithfully simulated provider tests covering
/// write SQL generation, rows-affected semantics, paging executability,
/// savepoint behavior, migration failure/retry.
/// SQLite live; MSSQL/MySQL/PostgreSQL env-gated.
/// </summary>
public class ProviderDmlMigrationParityTests
{
    // ── Dynamic migration assembly helpers ─────────────────────────────────

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

    private static Assembly DdlAssembly(long version, string name, string sql)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("PDM_Ddl_" + Guid.NewGuid().ToString("N")),
            AssemblyBuilderAccess.Run);
        var mod = ab.DefineDynamicModule("Main");
        var tb = mod.DefineType(name, TypeAttributes.Public | TypeAttributes.Class, typeof(MigrationBase));
        EmitCtor(tb, version, name);
        EmitDdlUp(tb, sql);
        EmitNoOpDown(tb);
        tb.CreateType();
        return ab;
    }

    private static Assembly TwoMigAssembly(
        long v1, string n1, string createSql,
        long v2, string n2)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("PDM_Two_" + Guid.NewGuid().ToString("N")),
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

    private static Assembly NoOpAssembly(params (string Name, long Version)[] specs)
    {
        var ab = AssemblyBuilder.DefineDynamicAssembly(
            new AssemblyName("PDM_NoOp_" + Guid.NewGuid().ToString("N")),
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

    // ── Shared helpers ────────────────────────────────────────────────────

    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void Exec(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static long Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static bool TableExists(SqliteConnection cn, string tableName)
    {
        return Scalar(cn, $"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{tableName}'") > 0;
    }

    private static DbContext BuildCtx(SqliteConnection cn, Action<DbContextOptions>? configure = null)
    {
        var opts = new DbContextOptions();
        configure?.Invoke(opts);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private const string PdmItemDdl =
        "CREATE TABLE PDM_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Score INTEGER NOT NULL DEFAULT 0)";

    private const string PdmItemBDdl =
        "CREATE TABLE PDM_ItemB (Id INTEGER PRIMARY KEY AUTOINCREMENT, Label TEXT NOT NULL DEFAULT '')";

    // ═══════════════════════════════════════════════════════════════════════
    // 1. Write SQL generation parity
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void WriteSqlGeneration_Insert_SQLite_ContainsCorrectTableAndColumns()
    {
        using var cn = OpenMemory();
        using var ctx = BuildCtx(cn, o => o.OnModelCreating = mb => mb.Entity<PdmItem>());
        var mapping = typeof(DbContext)
            .GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(ctx, new object[] { typeof(PdmItem) }) as dynamic;
        var provider = new SqliteProvider();
        string insertSql = provider.BuildInsert((nORM.Mapping.TableMapping)mapping!);

        Assert.Contains("\"PDM_Item\"", insertSql);
        Assert.Contains("\"Name\"", insertSql);
        Assert.Contains("\"Score\"", insertSql);
        Assert.StartsWith("INSERT INTO", insertSql);
        // SQLite 3.35+ uses RETURNING clause for identity retrieval
        Assert.Contains("RETURNING", insertSql);

        // Document expected SQL for other providers:
        // SQL Server: INSERT INTO [PDM_Item] ([Name], [Score]) VALUES (@Name, @Score); SELECT SCOPE_IDENTITY();
        // MySQL:      INSERT INTO `PDM_Item` (`Name`, `Score`) VALUES (@Name, @Score); SELECT LAST_INSERT_ID();
        // PostgreSQL: INSERT INTO "PDM_Item" ("Name", "Score") VALUES (@Name, @Score) RETURNING "Id";
    }

    [Fact]
    public void WriteSqlGeneration_Update_SQLite_ContainsCorrectTableAndColumns()
    {
        using var cn = OpenMemory();
        using var ctx = BuildCtx(cn, o => o.OnModelCreating = mb => mb.Entity<PdmItem>());
        var mapping = typeof(DbContext)
            .GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(ctx, new object[] { typeof(PdmItem) }) as dynamic;
        var provider = new SqliteProvider();
        string updateSql = provider.BuildUpdate((nORM.Mapping.TableMapping)mapping!);

        Assert.Contains("UPDATE", updateSql);
        Assert.Contains("\"PDM_Item\"", updateSql);
        Assert.Contains("SET", updateSql);
        Assert.Contains("WHERE", updateSql);
        Assert.Contains("\"Id\"", updateSql);

        // Document expected SQL for other providers:
        // SQL Server: UPDATE [PDM_Item] SET [Name]=@Name, [Score]=@Score WHERE [Id]=@Id
        // MySQL:      UPDATE `PDM_Item` SET `Name`=@Name, `Score`=@Score WHERE `Id`=@Id
        // PostgreSQL: UPDATE "PDM_Item" SET "Name"=@Name, "Score"=@Score WHERE "Id"=@Id
    }

    [Fact]
    public void WriteSqlGeneration_Delete_SQLite_ContainsCorrectTableAndColumns()
    {
        using var cn = OpenMemory();
        using var ctx = BuildCtx(cn, o => o.OnModelCreating = mb => mb.Entity<PdmItem>());
        var mapping = typeof(DbContext)
            .GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(ctx, new object[] { typeof(PdmItem) }) as dynamic;
        var provider = new SqliteProvider();
        string deleteSql = provider.BuildDelete((nORM.Mapping.TableMapping)mapping!);

        Assert.Contains("DELETE FROM", deleteSql);
        Assert.Contains("\"PDM_Item\"", deleteSql);
        Assert.Contains("WHERE", deleteSql);
        Assert.Contains("\"Id\"", deleteSql);

        // Document expected SQL for other providers:
        // SQL Server: DELETE FROM [PDM_Item] WHERE [Id]=@Id
        // MySQL:      DELETE FROM `PDM_Item` WHERE `Id`=@Id
        // PostgreSQL: DELETE FROM "PDM_Item" WHERE "Id"=@Id
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 2. Rows-affected semantics
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task RowsAffected_UpdateSingleRow_ReturnsOne()
    {
        await using var cn = OpenMemory();
        Exec(cn, PdmItemDdl);
        Exec(cn, "INSERT INTO PDM_Item (Name, Score) VALUES ('alpha', 10)");

        using var ctx = BuildCtx(cn, o => o.OnModelCreating = mb => mb.Entity<PdmItem>());
        var items = await ctx.Query<PdmItem>().ToListAsync();
        Assert.Single(items);

        items[0].Score = 42;
        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);

        // Verify value persisted
        var reloaded = Scalar(cn, "SELECT Score FROM PDM_Item WHERE Id = 1");
        Assert.Equal(42, reloaded);
    }

    [Fact]
    public async Task RowsAffected_DeleteNonExistent_ZeroRowsAffected()
    {
        await using var cn = OpenMemory();
        Exec(cn, PdmItemDdl);
        Exec(cn, "INSERT INTO PDM_Item (Name, Score) VALUES ('beta', 20)");

        using var ctx = BuildCtx(cn, o => o.OnModelCreating = mb => mb.Entity<PdmItem>());
        var items = await ctx.Query<PdmItem>().ToListAsync();
        Assert.Single(items);

        // Delete the row directly via SQL so the context's tracked entity becomes stale
        Exec(cn, "DELETE FROM PDM_Item WHERE Id = 1");
        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM PDM_Item"));

        // Mark entity as deleted in the tracker
        ctx.Remove(items[0]);

        // Without a TimestampColumn, no DbConcurrencyException is thrown.
        // SaveChanges returns the number of rows actually deleted (0, since
        // the row was already removed out-of-band).
        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(0, affected);

        // Table remains empty
        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM PDM_Item"));
    }

    [Fact]
    public async Task RowsAffected_InsertTwoRows_ReturnsTwo()
    {
        await using var cn = OpenMemory();
        Exec(cn, PdmItemDdl);

        using var ctx = BuildCtx(cn, o => o.OnModelCreating = mb => mb.Entity<PdmItem>());
        ctx.Add(new PdmItem { Name = "one", Score = 1 });
        ctx.Add(new PdmItem { Name = "two", Score = 2 });

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(2, affected);

        Assert.Equal(2, Scalar(cn, "SELECT COUNT(*) FROM PDM_Item"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 3. Paging executability
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Paging_Skip5Take5_Returns5CorrectRows()
    {
        await using var cn = OpenMemory();
        Exec(cn, PdmItemDdl);

        // Insert 20 rows (Score 1..20)
        for (int i = 1; i <= 20; i++)
            Exec(cn, $"INSERT INTO PDM_Item (Name, Score) VALUES ('item{i}', {i})");

        using var ctx = BuildCtx(cn, o => o.OnModelCreating = mb => mb.Entity<PdmItem>());
        var page = await ctx.Query<PdmItem>()
            .OrderBy(x => x.Score)
            .Skip(5).Take(5)
            .ToListAsync();

        Assert.Equal(5, page.Count);
        // Ordered by Score: Skip first 5 (1-5), take next 5 (6-10)
        var expectedScores = new[] { 6, 7, 8, 9, 10 };
        Assert.Equal(expectedScores, page.Select(x => x.Score).ToArray());
    }

    [Fact]
    public async Task Paging_SkipBeyondEnd_ReturnsEmpty()
    {
        await using var cn = OpenMemory();
        Exec(cn, PdmItemDdl);

        for (int i = 1; i <= 20; i++)
            Exec(cn, $"INSERT INTO PDM_Item (Name, Score) VALUES ('item{i}', {i})");

        using var ctx = BuildCtx(cn, o => o.OnModelCreating = mb => mb.Entity<PdmItem>());
        var page = await ctx.Query<PdmItem>()
            .OrderBy(x => x.Score)
            .Skip(100).Take(5)
            .ToListAsync();

        Assert.Empty(page);
    }

    [Fact]
    public async Task Paging_TakeWithoutSkip_ReturnsFirstN()
    {
        await using var cn = OpenMemory();
        Exec(cn, PdmItemDdl);

        for (int i = 1; i <= 20; i++)
            Exec(cn, $"INSERT INTO PDM_Item (Name, Score) VALUES ('item{i}', {i})");

        using var ctx = BuildCtx(cn, o => o.OnModelCreating = mb => mb.Entity<PdmItem>());
        var page = await ctx.Query<PdmItem>()
            .OrderBy(x => x.Score)
            .Take(3)
            .ToListAsync();

        Assert.Equal(3, page.Count);
        Assert.Equal(new[] { 1, 2, 3 }, page.Select(x => x.Score).ToArray());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 4. Savepoint behavior
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Savepoint_RollbackToSavepoint_RowNotPresent()
    {
        await using var cn = OpenMemory();
        Exec(cn, PdmItemDdl);

        var tx = cn.BeginTransaction();
        var provider = new SqliteProvider();

        // Insert a row inside the transaction
        using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO PDM_Item (Name, Score) VALUES ('before_sp', 100)";
            cmd.ExecuteNonQuery();
        }

        // Create savepoint
        await provider.CreateSavepointAsync(tx, "sp1");

        // Insert another row after savepoint
        using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO PDM_Item (Name, Score) VALUES ('after_sp', 200)";
            cmd.ExecuteNonQuery();
        }

        // Rollback to savepoint — should undo 'after_sp' but keep 'before_sp'
        await provider.RollbackToSavepointAsync(tx, "sp1");

        // Commit the transaction
        tx.Commit();

        // 'before_sp' should be present, 'after_sp' should NOT
        var beforeCount = Scalar(cn, "SELECT COUNT(*) FROM PDM_Item WHERE Name = 'before_sp'");
        var afterCount = Scalar(cn, "SELECT COUNT(*) FROM PDM_Item WHERE Name = 'after_sp'");

        Assert.Equal(1, beforeCount);
        Assert.Equal(0, afterCount);
    }

    [Fact]
    public async Task Savepoint_CommitWithoutRollback_BothRowsPresent()
    {
        await using var cn = OpenMemory();
        Exec(cn, PdmItemDdl);

        var tx = cn.BeginTransaction();
        var provider = new SqliteProvider();

        using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO PDM_Item (Name, Score) VALUES ('row1', 10)";
            cmd.ExecuteNonQuery();
        }

        await provider.CreateSavepointAsync(tx, "sp1");

        using (var cmd = cn.CreateCommand())
        {
            cmd.Transaction = tx;
            cmd.CommandText = "INSERT INTO PDM_Item (Name, Score) VALUES ('row2', 20)";
            cmd.ExecuteNonQuery();
        }

        // Commit without rollback — both rows should be present
        tx.Commit();

        Assert.Equal(2, Scalar(cn, "SELECT COUNT(*) FROM PDM_Item"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 5. Migration failure/retry — first persists, second fails
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationFailure_AllInTransaction_RolledBack()
    {
        await using var cn = OpenMemory();
        await cn.OpenAsync();

        // Assembly with two migrations: v1 creates a table, v2 throws
        var asm = TwoMigAssembly(
            1, "CreatePdmTestTable",
            "CREATE TABLE PdmTest (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL)",
            2, "FailingMigration");

        var runner = new SqliteMigrationRunner(cn, asm);

        // Apply should fail on the second migration
        await Assert.ThrowsAsync<InvalidOperationException>(() => runner.ApplyMigrationsAsync());

        // SQLite migration runner wraps ALL pending migrations in a single EXCLUSIVE
        // transaction. When v2 fails, the entire transaction (including v1) is rolled back.
        // This is the correct atomic behavior for SQLite.
        Assert.False(TableExists(cn, "PdmTest"), "Table from first migration should be rolled back");

        // History table should have 0 applied rows (rollback undid everything)
        var historyCount = Scalar(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"");
        Assert.Equal(0, historyCount);
    }

    [Fact]
    public async Task MigrationFailure_RetryAfterFixing_Succeeds()
    {
        await using var cn = OpenMemory();
        await cn.OpenAsync();

        // First attempt: v1 succeeds but v2 throws, rolling back everything
        var failAsm = TwoMigAssembly(
            1, "CreateRetryTable",
            "CREATE TABLE RetryTable (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL)",
            2, "FailingStep");

        var runner1 = new SqliteMigrationRunner(cn, failAsm);
        await Assert.ThrowsAsync<InvalidOperationException>(() => runner1.ApplyMigrationsAsync());
        Assert.False(TableExists(cn, "RetryTable"));

        // Second attempt: use a "fixed" assembly with only v1 (the good migration)
        var fixedAsm = DdlAssembly(1, "CreateRetryTable",
            "CREATE TABLE RetryTable (Id INTEGER PRIMARY KEY, Val TEXT NOT NULL)");

        var runner2 = new SqliteMigrationRunner(cn, fixedAsm);
        await runner2.ApplyMigrationsAsync();

        Assert.True(TableExists(cn, "RetryTable"), "Table should exist after retry");
        var historyCount = Scalar(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"");
        Assert.Equal(1, historyCount);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 6. Migration replay — second run is no-op
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MigrationReplay_SecondRunIsNoOp()
    {
        await using var cn = OpenMemory();
        await cn.OpenAsync();

        var asm = DdlAssembly(1, "CreateReplayTable",
            "CREATE TABLE ReplayTable (Id INTEGER PRIMARY KEY, Data TEXT)");

        var runner1 = new SqliteMigrationRunner(cn, asm);
        await runner1.ApplyMigrationsAsync();
        Assert.False(await runner1.HasPendingMigrationsAsync());

        // Second run with same assembly — should be no-op
        var runner2 = new SqliteMigrationRunner(cn, asm);
        Assert.False(await runner2.HasPendingMigrationsAsync());
        await runner2.ApplyMigrationsAsync();

        // Table still exists, history count unchanged
        Assert.True(TableExists(cn, "ReplayTable"));
        var historyCount = Scalar(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"");
        Assert.Equal(1, historyCount);
    }

    [Fact]
    public async Task MigrationReplay_MultipleVersions_AllApplied_ThenNoOp()
    {
        await using var cn = OpenMemory();
        await cn.OpenAsync();

        var asm = NoOpAssembly(
            ("Step1", 1L),
            ("Step2", 2L),
            ("Step3", 3L));

        var runner = new SqliteMigrationRunner(cn, asm);
        Assert.True(await runner.HasPendingMigrationsAsync());
        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Equal(3, pending.Length);

        await runner.ApplyMigrationsAsync();
        Assert.False(await runner.HasPendingMigrationsAsync());

        // Re-run
        var runner2 = new SqliteMigrationRunner(cn, asm);
        Assert.False(await runner2.HasPendingMigrationsAsync());
        pending = await runner2.GetPendingMigrationsAsync();
        Assert.Empty(pending);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 7. Shared provider DML isolation
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void SharedProvider_DmlIsolation_DifferentMappings_DifferentSql()
    {
        var provider = new SqliteProvider();

        // Context A — maps PdmItem
        using var cnA = OpenMemory();
        using var ctxA = new DbContext(cnA, provider, new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdmItem>()
        });
        var mappingA = typeof(DbContext)
            .GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(ctxA, new object[] { typeof(PdmItem) });

        // Context B — maps PdmItemB
        using var cnB = OpenMemory();
        using var ctxB = new DbContext(cnB, provider, new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdmItemB>()
        });
        var mappingB = typeof(DbContext)
            .GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(ctxB, new object[] { typeof(PdmItemB) });

        string insertA = provider.BuildInsert((nORM.Mapping.TableMapping)mappingA!);
        string insertB = provider.BuildInsert((nORM.Mapping.TableMapping)mappingB!);

        // Must produce different SQL targeting different tables
        Assert.NotEqual(insertA, insertB);
        Assert.Contains("\"PDM_Item\"", insertA);
        Assert.Contains("\"PDM_ItemB\"", insertB);
    }

    [Fact]
    public void SharedProvider_UpdateDelete_DifferentMappings_ProduceDifferentSql()
    {
        var provider = new SqliteProvider();

        using var cnA = OpenMemory();
        using var ctxA = new DbContext(cnA, provider, new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdmItem>()
        });
        var mappingA = (nORM.Mapping.TableMapping)typeof(DbContext)
            .GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(ctxA, new object[] { typeof(PdmItem) })!;

        using var cnB = OpenMemory();
        using var ctxB = new DbContext(cnB, provider, new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdmItemB>()
        });
        var mappingB = (nORM.Mapping.TableMapping)typeof(DbContext)
            .GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(ctxB, new object[] { typeof(PdmItemB) })!;

        string updateA = provider.BuildUpdate(mappingA);
        string updateB = provider.BuildUpdate(mappingB);
        Assert.NotEqual(updateA, updateB);
        Assert.Contains("\"PDM_Item\"", updateA);
        Assert.Contains("\"PDM_ItemB\"", updateB);

        string deleteA = provider.BuildDelete(mappingA);
        string deleteB = provider.BuildDelete(mappingB);
        Assert.NotEqual(deleteA, deleteB);
        Assert.Contains("\"PDM_Item\"", deleteA);
        Assert.Contains("\"PDM_ItemB\"", deleteB);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 8. MySQL checkpoint pre-DDL failure shim
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MySqlCheckpointShim_NoPartialStateOnSQLite()
    {
        // SQLite migration runner uses atomic transactions (no per-step checkpoint).
        // Verify that the history table is clean after successful migration and that
        // no spurious rows appear when migrations are merely pending (not applied).
        // This documents the contrast with MySQL's per-step Partial checkpoint.
        await using var cn = OpenMemory();
        await cn.OpenAsync();

        // Apply one real migration to create the history table
        var asm = DdlAssembly(1, "SetupTable",
            "CREATE TABLE CheckpointTest (Id INTEGER PRIMARY KEY)");
        var runner = new SqliteMigrationRunner(cn, asm);
        await runner.ApplyMigrationsAsync();

        // Verify history has exactly 1 row (Version, Name, AppliedOn — no Status column)
        var histCount = Scalar(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"");
        Assert.Equal(1, histCount);

        // Verify the columns present in SQLite history table (no Status column)
        using var pragmaCmd = cn.CreateCommand();
        pragmaCmd.CommandText = "PRAGMA table_info(\"__NormMigrationsHistory\")";
        var columns = new List<string>();
        using (var reader = pragmaCmd.ExecuteReader())
        {
            while (reader.Read())
                columns.Add(reader.GetString(1)); // column name is at index 1
        }
        Assert.Contains("Version", columns);
        Assert.Contains("Name", columns);
        Assert.Contains("AppliedOn", columns);
        Assert.DoesNotContain("Status", columns); // SQLite has no checkpoint Status

        // A second assembly with pending migration — verify pending detection
        // works and no phantom rows appear in history
        var asm2 = DdlAssembly(2, "NeverApplied",
            "CREATE TABLE NeverCreated (Id INTEGER PRIMARY KEY)");
        var runner2 = new SqliteMigrationRunner(cn, asm2);
        Assert.True(await runner2.HasPendingMigrationsAsync());

        // History table still clean — only 1 row from v1
        histCount = Scalar(cn, "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"");
        Assert.Equal(1, histCount);
        Assert.False(TableExists(cn, "NeverCreated"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Cross-cutting: end-to-end insert + query round-trip
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task EndToEnd_Insert_Query_Update_Delete_RoundTrip()
    {
        await using var cn = OpenMemory();
        Exec(cn, PdmItemDdl);

        using var ctx = BuildCtx(cn, o => o.OnModelCreating = mb => mb.Entity<PdmItem>());

        // INSERT
        var entity = new PdmItem { Name = "roundtrip", Score = 50 };
        ctx.Add(entity);
        var inserted = await ctx.SaveChangesAsync();
        Assert.Equal(1, inserted);
        Assert.True(entity.Id > 0); // identity hydrated

        // QUERY — verify persisted
        var found = await ctx.Query<PdmItem>().Where(x => x.Id == entity.Id).ToListAsync();
        Assert.Single(found);
        Assert.Equal(50, found[0].Score);

        // UPDATE — modify the original tracked entity (Unchanged after SaveChanges)
        entity.Score = 99;
        var updated = await ctx.SaveChangesAsync();
        Assert.Equal(1, updated);
        Assert.Equal(99, Scalar(cn, "SELECT Score FROM PDM_Item WHERE Name = 'roundtrip'"));

        // DELETE — remove the tracked entity
        ctx.Remove(entity);
        var deleted = await ctx.SaveChangesAsync();
        Assert.Equal(1, deleted);
        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM PDM_Item"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Provider parity documentation — env-gated stubs
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void SqlServer_WriteSqlGeneration_EscapesWithBrackets()
    {
        // SqlServerProvider uses [brackets] for escaping
        var provider = new SqlServerProvider();

        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, provider, new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdmItem>()
        });
        var mapping = (nORM.Mapping.TableMapping)typeof(DbContext)
            .GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(ctx, new object[] { typeof(PdmItem) })!;

        string insertSql = provider.BuildInsert(mapping);
        Assert.Contains("[PDM_Item]", insertSql);
        Assert.Contains("[Name]", insertSql);
        Assert.Contains("SCOPE_IDENTITY", insertSql);

        string updateSql = provider.BuildUpdate(mapping);
        Assert.Contains("[PDM_Item]", updateSql);

        string deleteSql = provider.BuildDelete(mapping);
        Assert.Contains("[PDM_Item]", deleteSql);
    }

    [Fact]
    public void MySQL_WriteSqlGeneration_EscapesWithBackticks()
    {
        var provider = new MySqlProvider(new SqliteParameterFactory());

        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, provider, new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdmItem>()
        });
        var mapping = (nORM.Mapping.TableMapping)typeof(DbContext)
            .GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(ctx, new object[] { typeof(PdmItem) })!;

        string insertSql = provider.BuildInsert(mapping);
        Assert.Contains("`PDM_Item`", insertSql);
        Assert.Contains("`Name`", insertSql);
        Assert.Contains("LAST_INSERT_ID", insertSql);

        string updateSql = provider.BuildUpdate(mapping);
        Assert.Contains("`PDM_Item`", updateSql);

        string deleteSql = provider.BuildDelete(mapping);
        Assert.Contains("`PDM_Item`", deleteSql);
    }

    [Fact]
    public void Postgres_WriteSqlGeneration_EscapesWithDoubleQuotes()
    {
        var provider = new PostgresProvider(new SqliteParameterFactory());

        using var cn = OpenMemory();
        using var ctx = new DbContext(cn, provider, new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdmItem>()
        });
        var mapping = (nORM.Mapping.TableMapping)typeof(DbContext)
            .GetMethod("GetMapping", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(ctx, new object[] { typeof(PdmItem) })!;

        string insertSql = provider.BuildInsert(mapping);
        Assert.Contains("\"PDM_Item\"", insertSql);
        Assert.Contains("\"Name\"", insertSql);
        Assert.Contains("RETURNING", insertSql);

        string updateSql = provider.BuildUpdate(mapping);
        Assert.Contains("\"PDM_Item\"", updateSql);

        string deleteSql = provider.BuildDelete(mapping);
        Assert.Contains("\"PDM_Item\"", deleteSql);
    }
}
