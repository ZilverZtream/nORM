using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using MigrationBase = nORM.Migration.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ══════════════════════════════════════════════════════════════════════════════
// Gate 3.5 — Per-subsystem coverage: query, save, materialization, migration,
//            bulk, and cache subsystems.  All tests use SQLite in-memory.
// ══════════════════════════════════════════════════════════════════════════════

// ── Entity types ─────────────────────────────────────────────────────────────

[Table("G35_Product")]
public class G35Product
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = "";
    public decimal Price { get; set; }
    public int Quantity { get; set; }
    public bool InStock { get; set; }
}

[Table("G35_TypedRow")]
public class G35TypedRow
{
    [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? TextVal { get; set; }
    public int IntVal { get; set; }
    public bool BoolVal { get; set; }
    public decimal DecimalVal { get; set; }
    public DateTime DateVal { get; set; }
    public string? GuidText { get; set; } // SQLite stores Guid as text
    public string? NullableText { get; set; }
}

[Table("G35_Renamed")]
public class G35RenamedEntity
{
    [Key]
    public int Id { get; set; }

    [Column("display_name")]
    public string DisplayName { get; set; } = "";

    [Column("unit_price")]
    public decimal UnitPrice { get; set; }
}

[Table("G35_BulkItem")]
public class G35BulkItem
{
    [Key]
    public int Id { get; set; }
    public string Label { get; set; } = "";
    public int Score { get; set; }
}

[Table("G35_CacheRow")]
public class G35CacheRow
{
    [Key]
    public int Id { get; set; }
    public string Data { get; set; } = "";
}

public class SubsystemRoundTripTests
{
    // ── Shared helpers ───────────────────────────────────────────────────────

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

    private static long CountRows(SqliteConnection cn, string table)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT COUNT(*) FROM \"{table}\"";
        return (long)cmd.ExecuteScalar()!;
    }

    private static object? ScalarQuery(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 1. QUERY PIPELINE
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Query_Where_ReturnsFilteredResults()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Price REAL, Quantity INTEGER, InStock INTEGER)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Widget', 9.99, 10, 1)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Gadget', 19.99, 0, 0)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Gizmo', 5.50, 25, 1)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var inStock = ctx.Query<G35Product>().Where(p => p.InStock).ToList();

        Assert.Equal(2, inStock.Count);
        Assert.All(inStock, p => Assert.True(p.InStock));
    }

    [Fact]
    public void Query_Select_ProjectsCorrectly()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Price REAL, Quantity INTEGER, InStock INTEGER)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Alpha', 10.00, 5, 1)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Beta', 20.00, 3, 1)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var names = ctx.Query<G35Product>().Select(p => p.Name).ToList();

        Assert.Equal(2, names.Count);
        Assert.Contains("Alpha", names);
        Assert.Contains("Beta", names);
    }

    [Fact]
    public void Query_OrderBy_ReturnsSortedResults()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Price REAL, Quantity INTEGER, InStock INTEGER)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Charlie', 30.00, 1, 1)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Alpha', 10.00, 3, 1)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Bravo', 20.00, 2, 1)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var sorted = ctx.Query<G35Product>().OrderBy(p => p.Name).ToList();

        Assert.Equal("Alpha", sorted[0].Name);
        Assert.Equal("Bravo", sorted[1].Name);
        Assert.Equal("Charlie", sorted[2].Name);
    }

    [Fact]
    public void Query_SkipTake_ReturnsPaginatedResults()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Price REAL, Quantity INTEGER, InStock INTEGER)");
        for (int i = 1; i <= 10; i++)
            Exec(cn, $"INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Item{i}', {i}.00, {i}, 1)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var page = ctx.Query<G35Product>().OrderBy(p => p.Id).Skip(3).Take(4).ToList();

        Assert.Equal(4, page.Count);
        Assert.Equal("Item4", page[0].Name);
        Assert.Equal("Item7", page[3].Name);
    }

    [Fact]
    public void Query_WhereWithMultipleConditions_FiltersCorrectly()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Price REAL, Quantity INTEGER, InStock INTEGER)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Cheap', 5.00, 100, 1)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Mid', 15.00, 50, 1)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('Pricey', 25.00, 10, 1)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('OutOfStock', 5.00, 0, 0)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = ctx.Query<G35Product>()
            .Where(p => p.Price < 20m && p.InStock)
            .OrderBy(p => p.Price)
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Cheap", results[0].Name);
        Assert.Equal("Mid", results[1].Name);
    }

    [Fact]
    public void Query_OrderByDescending_ReturnsSortedDescending()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Price REAL, Quantity INTEGER, InStock INTEGER)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('A', 10.00, 1, 1)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('B', 30.00, 2, 1)");
        Exec(cn, "INSERT INTO G35_Product (Name, Price, Quantity, InStock) VALUES ('C', 20.00, 3, 1)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var sorted = ctx.Query<G35Product>().OrderByDescending(p => p.Price).ToList();

        Assert.Equal("B", sorted[0].Name);
        Assert.Equal("C", sorted[1].Name);
        Assert.Equal("A", sorted[2].Name);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 2. SAVE PIPELINE
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Save_InsertReadUpdateDelete_RoundTrip()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Price REAL, Quantity INTEGER, InStock INTEGER)");

        // INSERT
        using var ctx = new DbContext(cn, new SqliteProvider());
        var product = new G35Product { Name = "TestProduct", Price = 12.50m, Quantity = 7, InStock = true };
        ctx.Add(product);
        await ctx.SaveChangesAsync();
        Assert.NotEqual(0, product.Id);
        var insertedId = product.Id;

        // READ
        var loaded = ctx.Query<G35Product>().Where(p => p.Id == insertedId).ToList();
        Assert.Single(loaded);
        Assert.Equal("TestProduct", loaded[0].Name);
        Assert.Equal(12.50m, loaded[0].Price);

        // UPDATE via raw SQL + Attach pattern (to avoid identity map returning cached entity)
        using var ctx2 = new DbContext(cn, new SqliteProvider());
        var toUpdate = new G35Product { Id = insertedId, Name = "UpdatedProduct", Price = 15.00m, Quantity = 10, InStock = true };
        ctx2.Attach(toUpdate);
        toUpdate.Name = "UpdatedProduct";
        var updated = await ctx2.UpdateAsync(toUpdate);
        Assert.Equal(1, updated);

        // Verify update via raw SQL
        var nameAfterUpdate = (string?)ScalarQuery(cn, $"SELECT Name FROM G35_Product WHERE Id = {insertedId}");
        Assert.Equal("UpdatedProduct", nameAfterUpdate);

        // DELETE
        var deleted = await ctx2.DeleteAsync(toUpdate);
        Assert.Equal(1, deleted);

        Assert.Equal(0L, CountRows(cn, "G35_Product"));
    }

    [Fact]
    public async Task Save_MultipleInserts_AllPersisted()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Price REAL, Quantity INTEGER, InStock INTEGER)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new G35Product { Name = "P1", Price = 1m, Quantity = 1, InStock = true });
        ctx.Add(new G35Product { Name = "P2", Price = 2m, Quantity = 2, InStock = false });
        ctx.Add(new G35Product { Name = "P3", Price = 3m, Quantity = 3, InStock = true });
        await ctx.SaveChangesAsync();

        Assert.Equal(3L, CountRows(cn, "G35_Product"));
    }

    [Fact]
    public async Task Save_Insert_AssignsAutoIncrementId()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_Product (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Price REAL, Quantity INTEGER, InStock INTEGER)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var e1 = new G35Product { Name = "First", Price = 1m, Quantity = 1, InStock = true };
        var e2 = new G35Product { Name = "Second", Price = 2m, Quantity = 2, InStock = true };
        ctx.Add(e1);
        ctx.Add(e2);
        await ctx.SaveChangesAsync();

        Assert.NotEqual(0, e1.Id);
        Assert.NotEqual(0, e2.Id);
        Assert.NotEqual(e1.Id, e2.Id);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 3. MATERIALIZATION
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Materialization_AllBasicClrTypes_RoundTrip()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE G35_TypedRow (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            TextVal TEXT, IntVal INTEGER, BoolVal INTEGER,
            DecimalVal REAL, DateVal TEXT, GuidText TEXT, NullableText TEXT)");

        var now = new DateTime(2026, 3, 19, 12, 0, 0, DateTimeKind.Unspecified);
        var guidStr = Guid.NewGuid().ToString();

        Exec(cn, $@"INSERT INTO G35_TypedRow
            (TextVal, IntVal, BoolVal, DecimalVal, DateVal, GuidText, NullableText)
            VALUES ('hello', 42, 1, 99.95, '{now:yyyy-MM-dd HH:mm:ss}', '{guidStr}', NULL)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var rows = ctx.Query<G35TypedRow>().ToList();

        Assert.Single(rows);
        var r = rows[0];
        Assert.Equal("hello", r.TextVal);
        Assert.Equal(42, r.IntVal);
        Assert.True(r.BoolVal);
        Assert.Equal(99.95m, r.DecimalVal);
        Assert.Equal(now, r.DateVal);
        Assert.Equal(guidStr, r.GuidText);
        Assert.Null(r.NullableText);
    }

    [Fact]
    public void Materialization_NullableColumns_HandledCorrectly()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE G35_TypedRow (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            TextVal TEXT, IntVal INTEGER, BoolVal INTEGER,
            DecimalVal REAL, DateVal TEXT, GuidText TEXT, NullableText TEXT)");

        Exec(cn, "INSERT INTO G35_TypedRow (TextVal, IntVal, BoolVal, DecimalVal, DateVal, GuidText, NullableText) VALUES (NULL, 0, 0, 0, '2020-01-01', NULL, 'has-value')");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var rows = ctx.Query<G35TypedRow>().ToList();

        Assert.Single(rows);
        Assert.Null(rows[0].TextVal);
        Assert.Null(rows[0].GuidText);
        Assert.Equal("has-value", rows[0].NullableText);
    }

    [Fact]
    public void Materialization_RenamedColumn_MapsCorrectly()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_Renamed (Id INTEGER PRIMARY KEY, display_name TEXT NOT NULL, unit_price REAL NOT NULL)");
        Exec(cn, "INSERT INTO G35_Renamed (Id, display_name, unit_price) VALUES (1, 'RenamedItem', 42.50)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var rows = ctx.Query<G35RenamedEntity>().ToList();

        Assert.Single(rows);
        Assert.Equal(1, rows[0].Id);
        Assert.Equal("RenamedItem", rows[0].DisplayName);
        Assert.Equal(42.50m, rows[0].UnitPrice);
    }

    [Fact]
    public void Materialization_MultipleRows_AllMaterialized()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE G35_TypedRow (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            TextVal TEXT, IntVal INTEGER, BoolVal INTEGER,
            DecimalVal REAL, DateVal TEXT, GuidText TEXT, NullableText TEXT)");

        for (int i = 1; i <= 5; i++)
            Exec(cn, $"INSERT INTO G35_TypedRow (TextVal, IntVal, BoolVal, DecimalVal, DateVal, GuidText, NullableText) VALUES ('row{i}', {i * 10}, {i % 2}, {i}.{i}, '2026-01-0{i}', NULL, NULL)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var rows = ctx.Query<G35TypedRow>().OrderBy(r => r.Id).ToList();

        Assert.Equal(5, rows.Count);
        Assert.Equal("row1", rows[0].TextVal);
        Assert.Equal(50, rows[4].IntVal);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 4. MIGRATION
    // ═══════════════════════════════════════════════════════════════════════

    /// <summary>
    /// Builds a dynamic assembly with migration classes to avoid polluting the test assembly.
    /// </summary>
    private static Assembly BuildDynMigrationAssembly(params (string Name, long Version, string UpSql)[] migrations)
    {
        var asmName = new AssemblyName("G35DynMig_" + Guid.NewGuid().ToString("N"));
        var ab = AssemblyBuilder.DefineDynamicAssembly(asmName, AssemblyBuilderAccess.Run);
        var mb = ab.DefineDynamicModule("MainModule");

        var baseMigType = typeof(MigrationBase);
        var baseCtor = baseMigType.GetConstructor(
            BindingFlags.NonPublic | BindingFlags.Instance,
            null, new[] { typeof(long), typeof(string) }, null)!;

        var upAbstract = baseMigType.GetMethod("Up", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;
        var downAbstract = baseMigType.GetMethod("Down", new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) })!;

        // We need a helper to create commands - use a static method on a real type
        var createCmdMethod = typeof(DbConnection).GetMethod("CreateCommand")!;
        var setCommandTextProp = typeof(DbCommand).GetProperty("CommandText")!.GetSetMethod()!;
        var setTransactionProp = typeof(DbCommand).GetProperty("Transaction")!.GetSetMethod()!;
        var execNonQuery = typeof(DbCommand).GetMethod("ExecuteNonQuery")!;

        foreach (var (typeName, version, upSql) in migrations)
        {
            var tb = mb.DefineType(typeName,
                TypeAttributes.Public | TypeAttributes.Class, baseMigType);

            // constructor: base(version, typeName)
            var ctor = tb.DefineConstructor(MethodAttributes.Public,
                CallingConventions.Standard, Type.EmptyTypes);
            var il = ctor.GetILGenerator();
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Ldc_I8, version);
            il.Emit(OpCodes.Ldstr, typeName);
            il.Emit(OpCodes.Call, baseCtor);
            il.Emit(OpCodes.Ret);

            // Up override: creates a command, sets SQL, executes
            var up = tb.DefineMethod("Up",
                MethodAttributes.Public | MethodAttributes.Virtual | MethodAttributes.ReuseSlot,
                typeof(void), new[] { typeof(DbConnection), typeof(DbTransaction), typeof(CancellationToken) });
            var upIl = up.GetILGenerator();
            // var cmd = connection.CreateCommand()
            upIl.DeclareLocal(typeof(DbCommand));
            upIl.Emit(OpCodes.Ldarg_1); // connection
            upIl.Emit(OpCodes.Callvirt, createCmdMethod);
            upIl.Emit(OpCodes.Stloc_0);
            // cmd.Transaction = transaction
            upIl.Emit(OpCodes.Ldloc_0);
            upIl.Emit(OpCodes.Ldarg_2); // transaction
            upIl.Emit(OpCodes.Callvirt, setTransactionProp);
            // cmd.CommandText = upSql
            upIl.Emit(OpCodes.Ldloc_0);
            upIl.Emit(OpCodes.Ldstr, upSql);
            upIl.Emit(OpCodes.Callvirt, setCommandTextProp);
            // cmd.ExecuteNonQuery()
            upIl.Emit(OpCodes.Ldloc_0);
            upIl.Emit(OpCodes.Callvirt, execNonQuery);
            upIl.Emit(OpCodes.Pop);
            upIl.Emit(OpCodes.Ret);
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

    [Fact]
    public async Task Migration_CreatesHistoryTable_AndTracksApplied()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var asm = BuildDynMigrationAssembly(
            ("CreateG35Table", 1, "CREATE TABLE G35_MigTest (Id INTEGER PRIMARY KEY, Val TEXT)"),
            ("AddIndexG35", 2, "CREATE INDEX idx_g35 ON G35_MigTest (Val)")
        );

        var runner = new nORM.Migration.SqliteMigrationRunner(cn, asm);
        Assert.True(await runner.HasPendingMigrationsAsync());

        var pending = await runner.GetPendingMigrationsAsync();
        Assert.Equal(2, pending.Length);
        Assert.Equal("1_CreateG35Table", pending[0]);
        Assert.Equal("2_AddIndexG35", pending[1]);

        await runner.ApplyMigrationsAsync();

        Assert.False(await runner.HasPendingMigrationsAsync());

        // Verify the history table was created and populated
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM \"__NormMigrationsHistory\"";
        var count = Convert.ToInt64(await cmd.ExecuteScalarAsync());
        Assert.Equal(2L, count);

        // Verify the actual migration DDL executed
        var tableExists = (string?)ScalarQuery(cn, "SELECT name FROM sqlite_master WHERE type='table' AND name='G35_MigTest'");
        Assert.Equal("G35_MigTest", tableExists);
    }

    [Fact]
    public async Task Migration_NoPending_IsIdempotent()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        var asm = BuildDynMigrationAssembly(
            ("CreateSingleTable", 1, "CREATE TABLE G35_Single (Id INTEGER PRIMARY KEY)")
        );

        var runner = new nORM.Migration.SqliteMigrationRunner(cn, asm);
        await runner.ApplyMigrationsAsync();
        Assert.False(await runner.HasPendingMigrationsAsync());

        // Apply again - should be idempotent (no error)
        await runner.ApplyMigrationsAsync();
        Assert.False(await runner.HasPendingMigrationsAsync());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 5. BULK OPERATIONS
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Bulk_InsertAsync_InsertsAllRows()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_BulkItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Score INTEGER NOT NULL)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var items = Enumerable.Range(1, 50).Select(i => new G35BulkItem { Id = i, Label = $"Item{i}", Score = i * 10 }).ToList();

        var inserted = await ctx.BulkInsertAsync(items);
        Assert.Equal(50, inserted);
        Assert.Equal(50L, CountRows(cn, "G35_BulkItem"));

        // Verify specific row via raw SQL
        var label = (string?)ScalarQuery(cn, "SELECT Label FROM G35_BulkItem WHERE Id = 25");
        Assert.Equal("Item25", label);
    }

    [Fact]
    public async Task Bulk_UpdateAsync_UpdatesExistingRows()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_BulkItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Score INTEGER NOT NULL)");
        Exec(cn, "INSERT INTO G35_BulkItem (Id, Label, Score) VALUES (1, 'Original1', 10)");
        Exec(cn, "INSERT INTO G35_BulkItem (Id, Label, Score) VALUES (2, 'Original2', 20)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var updates = new List<G35BulkItem>
        {
            new() { Id = 1, Label = "Updated1", Score = 100 },
            new() { Id = 2, Label = "Updated2", Score = 200 }
        };

        var affected = await ctx.BulkUpdateAsync(updates);
        Assert.Equal(2, affected);

        // Verify via raw SQL (fresh context to avoid identity map)
        var label1 = (string?)ScalarQuery(cn, "SELECT Label FROM G35_BulkItem WHERE Id = 1");
        Assert.Equal("Updated1", label1);

        var score2 = Convert.ToInt32(ScalarQuery(cn, "SELECT Score FROM G35_BulkItem WHERE Id = 2"));
        Assert.Equal(200, score2);
    }

    [Fact]
    public async Task Bulk_DeleteAsync_RemovesRows()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_BulkItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Score INTEGER NOT NULL)");
        Exec(cn, "INSERT INTO G35_BulkItem (Id, Label, Score) VALUES (1, 'A', 10)");
        Exec(cn, "INSERT INTO G35_BulkItem (Id, Label, Score) VALUES (2, 'B', 20)");
        Exec(cn, "INSERT INTO G35_BulkItem (Id, Label, Score) VALUES (3, 'C', 30)");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var toDelete = new List<G35BulkItem>
        {
            new() { Id = 1, Label = "A", Score = 10 },
            new() { Id = 3, Label = "C", Score = 30 }
        };

        await ctx.BulkDeleteAsync(toDelete);

        Assert.Equal(1L, CountRows(cn, "G35_BulkItem"));
        var remaining = (string?)ScalarQuery(cn, "SELECT Label FROM G35_BulkItem WHERE Id = 2");
        Assert.Equal("B", remaining);
    }

    [Fact]
    public async Task Bulk_InsertUpdateDelete_FullLifecycle()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_BulkItem (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, Score INTEGER NOT NULL)");

        // Insert
        using var ctx1 = new DbContext(cn, new SqliteProvider());
        var items = new List<G35BulkItem>
        {
            new() { Id = 10, Label = "Ten", Score = 100 },
            new() { Id = 20, Label = "Twenty", Score = 200 },
            new() { Id = 30, Label = "Thirty", Score = 300 }
        };
        await ctx1.BulkInsertAsync(items);
        Assert.Equal(3L, CountRows(cn, "G35_BulkItem"));

        // Update
        using var ctx2 = new DbContext(cn, new SqliteProvider());
        var updatedItems = new List<G35BulkItem>
        {
            new() { Id = 10, Label = "TenUpdated", Score = 1000 },
            new() { Id = 30, Label = "ThirtyUpdated", Score = 3000 }
        };
        await ctx2.BulkUpdateAsync(updatedItems);

        var newLabel = (string?)ScalarQuery(cn, "SELECT Label FROM G35_BulkItem WHERE Id = 10");
        Assert.Equal("TenUpdated", newLabel);

        // Delete
        using var ctx3 = new DbContext(cn, new SqliteProvider());
        await ctx3.BulkDeleteAsync(new[] { new G35BulkItem { Id = 20, Label = "", Score = 0 } });
        Assert.Equal(2L, CountRows(cn, "G35_BulkItem"));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // 6. CACHE
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Cache_SetAndTryGet_ReturnsStoredValue()
    {
        using var cache = new NormMemoryCacheProvider();

        cache.Set("key1", "value1", TimeSpan.FromMinutes(5), new[] { "tag1" });

        Assert.True(cache.TryGet<string>("key1", out var result));
        Assert.Equal("value1", result);
    }

    [Fact]
    public void Cache_TryGet_ReturnsFalseForMissingKey()
    {
        using var cache = new NormMemoryCacheProvider();

        Assert.False(cache.TryGet<string>("nonexistent", out var result));
        Assert.Null(result);
    }

    [Fact]
    public void Cache_InvalidateTag_EvictsTaggedEntries()
    {
        using var cache = new NormMemoryCacheProvider();

        cache.Set("k1", "v1", TimeSpan.FromMinutes(5), new[] { "products" });
        cache.Set("k2", "v2", TimeSpan.FromMinutes(5), new[] { "products" });
        cache.Set("k3", "v3", TimeSpan.FromMinutes(5), new[] { "orders" });

        Assert.True(cache.TryGet<string>("k1", out _));
        Assert.True(cache.TryGet<string>("k2", out _));
        Assert.True(cache.TryGet<string>("k3", out _));

        cache.InvalidateTag("products");

        Assert.False(cache.TryGet<string>("k1", out _));
        Assert.False(cache.TryGet<string>("k2", out _));
        // "orders" tag not invalidated — k3 still present
        Assert.True(cache.TryGet<string>("k3", out var v3));
        Assert.Equal("v3", v3);
    }

    [Fact]
    public void Cache_Expiration_EntryEvictedAfterTimeout()
    {
        using var cache = new NormMemoryCacheProvider();

        // Set with 1ms expiration — effectively immediate
        cache.Set("ephemeral", "data", TimeSpan.FromMilliseconds(1), new[] { "tag" });

        // After a small delay the entry should be gone
        Thread.Sleep(50);

        Assert.False(cache.TryGet<string>("ephemeral", out _));
    }

    [Fact]
    public void Cache_MultipleTagsOnSingleEntry_AnyTagInvalidatesIt()
    {
        using var cache = new NormMemoryCacheProvider();

        cache.Set("multi", "val", TimeSpan.FromMinutes(5), new[] { "tagA", "tagB" });
        Assert.True(cache.TryGet<string>("multi", out _));

        // Invalidating just one of the tags should evict the entry
        cache.InvalidateTag("tagB");
        Assert.False(cache.TryGet<string>("multi", out _));
    }

    [Fact]
    public async Task Cache_Cacheable_IntegrationWithQuery()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE G35_CacheRow (Id INTEGER PRIMARY KEY, Data TEXT NOT NULL)");
        Exec(cn, "INSERT INTO G35_CacheRow (Id, Data) VALUES (1, 'cached')");

        using var cache = new NormMemoryCacheProvider();
        var opts = new DbContextOptions { CacheProvider = cache };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // First query — hits DB and caches
        var first = await ctx.Query<G35CacheRow>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Single(first);
        Assert.Equal("cached", first[0].Data);

        // Insert a new row directly via raw SQL (bypasses cache)
        Exec(cn, "INSERT INTO G35_CacheRow (Id, Data) VALUES (2, 'new')");

        // Second query — should return cached result (still 1 row)
        var second = await ctx.Query<G35CacheRow>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Single(second); // cached — doesn't see the new row

        // Invalidate cache
        cache.InvalidateTag("G35_CacheRow");

        // Third query — cache miss, hits DB, sees both rows
        var third = await ctx.Query<G35CacheRow>()
            .Cacheable(TimeSpan.FromMinutes(5))
            .ToListAsync();
        Assert.Equal(2, third.Count);
    }

    [Fact]
    public void Cache_InvalidateTag_Twice_DoesNotThrow()
    {
        using var cache = new NormMemoryCacheProvider();

        cache.Set("x", 42, TimeSpan.FromMinutes(5), new[] { "t" });
        cache.InvalidateTag("t");
        // Second invalidation of same tag should be a no-op, not throw
        cache.InvalidateTag("t");

        Assert.False(cache.TryGet<int>("x", out _));
    }
}
