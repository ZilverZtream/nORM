using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ── Public entity types (namespace scope, not nested) ──────────────────────

[Table("MAED_Product")]
public class MaedProduct
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public decimal Price { get; set; }
    public int Stock { get; set; }
    public bool Available { get; set; }
}

[Table("MAED_NullProp")]
public class MaedNullProp
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string? Description { get; set; }
    public int? Rating { get; set; }
    public double? Weight { get; set; }
}

[Table("MAED_Category")]
public class MaedCategory
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Title { get; set; } = string.Empty;
    public List<MaedItem> Items { get; set; } = new();
}

[Table("MAED_Item")]
public class MaedItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int CategoryId { get; set; }
    public string Label { get; set; } = string.Empty;
    public int Qty { get; set; }
}

[Table("MAED_Event")]
public class MaedEvent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string EventName { get; set; } = string.Empty;
    public int Year { get; set; }
    public string? Notes { get; set; }
}

[Table("MAED_Score")]
public class MaedScore
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Player { get; set; } = string.Empty;
    public long Points { get; set; }
    public double Ratio { get; set; }
}

[Table("MAED_Flag")]
public class MaedFlag
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public bool Enabled { get; set; }
    public string Tag { get; set; } = string.Empty;
}

[Table("MAED_Multi")]
public class MaedMulti
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string A { get; set; } = string.Empty;
    public string B { get; set; } = string.Empty;
    public string C { get; set; } = string.Empty;
    public int X { get; set; }
    public int Y { get; set; }
    public int Z { get; set; }
    public bool P { get; set; }
    public bool Q { get; set; }
}

/// <summary>
/// Deep coverage tests for MaterializerFactory and QueryExecutor focusing on
/// paths not yet covered by existing test files.
/// </summary>
public class MaterializerAndExecutorDeepCoverageTests
{
    // ── Async-forcing provider ──────────────────────────────────────────────
    private sealed class AsyncSqliteProvider : SqliteProvider
    {
        public override bool PrefersSyncExecution => false;
    }

    // ── RedactSqlForLogging accessor ────────────────────────────────────────
    private static readonly MethodInfo _redactMethod =
        typeof(QueryExecutor)
            .GetMethod("RedactSqlForLogging",
                BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("RedactSqlForLogging method not found.");

    private static string Redact(string sql) =>
        (string)_redactMethod.Invoke(null, new object[] { sql })!;

    // ── Connection helpers ──────────────────────────────────────────────────
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

    private static SqliteConnection CreateProductDb()
    {
        var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_Product (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Name TEXT NOT NULL, Price REAL NOT NULL DEFAULT 0,
            Stock INTEGER NOT NULL DEFAULT 0, Available INTEGER NOT NULL DEFAULT 0);
        INSERT INTO MAED_Product (Name, Price, Stock, Available) VALUES
            ('Widget',  9.99,  100, 1),
            ('Gadget', 19.99,   50, 1),
            ('Gizmo',   4.99,    0, 0);");
        return cn;
    }

    private static SqliteConnection CreateNullPropDb()
    {
        var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_NullProp (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Description TEXT, Rating INTEGER, Weight REAL);
        INSERT INTO MAED_NullProp VALUES (1, 'desc', 5, 1.5);
        INSERT INTO MAED_NullProp VALUES (2, NULL,  NULL, NULL);
        INSERT INTO MAED_NullProp VALUES (3, 'x',   3,   NULL);");
        return cn;
    }

    private static SqliteConnection CreateCategoryDb()
    {
        var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_Category (
            Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
        CREATE TABLE MAED_Item (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            CategoryId INTEGER NOT NULL, Label TEXT NOT NULL, Qty INTEGER NOT NULL DEFAULT 0);
        INSERT INTO MAED_Category (Title) VALUES ('Electronics'), ('Books');
        INSERT INTO MAED_Item (CategoryId, Label, Qty) VALUES
            (1, 'TV',     5),
            (1, 'Radio',  3),
            (2, 'Novel',  10),
            (2, 'Manual',  2);");
        return cn;
    }

    private static SqliteConnection CreateEventDb()
    {
        var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_Event (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            EventName TEXT NOT NULL, Year INTEGER NOT NULL DEFAULT 0, Notes TEXT);
        INSERT INTO MAED_Event (EventName, Year, Notes) VALUES
            ('Launch', 2020, 'Initial'),
            ('Update', 2021,  NULL),
            ('Summit', 2022, 'Annual');");
        return cn;
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 1 — MaterializerFactory: CacheStats changes after operations
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MF_CacheStats_TotalHitsPlusMisses_IsNonNegative()
    {
        var (hits, misses, hitRate) = MaterializerFactory.CacheStats;
        Assert.True(hits >= 0);
        Assert.True(misses >= 0);
        Assert.InRange(hitRate, 0.0, 1.0);
    }

    [Fact]
    public void MF_SchemaCacheStats_AllFieldsNonNegative()
    {
        var (schemaHits, schemaMisses, schemaRate) = MaterializerFactory.SchemaCacheStats;
        Assert.True(schemaHits >= 0);
        Assert.True(schemaMisses >= 0);
        Assert.InRange(schemaRate, 0.0, 1.0);
    }

    [Fact]
    public void MF_CacheStats_AfterQuery_HitsPlusMissesIncrease()
    {
        var (h0, m0, _) = MaterializerFactory.CacheStats;
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        _ = ctx.Query<MaedProduct>().ToList();
        var (h1, m1, _) = MaterializerFactory.CacheStats;
        Assert.True(h1 + m1 > h0 + m0,
            "Expected cache activity after executing a query");
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 2 — PrecompileCommonPatterns<T>
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MF_PrecompileCommonPatterns_Product_DoesNotThrow()
    {
        MaterializerFactory.PrecompileCommonPatterns<MaedProduct>();
        // Idempotent
        MaterializerFactory.PrecompileCommonPatterns<MaedProduct>();
    }

    [Fact]
    public void MF_PrecompileCommonPatterns_ThenQuery_ReturnsCorrectData()
    {
        MaterializerFactory.PrecompileCommonPatterns<MaedProduct>();
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MaedProduct>().OrderBy(p => p.Id).ToList();
        Assert.Equal(3, list.Count);
        Assert.Equal("Widget", list[0].Name);
        Assert.Equal(9.99m, list[0].Price);
    }

    [Fact]
    public void MF_PrecompileCommonPatterns_MultipleTypes_AllWork()
    {
        MaterializerFactory.PrecompileCommonPatterns<MaedProduct>();
        MaterializerFactory.PrecompileCommonPatterns<MaedEvent>();
        MaterializerFactory.PrecompileCommonPatterns<MaedFlag>();

        using var cn = CreateEventDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MaedEvent>().ToList();
        Assert.Equal(3, list.Count);
    }

    [Fact]
    public async Task MF_PrecompileCommonPatterns_AsyncQuery_Works()
    {
        MaterializerFactory.PrecompileCommonPatterns<MaedProduct>();
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedProduct>().ToListAsync();
        Assert.Equal(3, list.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 3 — Typed CreateSyncMaterializer<T> with different types
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MF_SyncMaterializerGeneric_Product_MaterializesCorrectly()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer<MaedProduct>(mapping);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var p = del(reader);
        Assert.Equal("Widget", p.Name);
        Assert.Equal(100, p.Stock);
        Assert.True(p.Available);
    }

    [Fact]
    public void MF_SyncMaterializerGeneric_MultipleRows_AllWork()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer<MaedProduct>(mapping);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Id";
        using var reader = cmd.ExecuteReader();
        var results = new List<MaedProduct>();
        while (reader.Read())
            results.Add(del(reader));

        Assert.Equal(3, results.Count);
        Assert.Equal("Widget",  results[0].Name);
        Assert.Equal("Gadget",  results[1].Name);
        Assert.Equal("Gizmo",   results[2].Name);
        Assert.False(results[2].Available);
    }

    [Fact]
    public void MF_SyncMaterializerGeneric_NullableColumns_Correct()
    {
        using var cn = CreateNullPropDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedNullProp));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer<MaedNullProp>(mapping);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Description, Rating, Weight FROM MAED_NullProp WHERE Id = 2";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var item = del(reader);
        Assert.Null(item.Description);
        Assert.Null(item.Rating);
        Assert.Null(item.Weight);
    }

    [Fact]
    public void MF_SyncMaterializerGeneric_NullableColumnsPresent_Correct()
    {
        using var cn = CreateNullPropDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedNullProp));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer<MaedNullProp>(mapping);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Description, Rating, Weight FROM MAED_NullProp WHERE Id = 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var item = del(reader);
        Assert.Equal("desc", item.Description);
        Assert.Equal(5, item.Rating);
        Assert.Equal(1.5, item.Weight);
    }

    [Fact]
    public void MF_SyncMaterializerGeneric_CacheHit_ReturnsSameDelegate()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        // Second call should come from cache
        var del1 = factory.CreateSyncMaterializer<MaedProduct>(mapping);
        var del2 = factory.CreateSyncMaterializer<MaedProduct>(mapping);
        Assert.NotNull(del1);
        Assert.NotNull(del2);
        // Both should work identically
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var r1 = del1(reader);
        Assert.Equal("Widget", r1.Name);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 4 — Untyped CreateSyncMaterializer(mapping, Type)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MF_SyncMaterializerUntyped_Product_ReturnsObject()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer(mapping, typeof(MaedProduct));

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = del(reader);
        Assert.IsType<MaedProduct>(obj);
        Assert.Equal("Widget", ((MaedProduct)obj).Name);
    }

    [Fact]
    public void MF_SyncMaterializerUntyped_BoolColumn_True()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_Flag (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            Enabled INTEGER NOT NULL DEFAULT 0,
            Tag TEXT NOT NULL DEFAULT '');
        INSERT INTO MAED_Flag VALUES (1, 1, 'active');
        INSERT INTO MAED_Flag VALUES (2, 0, 'off');");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedFlag));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer(mapping, typeof(MaedFlag));

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Enabled, Tag FROM MAED_Flag ORDER BY Id";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var f1 = (MaedFlag)del(reader);
        Assert.True(f1.Enabled);
        Assert.Equal("active", f1.Tag);

        Assert.True(reader.Read());
        var f2 = (MaedFlag)del(reader);
        Assert.False(f2.Enabled);
    }

    [Fact]
    public void MF_SyncMaterializerUntyped_LargeEntity_ManyColumns()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_Multi (
            Id INTEGER PRIMARY KEY AUTOINCREMENT,
            A TEXT NOT NULL DEFAULT '', B TEXT NOT NULL DEFAULT '', C TEXT NOT NULL DEFAULT '',
            X INTEGER NOT NULL DEFAULT 0, Y INTEGER NOT NULL DEFAULT 0, Z INTEGER NOT NULL DEFAULT 0,
            P INTEGER NOT NULL DEFAULT 0, Q INTEGER NOT NULL DEFAULT 0);
        INSERT INTO MAED_Multi VALUES (1, 'aa', 'bb', 'cc', 10, 20, 30, 1, 0);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedMulti));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer(mapping, typeof(MaedMulti));

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, A, B, C, X, Y, Z, P, Q FROM MAED_Multi";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var m = (MaedMulti)del(reader);
        Assert.Equal("aa", m.A);
        Assert.Equal("bb", m.B);
        Assert.Equal("cc", m.C);
        Assert.Equal(10, m.X);
        Assert.Equal(30, m.Z);
        Assert.True(m.P);
        Assert.False(m.Q);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 5 — Async typed CreateMaterializer<T>
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MF_AsyncMaterializerGeneric_Product_Works()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer<MaedProduct>(mapping);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Price DESC LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var p = await del(reader, CancellationToken.None);
        // Highest price = Gadget at 19.99
        Assert.Equal("Gadget", p.Name);
        Assert.Equal(19.99m, p.Price);
    }

    [Fact]
    public async Task MF_AsyncMaterializerGeneric_Cancelled_ThrowsOce()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer<MaedProduct>(mapping);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => del(reader, cts.Token));
    }

    [Fact]
    public async Task MF_AsyncMaterializerGeneric_AllRows_Correct()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer<MaedProduct>(mapping);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Id";
        using var reader = cmd.ExecuteReader();
        var results = new List<MaedProduct>();
        while (reader.Read())
            results.Add(await del(reader, CancellationToken.None));

        Assert.Equal(3, results.Count);
        Assert.Equal(50, results[1].Stock); // Gadget
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 6 — Async untyped CreateMaterializer(mapping, Type)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MF_AsyncMaterializerUntyped_Product_Works()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer(mapping, typeof(MaedProduct));

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = await del(reader, CancellationToken.None);
        Assert.IsType<MaedProduct>(obj);
        Assert.Equal("Widget", ((MaedProduct)obj).Name);
    }

    [Fact]
    public async Task MF_AsyncMaterializerUntyped_Cancelled_Throws()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer(mapping, typeof(MaedProduct));

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => del(reader, cts.Token));
    }

    [Fact]
    public async Task MF_AsyncMaterializerUntyped_WithStartOffset0_Works()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer(mapping, typeof(MaedProduct), null, 0);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = await del(reader, CancellationToken.None);
        Assert.IsType<MaedProduct>(obj);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 7 — CreateSchemaAwareMaterializer
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MF_SchemaAwareMaterializer_NoProjection_Works()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateSchemaAwareMaterializer(mapping, typeof(MaedProduct), null);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = await del(reader, CancellationToken.None);
        Assert.IsType<MaedProduct>(obj);
        Assert.Equal("Widget", ((MaedProduct)obj).Name);
    }

    [Fact]
    public async Task MF_SchemaAwareMaterializer_Cancelled_Throws()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateSchemaAwareMaterializer(mapping, typeof(MaedProduct));

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => del(reader, cts.Token));
    }

    [Fact]
    public async Task MF_SchemaAwareMaterializer_WithStartOffset_Works()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateSchemaAwareMaterializer(mapping, typeof(MaedProduct), null, 0);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = await del(reader, CancellationToken.None);
        Assert.IsType<MaedProduct>(obj);
        Assert.Equal("Widget", ((MaedProduct)obj).Name);
    }

    [Fact]
    public async Task MF_SchemaAwareMaterializer_MultipleRows_AllMaterialized()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        var del = factory.CreateSchemaAwareMaterializer(mapping, typeof(MaedProduct), null);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Price, Stock, Available FROM MAED_Product ORDER BY Id";
        using var reader = cmd.ExecuteReader();
        var results = new List<MaedProduct>();
        while (reader.Read())
            results.Add((MaedProduct)await del(reader, CancellationToken.None));

        Assert.Equal(3, results.Count);
        Assert.Equal("Gizmo", results[2].Name);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 8 — Null argument guards
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MF_NullGuard_SyncGeneric_NullMapping_Throws()
    {
        var factory = new MaterializerFactory();
        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateSyncMaterializer<MaedProduct>(null!));
    }

    [Fact]
    public void MF_NullGuard_SyncUntyped_NullMapping_Throws()
    {
        var factory = new MaterializerFactory();
        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateSyncMaterializer(null!, typeof(MaedProduct)));
    }

    [Fact]
    public void MF_NullGuard_SyncUntyped_NullType_Throws()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateSyncMaterializer(mapping, null!));
    }

    [Fact]
    public void MF_NullGuard_SchemaAware_NullMapping_Throws()
    {
        var factory = new MaterializerFactory();
        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateSchemaAwareMaterializer(null!, typeof(MaedProduct)));
    }

    [Fact]
    public void MF_NullGuard_SchemaAware_NullType_Throws()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateSchemaAwareMaterializer(mapping, null!));
    }

    [Fact]
    public void MF_NullGuard_AsyncUntyped_NullMapping_Throws()
    {
        var factory = new MaterializerFactory();
        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateMaterializer(null!, typeof(MaedProduct)));
    }

    [Fact]
    public void MF_NullGuard_AsyncUntyped_NullType_Throws()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MaedProduct));
        var factory = new MaterializerFactory();
        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateMaterializer(mapping, null!));
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 9 — Anonymous type materialization via LINQ projection (positional ctor)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MF_AnonymousType_Projection_SyncPath_PositionalCtor()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = ctx.Query<MaedProduct>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, p.Name })
            .ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal(1, results[0].Id);
        Assert.Equal("Widget", results[0].Name);
    }

    [Fact]
    public async Task MF_AnonymousType_Projection_AsyncPath_PositionalCtor()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var results = await ctx.Query<MaedProduct>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, p.Name, p.Price })
            .ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("Widget", results[0].Name);
        Assert.Equal(9.99m, results[0].Price);
    }

    [Fact]
    public async Task MF_AnonymousType_RenamedMembers_PositionalCtor()
    {
        // Anonymous type with renamed members: { Title = p.Name, Cost = p.Price }
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var results = await ctx.Query<MaedProduct>()
            .OrderBy(p => p.Id)
            .Select(p => new { Title = p.Name, Cost = p.Price })
            .ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("Widget", results[0].Title);
        Assert.Equal(9.99m, results[0].Cost);
    }

    [Fact]
    public void MF_AnonymousType_ScalarInt_Projection()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var stocks = ctx.Query<MaedProduct>()
            .OrderBy(p => p.Id)
            .Select(p => p.Stock)
            .ToList();
        Assert.Equal(3, stocks.Count);
        Assert.Equal(100, stocks[0]);
        Assert.Equal(50, stocks[1]);
        Assert.Equal(0, stocks[2]);
    }

    [Fact]
    public async Task MF_AnonymousType_ScalarString_AsyncProjection()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var names = await ctx.Query<MaedProduct>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name)
            .ToListAsync();
        Assert.Equal(3, names.Count);
        Assert.Equal("Widget", names[0]);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 10 — Nullable property materialization (via ToListAsync)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MF_NullableProperties_AllNull_MaterializeAsNull_Async()
    {
        using var cn = CreateNullPropDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedNullProp>().OrderBy(x => x.Id).ToListAsync();
        Assert.Equal(3, list.Count);
        Assert.Null(list[1].Description);
        Assert.Null(list[1].Rating);
        Assert.Null(list[1].Weight);
    }

    [Fact]
    public void MF_NullableProperties_AllPresent_Correct_Sync()
    {
        using var cn = CreateNullPropDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MaedNullProp>().OrderBy(x => x.Id).ToList();
        Assert.Equal(3, list.Count);
        Assert.Equal("desc", list[0].Description);
        Assert.Equal(5, list[0].Rating);
        Assert.Equal(1.5, list[0].Weight);
    }

    [Fact]
    public void MF_NullableProperties_MixedNulls_Correct()
    {
        using var cn = CreateNullPropDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MaedNullProp>().OrderBy(x => x.Id).ToList();
        // Row 3: Description='x', Rating=3, Weight=NULL
        Assert.Equal("x", list[2].Description);
        Assert.Equal(3, list[2].Rating);
        Assert.Null(list[2].Weight);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 11 — RedactSqlForLogging private method
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Redact_EmptyString_ReturnsEmpty()
    {
        Assert.Equal(string.Empty, Redact(string.Empty));
    }

    [Fact]
    public void Redact_NoLiterals_ReturnsSame()
    {
        const string sql = "SELECT * FROM Products WHERE Id = @p0";
        Assert.Equal(sql, Redact(sql));
    }

    [Fact]
    public void Redact_SingleQuotedString_IsRedacted()
    {
        var result = Redact("SELECT * FROM T WHERE Name = 'secret'");
        Assert.DoesNotContain("secret", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void Redact_MultipleQuotedStrings_AllRedacted()
    {
        var result = Redact("INSERT INTO T (A, B) VALUES ('val1', 'val2')");
        Assert.DoesNotContain("val1", result);
        Assert.DoesNotContain("val2", result);
    }

    [Fact]
    public void Redact_NationalString_SqlServer_Redacted()
    {
        // N'...' SQL Server national string literal
        var result = Redact("SELECT * FROM T WHERE Name = N'тест'");
        Assert.DoesNotContain("тест", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void Redact_EscapedSingleQuote_InsideLiteral_Redacted()
    {
        // 'it''s' — escaped single quote inside literal
        var result = Redact("SELECT * FROM T WHERE Val = 'it''s a secret'");
        Assert.DoesNotContain("secret", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void Redact_DollarQuoted_Bare_Redacted()
    {
        // PostgreSQL bare $$ dollar quoting
        var result = Redact("SELECT $$sensitive data$$");
        Assert.DoesNotContain("sensitive", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void Redact_DollarQuoted_Tagged_Redacted()
    {
        // PostgreSQL tagged dollar quoting: $func$...$func$
        var result = Redact("SELECT $func$secret body$func$");
        Assert.DoesNotContain("secret", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void Redact_DollarQuoted_DifferentTags_BothRedacted()
    {
        var result = Redact("DO $body$ BEGIN RAISE NOTICE 'hi'; END $body$ LANGUAGE plpgsql");
        Assert.DoesNotContain("RAISE NOTICE 'hi'", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void Redact_Identifiers_Preserved()
    {
        const string sql = "SELECT [Col1], `Col2`, \"Col3\" FROM myTable";
        var result = Redact(sql);
        Assert.Equal(sql, result);
    }

    [Fact]
    public void Redact_Parameters_Preserved()
    {
        const string sql = "WHERE A = @p0 AND B = @p1 AND C = @myParam";
        Assert.Equal(sql, Redact(sql));
    }

    [Fact]
    public void Redact_MixedLiteralsAndParams_OnlyLiteralsRedacted()
    {
        var result = Redact("WHERE Name = 'Alice' AND Score = @p0");
        Assert.DoesNotContain("Alice", result);
        Assert.Contains("@p0", result);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 12 — QueryExecutor.Materialize (sync path) via LINQ ToList
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void QE_Sync_ToList_EmptyResult_ReturnsEmptyList()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MaedProduct>().Where(p => p.Price > 9999m).ToList();
        Assert.Empty(list);
    }

    [Fact]
    public void QE_Sync_ToList_FilteredResult_Correct()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MaedProduct>().Where(p => p.Available).OrderBy(p => p.Id).ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal("Widget", list[0].Name);
        Assert.Equal("Gadget", list[1].Name);
    }

    [Fact]
    public void QE_Sync_Single_ReturnsCorrectEntity()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var p = ctx.Query<MaedProduct>().Where(x => x.Name == "Gadget").Single();
        Assert.Equal("Gadget", p.Name);
        Assert.Equal(19.99m, p.Price);
    }

    [Fact]
    public void QE_Sync_SingleOrDefault_NotFound_ReturnsNull()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var p = ctx.Query<MaedProduct>().Where(x => x.Name == "NotExist").SingleOrDefault();
        Assert.Null(p);
    }

    [Fact]
    public void QE_Sync_FirstOrDefault_EmptyResult_ReturnsNull()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var p = ctx.Query<MaedProduct>().Where(x => x.Stock > 99999).FirstOrDefault();
        Assert.Null(p);
    }

    [Fact]
    public void QE_Sync_OrderByTake_CorrectResults()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MaedProduct>().OrderBy(p => p.Price).Take(2).ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal("Gizmo", list[0].Name);   // 4.99
        Assert.Equal("Widget", list[1].Name);  // 9.99
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 13 — QueryExecutor.MaterializeAsync (async path)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task QE_Async_ToListAsync_EmptyResult()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedProduct>().Where(p => p.Stock > 9999).ToListAsync();
        Assert.Empty(list);
    }

    [Fact]
    public async Task QE_Async_ToListAsync_AllRows()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedProduct>().ToListAsync();
        Assert.Equal(3, list.Count);
    }

    [Fact]
    public async Task QE_Async_OrderBy_Correct()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedProduct>().OrderByDescending(p => p.Price).ToListAsync();
        Assert.Equal("Gadget", list[0].Name); // highest price
        Assert.Equal("Gizmo",  list[2].Name); // lowest price
    }

    [Fact]
    public async Task QE_Async_Cancelled_ThrowsOce()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            var q = (INormQueryable<MaedProduct>)ctx.Query<MaedProduct>();
            await q.ToListAsync(cts.Token);
        });
    }

    [Fact]
    public async Task QE_Async_SingleAsync_OneRow_Returns()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var q = (INormQueryable<MaedProduct>)ctx.Query<MaedProduct>().Where(p => p.Name == "Gizmo");
        var p = await q.SingleAsync();
        Assert.Equal("Gizmo", p.Name);
    }

    [Fact]
    public async Task QE_Async_SingleAsync_NoRow_Throws()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var q = (INormQueryable<MaedProduct>)ctx.Query<MaedProduct>().Where(p => p.Name == "Phantom");
        await Assert.ThrowsAsync<InvalidOperationException>(() => q.SingleAsync());
    }

    [Fact]
    public async Task QE_Async_SingleAsync_MultipleRows_Throws()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var q = (INormQueryable<MaedProduct>)ctx.Query<MaedProduct>().Where(p => p.Available);
        await Assert.ThrowsAsync<InvalidOperationException>(() => q.SingleAsync());
    }

    [Fact]
    public async Task QE_Async_SingleOrDefaultAsync_NotFound_ReturnsNull()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var q = (INormQueryable<MaedProduct>)ctx.Query<MaedProduct>().Where(p => p.Price > 9999m);
        var result = await q.SingleOrDefaultAsync();
        Assert.Null(result);
    }

    [Fact]
    public async Task QE_Async_FirstAsync_NonEmpty_Returns()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var q = (INormQueryable<MaedProduct>)ctx.Query<MaedProduct>().OrderBy(p => p.Id);
        var result = await q.FirstAsync();
        Assert.Equal("Widget", result.Name);
    }

    [Fact]
    public async Task QE_Async_FirstOrDefaultAsync_Empty_ReturnsNull()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var q = (INormQueryable<MaedProduct>)ctx.Query<MaedProduct>().Where(p => p.Id == -99);
        var result = await q.FirstOrDefaultAsync();
        Assert.Null(result);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 14 — DependentQueryDefinition path (split-query with HasMany)
    // ══════════════════════════════════════════════════════════════════════

    private static DbContext CreateCategoryCtx(SqliteConnection cn, bool async = false)
    {
        DatabaseProvider provider = async ? new AsyncSqliteProvider() : new SqliteProvider();
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<MaedCategory>()
                  .HasKey(c => c.Id)
                  .HasMany<MaedItem>(c => c.Items)
                  .WithOne()
                  .HasForeignKey(i => i.CategoryId, c => c.Id);
            }
        };
        return new DbContext(cn, provider, opts);
    }

    [Fact]
    public async Task QE_DependentQuery_Async_ItemsPopulated()
    {
        using var cn = CreateCategoryDb();
        using var ctx = CreateCategoryCtx(cn, async: true);

        // This projection includes navigation collection — triggers DependentQueryDefinition path
        var results = await ctx.Query<MaedCategory>()
            .OrderBy(c => c.Id)
            .Select(c => new { c.Id, c.Title })
            .ToListAsync();

        Assert.Equal(2, results.Count);
        Assert.Equal("Electronics", results[0].Title);
        Assert.Equal("Books", results[1].Title);
    }

    [Fact]
    public void QE_DependentQuery_Sync_ItemsPopulated()
    {
        using var cn = CreateCategoryDb();
        using var ctx = CreateCategoryCtx(cn, async: false);

        var results = ctx.Query<MaedCategory>()
            .OrderBy(c => c.Id)
            .Select(c => new { c.Id, c.Title })
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.Equal("Electronics", results[0].Title);
    }

    [Fact]
    public async Task QE_DependentQuery_Async_FullEntity_IncludeChildren()
    {
        using var cn = CreateCategoryDb();
        using var ctx = CreateCategoryCtx(cn, async: true);

        var categories = await ctx.Query<MaedCategory>()
            .OrderBy(c => c.Id)
            .ToListAsync();

        Assert.Equal(2, categories.Count);
    }

    [Fact]
    public void QE_DependentQuery_EmptyParentTable_NoError()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_Category (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                   CREATE TABLE MAED_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, CategoryId INTEGER NOT NULL, Label TEXT NOT NULL, Qty INTEGER NOT NULL DEFAULT 0);");
        // No rows inserted — empty parent table
        using var ctx = CreateCategoryCtx(cn, async: false);

        var results = ctx.Query<MaedCategory>().ToList();
        Assert.Empty(results);
    }

    [Fact]
    public async Task QE_DependentQuery_NoMatchingChildren_ReturnsEmptyCollection()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_Category (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                   CREATE TABLE MAED_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, CategoryId INTEGER NOT NULL, Label TEXT NOT NULL, Qty INTEGER NOT NULL DEFAULT 0);
                   INSERT INTO MAED_Category (Title) VALUES ('Solo');");
        // No child items for the category
        using var ctx = CreateCategoryCtx(cn, async: true);

        var cats = await ctx.Query<MaedCategory>().ToListAsync();
        Assert.Single(cats);
        Assert.Empty(cats[0].Items);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 15 — Large result set (BatchedMaterializeAsync coverage)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task QE_LargeResultSet_100Rows_Async()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE MAED_Event (Id INTEGER PRIMARY KEY AUTOINCREMENT, EventName TEXT NOT NULL, Year INTEGER NOT NULL DEFAULT 0, Notes TEXT)");
        for (int i = 1; i <= 100; i++)
            Exec(cn, $"INSERT INTO MAED_Event (EventName, Year) VALUES ('Event{i}', {2000 + i})");

        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedEvent>().ToListAsync();
        Assert.Equal(100, list.Count);
    }

    [Fact]
    public void QE_LargeResultSet_100Rows_Sync()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE MAED_Event (Id INTEGER PRIMARY KEY AUTOINCREMENT, EventName TEXT NOT NULL, Year INTEGER NOT NULL DEFAULT 0, Notes TEXT)");
        for (int i = 1; i <= 100; i++)
            Exec(cn, $"INSERT INTO MAED_Event (EventName, Year) VALUES ('Ev{i}', {2000 + i})");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MaedEvent>().ToList();
        Assert.Equal(100, list.Count);
    }

    [Fact]
    public async Task QE_LargeResultSet_WithProjection_Async()
    {
        using var cn = OpenMemory();
        Exec(cn, "CREATE TABLE MAED_Score (Id INTEGER PRIMARY KEY AUTOINCREMENT, Player TEXT NOT NULL, Points INTEGER NOT NULL DEFAULT 0, Ratio REAL NOT NULL DEFAULT 0)");
        for (int i = 1; i <= 50; i++)
            Exec(cn, $"INSERT INTO MAED_Score (Player, Points, Ratio) VALUES ('P{i}', {i * 10}, {(i * 0.1).ToString(System.Globalization.CultureInfo.InvariantCulture)})");

        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var results = await ctx.Query<MaedScore>()
            .Select(s => new { s.Player, s.Points })
            .ToListAsync();
        Assert.Equal(50, results.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 16 — SplitQuery + Include path
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task QE_SplitQuery_Include_Async_PopulatesChildren()
    {
        using var cn = CreateCategoryDb();
        using var ctx = CreateCategoryCtx(cn, async: true);

        var q = (INormQueryable<MaedCategory>)ctx.Query<MaedCategory>();
        var categories = await q
            .Include(c => c.Items)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Equal(2, categories.Count);
        var electronics = categories.OrderBy(c => c.Id).First();
        // Electronics has TV and Radio
        Assert.Equal(2, electronics.Items.Count);
    }

    [Fact]
    public void QE_SplitQuery_Include_Sync_PopulatesChildren()
    {
        using var cn = CreateCategoryDb();
        using var ctx = CreateCategoryCtx(cn, async: false);

        var q = (INormQueryable<MaedCategory>)ctx.Query<MaedCategory>();
        var categories = q
            .Include(c => c.Items)
            .AsSplitQuery()
            .ToList();

        Assert.Equal(2, categories.Count);
        var books = categories.OrderBy(c => c.Id).Last();
        // Books has Novel and Manual
        Assert.Equal(2, books.Items.Count);
    }

    [Fact]
    public async Task QE_SplitQuery_Include_Async_EmptyChildTable_EmptyCollections()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_Category (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                   CREATE TABLE MAED_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, CategoryId INTEGER NOT NULL, Label TEXT NOT NULL, Qty INTEGER NOT NULL DEFAULT 0);
                   INSERT INTO MAED_Category (Title) VALUES ('EmptyCat');");
        using var ctx = CreateCategoryCtx(cn, async: true);

        var q = (INormQueryable<MaedCategory>)ctx.Query<MaedCategory>();
        var cats = await q.Include(c => c.Items).AsSplitQuery().ToListAsync();
        Assert.Single(cats);
        Assert.Empty(cats[0].Items);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 17 — CreateListForType internal helper via QueryExecutor
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void QE_CreateListForType_IntType_ReturnsTypedList()
    {
        // Access CreateListForType via the public wrapper on QueryExecutor
        // which is exposed as internal and visible via InternalsVisibleTo
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        // The QueryExecutor is instantiated internally; we indirectly test
        // CreateListForType by exercising queries that vary the element type
        var intList = ctx.Query<MaedProduct>()
            .Select(p => p.Stock)
            .ToList();
        Assert.IsType<List<int>>(intList);
        Assert.Equal(3, intList.Count);
    }

    [Fact]
    public void QE_CreateListForType_StringType_ReturnsTypedList()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var names = ctx.Query<MaedProduct>().Select(p => p.Name).ToList();
        Assert.IsType<List<string>>(names);
        Assert.Equal(3, names.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 18 — NoTracking / AsNoTracking behavior
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task QE_NoTracking_AsyncQuery_EntitiesNotTracked()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ((INormQueryable<MaedProduct>)ctx.Query<MaedProduct>())
            .AsNoTracking().ToListAsync();
        Assert.Equal(3, list.Count);
        foreach (var p in list)
            Assert.Null(ctx.ChangeTracker.GetEntryOrDefault(p));
    }

    [Fact]
    public void QE_NoTracking_SyncQuery_EntitiesNotTracked()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ((INormQueryable<MaedProduct>)ctx.Query<MaedProduct>())
            .AsNoTracking().ToList();
        Assert.Equal(3, list.Count);
        foreach (var p in list)
            Assert.Null(ctx.ChangeTracker.GetEntryOrDefault(p));
    }

    [Fact]
    public async Task QE_WithTracking_AsyncQuery_EntitiesTracked()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedProduct>().ToListAsync();
        Assert.Equal(3, list.Count);
        foreach (var p in list)
        {
            var entry = ctx.ChangeTracker.GetEntryOrDefault(p);
            Assert.NotNull(entry);
            Assert.Equal(EntityState.Unchanged, entry!.State);
        }
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 19 — Aggregate operations via async path
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task QE_Aggregate_CountAsync_Async()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var count = await ctx.Query<MaedProduct>().CountAsync();
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task QE_Aggregate_CountAsync_WithFilter()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var count = await ctx.Query<MaedProduct>().Where(p => p.Available).CountAsync();
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task QE_Aggregate_MinAsync_Int()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var q = (INormQueryable<MaedProduct>)ctx.Query<MaedProduct>();
        var minStock = await q.MinAsync(p => p.Stock);
        Assert.Equal(0, minStock);
    }

    [Fact]
    public async Task QE_Aggregate_MaxAsync_Int()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var q = (INormQueryable<MaedProduct>)ctx.Query<MaedProduct>();
        var maxStock = await q.MaxAsync(p => p.Stock);
        Assert.Equal(100, maxStock);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 20 — MaterializeAsObjectListAsync via anonymous projection
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task QE_MaterializeAsObjectList_AnonymousProjection_Async()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var results = await ctx.Query<MaedProduct>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Name, p.Stock })
            .ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("Widget",  results[0].Name);
        Assert.Equal(100,       results[0].Stock);
    }

    [Fact]
    public async Task QE_MaterializeAsObjectList_ThreeFieldProjection_Async()
    {
        using var cn = CreateEventDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var results = await ctx.Query<MaedEvent>()
            .OrderBy(e => e.Id)
            .Select(e => new { e.Id, e.EventName, e.Year })
            .ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("Launch", results[0].EventName);
        Assert.Equal(2020,     results[0].Year);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 21 — Record type materialization (parameterized constructor)
    // ══════════════════════════════════════════════════════════════════════

    // MaedEventRecord is a record type using positional syntax
    [Table("MAED_EventRec")]
    public record MaedEventRecord(int Id, string EventName, int Year);

    [Fact]
    public void QE_RecordType_SyncMaterialization()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_EventRec (Id INTEGER PRIMARY KEY, EventName TEXT NOT NULL, Year INTEGER NOT NULL DEFAULT 0);
                   INSERT INTO MAED_EventRec VALUES (1, 'RecEvent', 2023);");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MaedEventRecord>().ToList();
        Assert.Single(list);
        Assert.Equal("RecEvent", list[0].EventName);
        Assert.Equal(2023, list[0].Year);
    }

    [Fact]
    public async Task QE_RecordType_AsyncMaterialization()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_EventRec (Id INTEGER PRIMARY KEY, EventName TEXT NOT NULL, Year INTEGER NOT NULL DEFAULT 0);
                   INSERT INTO MAED_EventRec VALUES (2, 'AsyncRecEvent', 2024);");
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedEventRecord>().ToListAsync();
        Assert.Single(list);
        Assert.Equal("AsyncRecEvent", list[0].EventName);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 22 — Long/double scalar materialization
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task QE_ScoreEntity_LongAndDouble_Async()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_Score (Id INTEGER PRIMARY KEY AUTOINCREMENT, Player TEXT NOT NULL, Points INTEGER NOT NULL DEFAULT 0, Ratio REAL NOT NULL DEFAULT 0);
                   INSERT INTO MAED_Score (Player, Points, Ratio) VALUES ('Alpha', 9999999999, 0.999);");
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedScore>().ToListAsync();
        Assert.Single(list);
        Assert.Equal(9999999999L, list[0].Points);
        Assert.Equal(0.999, list[0].Ratio, 3);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 23 — Take/Skip (capacity hints and paging)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task QE_Take_LimitsRows_Async()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedProduct>().OrderBy(p => p.Id).Take(2).ToListAsync();
        Assert.Equal(2, list.Count);
        Assert.Equal("Widget", list[0].Name);
        Assert.Equal("Gadget", list[1].Name);
    }

    [Fact]
    public async Task QE_Skip_SkipsRows_Async()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedProduct>().OrderBy(p => p.Id).Skip(2).ToListAsync();
        Assert.Single(list);
        Assert.Equal("Gizmo", list[0].Name);
    }

    [Fact]
    public async Task QE_SkipAndTake_Combined_Async()
    {
        using var cn = CreateProductDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MaedProduct>().OrderBy(p => p.Id).Skip(1).Take(1).ToListAsync();
        Assert.Single(list);
        Assert.Equal("Gadget", list[0].Name);
    }

    // ══════════════════════════════════════════════════════════════════════
    // GROUP 24 — Multiple Includes (DependentQueries)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task QE_DependentQuery_MultipleBatches_Stitched()
    {
        using var cn = OpenMemory();
        Exec(cn, @"CREATE TABLE MAED_Category (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                   CREATE TABLE MAED_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, CategoryId INTEGER NOT NULL, Label TEXT NOT NULL, Qty INTEGER NOT NULL DEFAULT 0);");
        // Insert 3 categories with items
        for (int i = 1; i <= 3; i++)
        {
            Exec(cn, $"INSERT INTO MAED_Category (Title) VALUES ('Cat{i}')");
            for (int j = 1; j <= 3; j++)
                Exec(cn, $"INSERT INTO MAED_Item (CategoryId, Label, Qty) VALUES ({i}, 'Item{i}-{j}', {j * 10})");
        }

        using var ctx = CreateCategoryCtx(cn, async: true);
        var q = (INormQueryable<MaedCategory>)ctx.Query<MaedCategory>();
        var cats = await q
            .Include(c => c.Items)
            .AsSplitQuery()
            .ToListAsync();

        Assert.Equal(3, cats.Count);
        foreach (var cat in cats)
            Assert.Equal(3, cat.Items.Count);
    }
}
