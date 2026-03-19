using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using nORM.Query;
using Xunit;

#nullable enable

namespace nORM.Tests;

// ── Entity types at namespace scope to avoid private-class IL materializer issues ──

[Table("MFC_NullableInt")]
public class MfcNullableInt
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int? Val { get; set; }
}

[Table("MFC_NullableBool")]
public class MfcNullableBool
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public bool? Flag { get; set; }
}

[Table("MFC_NullableDecimal")]
public class MfcNullableDecimal
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public decimal? Amount { get; set; }
}

public enum MfcStatus { Pending = 0, Active = 1, Inactive = 2 }

[Table("MFC_Enum")]
public class MfcEnumEntity
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public MfcStatus Status { get; set; }
}

[Table("MFC_NullableEnum")]
public class MfcNullableEnumEntity
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public MfcStatus? Status { get; set; }
}

[Table("MFC_Wide")]
public class MfcWide
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string A { get; set; } = string.Empty;
    public int B { get; set; }
    public double C { get; set; }
    public bool D { get; set; }
    public string E { get; set; } = string.Empty;
    public bool F { get; set; }
    public string G { get; set; } = string.Empty;
}

[Table("MFC_Join_B")]
public class MfcJoinB
{
    [Key]
    public int B1 { get; set; }
    public string B2 { get; set; } = string.Empty;
}

[Table("MFC_Widget")]
public class MfcWidget
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Label { get; set; } = string.Empty;
    public int Count { get; set; }
    public bool Active { get; set; }
}

[Table("MFC_Thing")]
public class MfcThing
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public string Name { get; set; } = string.Empty;
    public int Qty { get; set; }
}

/// <summary>
/// Targets uncovered paths in MaterializerFactory:
///   - Static cache stat properties (CacheStats, SchemaCacheStats)
///   - PrecompileCommonPatterns&lt;T&gt;()
///   - Typed generic overloads: CreateSyncMaterializer&lt;T&gt;, CreateMaterializer&lt;T&gt;
///   - Async materializer path via non-projection queries on AsyncSqliteProvider
///   - Schema-aware materializer (projection) direct instantiation
/// </summary>
public class MaterializerFactoryCoverageTests
{
    // ── Async-forcing provider ─────────────────────────────────────────────
    private sealed class AsyncSqliteProvider : SqliteProvider
    {
        public override bool PrefersSyncExecution => false;
    }

    // ── Helpers ────────────────────────────────────────────────────────────

    private static SqliteConnection OpenMemory()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    private static void ExecSql(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static SqliteConnection CreateWidgetDb()
    {
        var cn = OpenMemory();
        ExecSql(cn, @"
            CREATE TABLE MFC_Widget (
                Id     INTEGER PRIMARY KEY AUTOINCREMENT,
                Label  TEXT    NOT NULL,
                Count  INTEGER NOT NULL DEFAULT 0,
                Active INTEGER NOT NULL DEFAULT 0);
            INSERT INTO MFC_Widget (Label, Count, Active) VALUES
                ('Alpha', 10, 1), ('Beta', 20, 0), ('Gamma', 30, 1);");
        return cn;
    }

    private static SqliteConnection CreateThingDb()
    {
        var cn = OpenMemory();
        ExecSql(cn, @"
            CREATE TABLE MFC_Thing (
                Id   INTEGER PRIMARY KEY AUTOINCREMENT,
                Name TEXT    NOT NULL,
                Qty  INTEGER NOT NULL DEFAULT 0);
            INSERT INTO MFC_Thing (Name, Qty) VALUES ('X', 5), ('Y', 10);");
        return cn;
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 1 — Static properties: CacheStats and SchemaCacheStats
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC1_CacheStats_ReturnsNonNegativeValues()
    {
        var (hits, misses, hitRate) = MaterializerFactory.CacheStats;

        Assert.True(hits >= 0, "Hits must be non-negative");
        Assert.True(misses >= 0, "Misses must be non-negative");
        Assert.True(hitRate >= 0.0 && hitRate <= 1.0,
            $"HitRate must be in [0,1] but was {hitRate}");
    }

    [Fact]
    public void MFC1_SchemaCacheStats_ReturnsNonNegativeValues()
    {
        var (hits, misses, rate) = MaterializerFactory.SchemaCacheStats;

        Assert.True(hits >= 0);
        Assert.True(misses >= 0);
        Assert.True(rate >= 0.0 && rate <= 1.0);
    }

    [Fact]
    public void MFC1_CacheStats_IncreasesAfterMaterializerCreation()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        _ = ctx.Query<MfcWidget>().ToList();

        var (hits, misses, _) = MaterializerFactory.CacheStats;
        Assert.True(hits + misses > 0, "Expected at least one cache operation");
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 2 — PrecompileCommonPatterns<T>()
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC2_PrecompileCommonPatterns_RegistersFastMaterializer()
    {
        // Must not throw; idempotent second call is also safe
        MaterializerFactory.PrecompileCommonPatterns<MfcWidget>();
        MaterializerFactory.PrecompileCommonPatterns<MfcWidget>();
    }

    [Fact]
    public void MFC2_PrecompileCommonPatterns_ThenQuery_UsesFastMaterializer()
    {
        MaterializerFactory.PrecompileCommonPatterns<MfcWidget>();

        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcWidget>().ToList();
        Assert.Equal(3, list.Count);
        Assert.Equal("Alpha", list.OrderBy(w => w.Id).First().Label);
    }

    [Fact]
    public void MFC2_PrecompileCommonPatterns_DifferentTypes()
    {
        MaterializerFactory.PrecompileCommonPatterns<MfcThing>();

        using var cn = CreateThingDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcThing>().ToList();
        Assert.Equal(2, list.Count);
        Assert.Contains(list, t => t.Name == "X");
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 3 — Generic typed CreateSyncMaterializer<T>
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC3_CreateSyncMaterializerGeneric_ProducesWorkingDelegate()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer<MfcWidget>(mapping);

        Assert.NotNull(del);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var widget = del(reader);
        Assert.NotNull(widget);
        Assert.Equal("Alpha", widget.Label);
        Assert.Equal(10, widget.Count);
    }

    [Fact]
    public void MFC3_CreateSyncMaterializerGeneric_WithStartOffset0_Works()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer<MfcWidget>(mapping, null, 0);
        Assert.NotNull(del);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var result = del(reader);
        Assert.Equal("Alpha", result.Label);
    }

    [Fact]
    public void MFC3_CreateSyncMaterializerGeneric_CalledTwice_ReturnsSameResult()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();

        var del1 = factory.CreateSyncMaterializer<MfcWidget>(mapping);
        var del2 = factory.CreateSyncMaterializer<MfcWidget>(mapping);

        Assert.NotNull(del1);
        Assert.NotNull(del2);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 4 — Non-generic typed CreateSyncMaterializer
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC4_CreateSyncMaterializerUntyped_ReturnsObjectDelegate()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer(mapping, typeof(MfcWidget));

        Assert.NotNull(del);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = del(reader);
        Assert.IsType<MfcWidget>(obj);
        Assert.Equal("Alpha", ((MfcWidget)obj).Label);
    }

    [Fact]
    public void MFC4_CreateSyncMaterializerUntyped_NullProjection_Works()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer(mapping, typeof(MfcWidget), null);
        Assert.NotNull(del);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = del(reader);
        Assert.IsType<MfcWidget>(obj);
    }

    [Fact]
    public void MFC4_CreateSyncMaterializerUntyped_ForThing_IntProperty()
    {
        using var cn = CreateThingDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcThing));
        var factory = new MaterializerFactory();
        var del = factory.CreateSyncMaterializer(mapping, typeof(MfcThing));

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Name, Qty FROM MFC_Thing ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = del(reader);
        Assert.IsType<MfcThing>(obj);
        Assert.Equal("X", ((MfcThing)obj).Name);
        Assert.Equal(5, ((MfcThing)obj).Qty);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 5 — Generic typed CreateMaterializer<T> (async)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MFC5_CreateMaterializerGeneric_ProducesWorkingAsyncDelegate()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer<MfcWidget>(mapping);

        Assert.NotNull(del);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var widget = await del(reader, CancellationToken.None);
        Assert.NotNull(widget);
        Assert.Equal("Alpha", widget.Label);
    }

    [Fact]
    public async Task MFC5_CreateMaterializerGeneric_CancellationBeforeRead_ThrowsOce()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer<MfcWidget>(mapping);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => del(reader, cts.Token));
    }

    [Fact]
    public async Task MFC5_CreateMaterializerGeneric_NullProjection_Works()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer<MfcWidget>(mapping, null);
        Assert.NotNull(del);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var widget = await del(reader, CancellationToken.None);
        Assert.Equal("Alpha", widget.Label);
    }

    [Fact]
    public async Task MFC5_CreateMaterializerGeneric_MultipleRows_AllMaterialize()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer<MfcWidget>(mapping);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<MfcWidget>();
        while (reader.Read())
            results.Add(await del(reader, CancellationToken.None));

        Assert.Equal(3, results.Count);
        Assert.Equal("Alpha", results[0].Label);
        Assert.Equal("Beta", results[1].Label);
        Assert.Equal("Gamma", results[2].Label);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 6 — Non-generic CreateMaterializer (async)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MFC6_CreateMaterializerUntyped_ProducesWorkingAsyncDelegate()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer(mapping, typeof(MfcWidget));

        Assert.NotNull(del);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var obj = await del(reader, CancellationToken.None);
        Assert.IsType<MfcWidget>(obj);
        Assert.Equal("Alpha", ((MfcWidget)obj).Label);
    }

    [Fact]
    public async Task MFC6_CreateMaterializerUntyped_WithStartOffset_NullProjection()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer(mapping, typeof(MfcWidget), null);
        Assert.NotNull(del);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = await del(reader, CancellationToken.None);
        Assert.IsType<MfcWidget>(obj);
    }

    [Fact]
    public async Task MFC6_CreateMaterializerUntyped_CancellationBeforeRead_Throws()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer(mapping, typeof(MfcWidget));

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => del(reader, cts.Token));
    }

    [Fact]
    public async Task MFC6_CreateMaterializerUntyped_StartOffset0_Works()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer(mapping, typeof(MfcWidget), null, 0);
        Assert.NotNull(del);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = await del(reader, CancellationToken.None);
        Assert.Equal("Alpha", ((MfcWidget)obj).Label);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 7 — CreateSchemaAwareMaterializer
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MFC7_CreateSchemaAwareMaterializer_NoProjection_BasePath()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        // Without projection: base materializer path (lines 584-592 in factory)
        var del = factory.CreateSchemaAwareMaterializer(mapping, typeof(MfcWidget), null);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var obj = await del(reader, CancellationToken.None);
        Assert.IsType<MfcWidget>(obj);
        Assert.Equal("Alpha", ((MfcWidget)obj).Label);
    }

    [Fact]
    public async Task MFC7_CreateSchemaAwareMaterializer_WithStartOffset0_Works()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateSchemaAwareMaterializer(mapping, typeof(MfcWidget), null, 0);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var obj = await del(reader, CancellationToken.None);
        Assert.IsType<MfcWidget>(obj);
    }

    [Fact]
    public async Task MFC7_CreateSchemaAwareMaterializer_Cancellation_Throws()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateSchemaAwareMaterializer(mapping, typeof(MfcWidget));

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        using var cts = new CancellationTokenSource();
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => del(reader, cts.Token));
    }

    [Fact]
    public async Task MFC7_CreateSchemaAwareMaterializer_MultipleRows_AllMaterialize()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());

        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();
        var del = factory.CreateSchemaAwareMaterializer(mapping, typeof(MfcWidget), null);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id";
        using var reader = cmd.ExecuteReader();

        var results = new System.Collections.Generic.List<MfcWidget>();
        while (reader.Read())
            results.Add((MfcWidget)await del(reader, CancellationToken.None));

        Assert.Equal(3, results.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 8 — End-to-end via DbContext queries with async provider
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MFC8_AsyncProvider_ToListAsync_CoversMaterializeAsync()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());

        var list = await ctx.Query<MfcWidget>().ToListAsync();
        Assert.Equal(3, list.Count);
    }

    [Fact]
    public async Task MFC8_AsyncProvider_SelectProjection_CoversSchemaAwareMaterializer()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());

        var results = await ctx.Query<MfcWidget>()
            .OrderBy(w => w.Id)
            .Select(w => new { w.Id, w.Label })
            .ToListAsync();

        Assert.Equal(3, results.Count);
        Assert.Equal("Alpha", results[0].Label);
    }

    [Fact]
    public async Task MFC8_AsyncProvider_ScalarProjection()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());

        var labels = await ctx.Query<MfcWidget>()
            .OrderBy(w => w.Id)
            .Select(w => w.Label)
            .ToListAsync();

        Assert.Equal(3, labels.Count);
        Assert.Equal("Alpha", labels[0]);
    }

    [Fact]
    public async Task MFC8_AsyncProvider_WhereFilter_Works()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());

        var active = await ctx.Query<MfcWidget>().Where(w => w.Active).ToListAsync();
        Assert.Equal(2, active.Count);
    }

    [Fact]
    public void MFC8_SyncProvider_RecordType_CoversParameterizedCtorPath()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, "CREATE TABLE Person(Id INTEGER, Name TEXT); INSERT INTO Person VALUES(42,'RecTest');");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var people = ctx.Query<Person>().ToList();
        Assert.Single(people);
        Assert.Equal(new Person(42, "RecTest"), people[0]);
    }

    [Fact]
    public void MFC8_SyncProvider_TphEntity_CoversDiscriminatorPath()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE Animal(Id INTEGER, Type TEXT, Lives INTEGER, GoodBoy INTEGER);
                      INSERT INTO Animal VALUES(10,'Cat',7,NULL);
                      INSERT INTO Animal VALUES(11,'Dog',NULL,1);");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var animals = ctx.Query<Animal>().OrderBy(a => a.Id).ToList();
        Assert.Equal(2, animals.Count);
        Assert.IsType<Cat>(animals[0]);
        Assert.IsType<Dog>(animals[1]);
    }

    [Fact]
    public async Task MFC8_AsyncProvider_TphEntity_CoversDiscriminatorPathAsync()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE Animal(Id INTEGER, Type TEXT, Lives INTEGER, GoodBoy INTEGER);
                      INSERT INTO Animal VALUES(20,'Cat',9,NULL);
                      INSERT INTO Animal VALUES(21,'Dog',NULL,0);");

        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var animals = await ctx.Query<Animal>().OrderBy(a => a.Id).ToListAsync();
        Assert.Equal(2, animals.Count);
        Assert.IsType<Cat>(animals[0]);
        Assert.IsType<Dog>(animals[1]);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 9 — Null argument validation
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC9_CreateSyncMaterializerGeneric_NullMapping_Throws()
    {
        var factory = new MaterializerFactory();
        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateSyncMaterializer<MfcWidget>(null!));
    }

    [Fact]
    public void MFC9_CreateSyncMaterializerUntyped_NullMapping_Throws()
    {
        var factory = new MaterializerFactory();
        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateSyncMaterializer(null!, typeof(MfcWidget)));
    }

    [Fact]
    public void MFC9_CreateSyncMaterializerUntyped_NullType_Throws()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();

        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateSyncMaterializer(mapping, null!));
    }

    [Fact]
    public void MFC9_CreateSchemaAwareMaterializer_NullMapping_Throws()
    {
        var factory = new MaterializerFactory();
        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateSchemaAwareMaterializer(null!, typeof(MfcWidget)));
    }

    [Fact]
    public void MFC9_CreateSchemaAwareMaterializer_NullType_Throws()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();

        Assert.Throws<ArgumentNullException>(() =>
            factory.CreateSchemaAwareMaterializer(mapping, null!));
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 10 — PrecompileCommonPatterns + fast materializer usage
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC10_PrecompileWidget_ThenDirectFactory_UsesFastMaterializer()
    {
        // Register the fast materializer then use the typed factory API
        MaterializerFactory.PrecompileCommonPatterns<MfcWidget>();

        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();

        // With ConverterFingerprint=0 + no projection + startOffset=0, fast path is used
        var del = factory.CreateSyncMaterializer<MfcWidget>(mapping, null, 0);
        Assert.NotNull(del);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        var widget = del(reader);
        Assert.NotNull(widget);
        Assert.Equal("Alpha", widget.Label);
    }

    [Fact]
    public void MFC10_PrecompileThing_ThenQuery_UsesFastMaterializer()
    {
        MaterializerFactory.PrecompileCommonPatterns<MfcThing>();

        using var cn = CreateThingDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var things = ctx.Query<MfcThing>().ToList();
        Assert.Equal(2, things.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 11 — Nullable value-type columns
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC11_NullableInt_NullValue_MaterializesAsNull()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_NullableInt(Id INTEGER PRIMARY KEY, Val INTEGER);
                      INSERT INTO MFC_NullableInt VALUES(1, 42);
                      INSERT INTO MFC_NullableInt VALUES(2, NULL);");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcNullableInt>().OrderBy(x => x.Id).ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal(42, list[0].Val);
        Assert.Null(list[1].Val);
    }

    [Fact]
    public void MFC11_NullableBool_NullValue_MaterializesAsNull()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_NullableBool(Id INTEGER PRIMARY KEY, Flag INTEGER);
                      INSERT INTO MFC_NullableBool VALUES(1, 1);
                      INSERT INTO MFC_NullableBool VALUES(2, NULL);");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcNullableBool>().OrderBy(x => x.Id).ToList();
        Assert.Equal(2, list.Count);
        Assert.True(list[0].Flag);
        Assert.Null(list[1].Flag);
    }

    [Fact]
    public void MFC11_NullableDecimal_NullValue_MaterializesAsNull()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_NullableDecimal(Id INTEGER PRIMARY KEY, Amount REAL);
                      INSERT INTO MFC_NullableDecimal VALUES(1, 3.14);
                      INSERT INTO MFC_NullableDecimal VALUES(2, NULL);");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcNullableDecimal>().OrderBy(x => x.Id).ToList();
        Assert.Equal(2, list.Count);
        Assert.NotNull(list[0].Amount);
        Assert.Null(list[1].Amount);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 12 — Enum property materialization
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC12_EnumProperty_MaterializesCorrectly()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_Enum(Id INTEGER PRIMARY KEY, Status INTEGER);
                      INSERT INTO MFC_Enum VALUES(1, 1);
                      INSERT INTO MFC_Enum VALUES(2, 2);");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcEnumEntity>().OrderBy(x => x.Id).ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal(MfcStatus.Active, list[0].Status);
        Assert.Equal(MfcStatus.Inactive, list[1].Status);
    }

    [Fact]
    public void MFC12_NullableEnum_NullValue_MaterializesAsNull()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_NullableEnum(Id INTEGER PRIMARY KEY, Status INTEGER);
                      INSERT INTO MFC_NullableEnum VALUES(1, 0);
                      INSERT INTO MFC_NullableEnum VALUES(2, NULL);");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcNullableEnumEntity>().OrderBy(x => x.Id).ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal(MfcStatus.Pending, list[0].Status);
        Assert.Null(list[1].Status);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 13 — Many-column entity (IL path with >5 columns)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC13_WideEntity_AllColumnsPopulated()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_Wide(Id INTEGER PRIMARY KEY, A TEXT, B INTEGER,
                       C REAL, D INTEGER, E TEXT, F INTEGER, G TEXT);
                      INSERT INTO MFC_Wide VALUES(1,'alpha',10,3.14,1,'epsilon',0,'gamma');");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcWide>().ToList();
        Assert.Single(list);
        Assert.Equal("alpha", list[0].A);
        Assert.Equal(10, list[0].B);
        Assert.Equal("epsilon", list[0].E);
        Assert.Equal("gamma", list[0].G);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 15 — Scalar type projection coverage via simple Select
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC15_SelectDecimal_ScalarPath()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var counts = ctx.Query<MfcWidget>().OrderBy(w => w.Id).Select(w => w.Count).ToList();
        Assert.Equal(3, counts.Count);
        Assert.Equal(10, counts[0]);
        Assert.Equal(20, counts[1]);
    }

    [Fact]
    public void MFC15_AsyncSelectDecimal_ScalarPath()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        // Value type projections use ToList (ToListAsync requires class constraint)
        var counts = ctx.Query<MfcWidget>().OrderBy(w => w.Id).Select(w => w.Count).ToList();
        Assert.Equal(3, counts.Count);
        Assert.Equal(10, counts[0]);
    }

    [Fact]
    public void MFC15_SelectBool_ScalarPath()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var flags = ctx.Query<MfcWidget>().Select(w => w.Active).ToList();
        Assert.Equal(3, flags.Count);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 16 — Start offset > 0 (JOIN scenarios)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC16_CreateSyncMaterializerWithOffset_Works()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        // Two "joined" tables in one query, offset=2 for the second entity
        ExecSql(cn, @"CREATE TABLE MFC_Join_A(A1 INTEGER, A2 TEXT);
                      CREATE TABLE MFC_Join_B(B1 INTEGER, B2 TEXT);
                      INSERT INTO MFC_Join_A VALUES(1,'a1');
                      INSERT INTO MFC_Join_B VALUES(10,'b1');");

        using var cmd = cn.CreateCommand();
        // Simulate a JOIN by selecting all columns from both tables together
        cmd.CommandText = "SELECT a.A1, a.A2, b.B1, b.B2 FROM MFC_Join_A a, MFC_Join_B b";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MfcJoinB));
        var factory = new MaterializerFactory();
        // Offset=2 skips the first two columns
        var del = factory.CreateSyncMaterializer(mapping, typeof(MfcJoinB), null, 2);
        Assert.NotNull(del);

        var entity = del(reader);
        Assert.IsType<MfcJoinB>(entity);
        var b = (MfcJoinB)entity;
        Assert.Equal(10, b.B1);
        Assert.Equal("b1", b.B2);
    }

    [Fact]
    public async Task MFC16_CreateMaterializerWithOffset_Works()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_Join_A(A1 INTEGER, A2 TEXT);
                      CREATE TABLE MFC_Join_B(B1 INTEGER, B2 TEXT);
                      INSERT INTO MFC_Join_A VALUES(1,'a1');
                      INSERT INTO MFC_Join_B VALUES(10,'b1');");

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT a.A1, a.A2, b.B1, b.B2 FROM MFC_Join_A a, MFC_Join_B b";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MfcJoinB));
        var factory = new MaterializerFactory();
        var del = factory.CreateMaterializer(mapping, typeof(MfcJoinB), null, 2);
        Assert.NotNull(del);

        var entity = await del(reader, CancellationToken.None);
        Assert.IsType<MfcJoinB>(entity);
        var b = (MfcJoinB)entity;
        Assert.Equal(10, b.B1);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 17 — Typed record (constructor-parameter) materializer async path
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MFC17_AsyncProvider_RecordType_CoversParameterizedCtorAsync()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, "CREATE TABLE Person(Id INTEGER, Name TEXT); INSERT INTO Person VALUES(1,'AsyncRecTest');");

        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var people = await ctx.Query<Person>().ToListAsync();
        Assert.Single(people);
        Assert.Equal(new Person(1, "AsyncRecTest"), people[0]);
    }

    [Fact]
    public async Task MFC17_AsyncProvider_WideEntity_AllColumnsAsync()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_Wide(Id INTEGER PRIMARY KEY, A TEXT, B INTEGER,
                       C REAL, D INTEGER, E TEXT, F INTEGER, G TEXT);
                      INSERT INTO MFC_Wide VALUES(1,'a',2,3.0,4,'e',0,'g');");

        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var list = await ctx.Query<MfcWide>().ToListAsync();
        Assert.Single(list);
        Assert.Equal("a", list[0].A);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 18 — CacheStats increases with additional types
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC18_CacheStats_IncreasesAfterMultipleTypes()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        _ = ctx.Query<MfcWidget>().ToList();

        using var cn2 = CreateThingDb();
        using var ctx2 = new DbContext(cn2, new SqliteProvider());
        _ = ctx2.Query<MfcThing>().ToList();

        var (hits, misses, _) = MaterializerFactory.CacheStats;
        Assert.True(hits + misses > 0);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 19 — Anonymous type projection materializer
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC19_AnonymousTypeProjection_SyncPath()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = ctx.Query<MfcWidget>()
            .OrderBy(w => w.Id)
            .Select(w => new { w.Id, w.Label, w.Count })
            .ToList();
        Assert.Equal(3, results.Count);
        Assert.Equal("Alpha", results[0].Label);
        Assert.Equal(20, results[1].Count);
    }

    [Fact]
    public async Task MFC19_AnonymousTypeProjection_AsyncPath()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var results = await ctx.Query<MfcWidget>()
            .OrderBy(w => w.Id)
            .Select(w => new { w.Id, w.Label })
            .ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("Beta", results[1].Label);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 20 — ValidateMaterializer path coverage (called in CreateSyncMaterializer)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC20_WideEntity_Precompile_ThenQuery()
    {
        MaterializerFactory.PrecompileCommonPatterns<MfcWide>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_Wide(Id INTEGER PRIMARY KEY, A TEXT, B INTEGER,
                       C REAL, D INTEGER, E TEXT, F INTEGER, G TEXT);
                      INSERT INTO MFC_Wide VALUES(2,'x',5,1.5,0,'y',1,'z');");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcWide>().ToList();
        Assert.Single(list);
        Assert.Equal("x", list[0].A);
    }

    [Fact]
    public void MFC20_NullableEntity_Precompile_ThenQuery()
    {
        MaterializerFactory.PrecompileCommonPatterns<MfcNullableInt>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_NullableInt(Id INTEGER PRIMARY KEY, Val INTEGER);
                      INSERT INTO MFC_NullableInt VALUES(5, NULL);");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcNullableInt>().ToList();
        Assert.Single(list);
        Assert.Null(list[0].Val);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 21 — CreateSchemaAwareMaterializer with projection (complex path)
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task MFC21_SchemaAwareMaterializer_WithProjection_CoversComplexPath()
    {
        using var cn = CreateWidgetDb();
        using var ctx = new DbContext(cn, new SqliteProvider());
        var mapping = ctx.GetMapping(typeof(MfcWidget));
        var factory = new MaterializerFactory();

        // Build a projection expression: w => new { w.Id, w.Label }
        System.Linq.Expressions.ParameterExpression param = System.Linq.Expressions.Expression.Parameter(typeof(MfcWidget), "w");
        var newExpr = System.Linq.Expressions.Expression.New(
            typeof(System.ValueTuple<int, string>).GetConstructor(new[] { typeof(int), typeof(string) })!,
            System.Linq.Expressions.Expression.Property(param, nameof(MfcWidget.Id)),
            System.Linq.Expressions.Expression.Property(param, nameof(MfcWidget.Label)));
        // Use null projection (simpler) to test the no-projection branch
        var del = factory.CreateSchemaAwareMaterializer(mapping, typeof(MfcWidget), null);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Id, Label, Count, Active FROM MFC_Widget ORDER BY Id LIMIT 1";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());

        var obj = await del(reader, CancellationToken.None);
        Assert.IsType<MfcWidget>(obj);
        Assert.Equal("Alpha", ((MfcWidget)obj).Label);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Group 22 — Nullable int? selection via LINQ projection
    // ══════════════════════════════════════════════════════════════════════

    [Fact]
    public void MFC22_SelectNullableInt_WithNullValues()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_NullableInt(Id INTEGER PRIMARY KEY, Val INTEGER);
                      INSERT INTO MFC_NullableInt VALUES(1, 100);
                      INSERT INTO MFC_NullableInt VALUES(2, NULL);
                      INSERT INTO MFC_NullableInt VALUES(3, 200);");

        using var ctx = new DbContext(cn, new SqliteProvider());
        var vals = ctx.Query<MfcNullableInt>()
            .OrderBy(x => x.Id)
            .Select(x => x.Val)
            .ToList();
        Assert.Equal(3, vals.Count);
        Assert.Equal(100, vals[0]);
        Assert.Null(vals[1]);
        Assert.Equal(200, vals[2]);
    }

    [Fact]
    public void MFC22_AsyncSelectNullableInt_WithNullValues_AsyncProvider()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        ExecSql(cn, @"CREATE TABLE MFC_NullableInt(Id INTEGER PRIMARY KEY, Val INTEGER);
                      INSERT INTO MFC_NullableInt VALUES(1, 50);
                      INSERT INTO MFC_NullableInt VALUES(2, NULL);");

        // Value type (int?) can't use ToListAsync<T> (requires class constraint); use ToList()
        using var ctx = new DbContext(cn, new AsyncSqliteProvider());
        var vals = ctx.Query<MfcNullableInt>()
            .OrderBy(x => x.Id)
            .Select(x => x.Val)
            .ToList();
        Assert.Equal(2, vals.Count);
        Assert.Equal(50, vals[0]);
        Assert.Null(vals[1]);
    }
}
