using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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
using nORM.SourceGeneration;
using Xunit;

#nullable enable

namespace nORM.Tests.SgFluentRename;

// ── Entities ──────────────────────────────────────────────────────────────────
// Each lives in this namespace so source-generated materializers are distinct.

/// <summary>
/// Entity with [GenerateMaterializer] and NO [Column] attributes.
/// When used with fluent HasColumnName, HasFluentColumnRenames must detect divergence
/// and fall back to the runtime materializer.
/// </summary>
[GenerateMaterializer]
[Table("fluent_items")]
internal class FluentRenameItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    // No [Column] — fluent HasColumnName("display_name") will rename this at runtime.
    public string Name { get; set; } = string.Empty;

    public decimal Price { get; set; }
}

/// <summary>
/// Entity with [GenerateMaterializer] and [Column] attributes only.
/// No fluent renames — compiled materializer should always be used.
/// </summary>
[GenerateMaterializer]
[Table("attr_items")]
internal class AttrRenameItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    [Column("display_name")]
    public string Name { get; set; } = string.Empty;

    public decimal Price { get; set; }
}

/// <summary>
/// Entity with [GenerateMaterializer], one [Column] property and one property
/// that will get a fluent-only rename. Mixed case: should fall back to runtime.
/// </summary>
[GenerateMaterializer]
[Table("mixed_items")]
internal class MixedRenameItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    [Column("display_name")]
    public string Name { get; set; } = string.Empty;

    // No [Column] — fluent rename will be applied at runtime.
    public string Category { get; set; } = string.Empty;

    public decimal Price { get; set; }
}

/// <summary>
/// Entity with [GenerateMaterializer] and no column renames at all.
/// Compiled materializer should always be used.
/// </summary>
[GenerateMaterializer]
[Table("plain_items")]
internal class PlainItem
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;

    public decimal Price { get; set; }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

public class SourceGenFluentRenameTests
{
    // ── Helpers ───────────────────────────────────────────────────────────────

    private static SqliteConnection CreateOpenDb(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static void ExecNonQuery(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        cmd.ExecuteNonQuery();
    }

    private static DbContext CreateCtx(SqliteConnection cn, Action<ModelBuilder>? configure = null)
    {
        if (configure != null)
        {
            var opts = new DbContextOptions { OnModelCreating = configure };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return new DbContext(cn, new SqliteProvider());
    }

    // ── X1-1: Fluent rename fallback ─────────────────────────────────────────
    // Entity with [GenerateMaterializer] + fluent HasColumnName("display_name")
    // on a property WITHOUT [Column] → runtime materializer must be used, and
    // query must return correct data.

    [Fact]
    public async Task Fluent_rename_falls_back_to_runtime_materializer()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE fluent_items (Id INTEGER PRIMARY KEY AUTOINCREMENT, display_name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0)");
        ExecNonQuery(cn, "INSERT INTO fluent_items (display_name, Price) VALUES ('Alice', 9.99)");

        await using var ctx = CreateCtx(cn, mb =>
            mb.Entity<FluentRenameItem>()
              .Property(x => x.Name).HasColumnName("display_name"));

        var items = await ctx.Query<FluentRenameItem>().ToListAsync();

        Assert.Single(items);
        Assert.Equal("Alice", items[0].Name);
        Assert.Equal(9.99m, items[0].Price);
    }

    // ── X1-2: Attribute rename still uses compiled ───────────────────────────
    // Entity with [GenerateMaterializer] + [Column("display_name")] → compiled
    // materializer IS used (no fluent rename detected).

    [Fact]
    public async Task Attribute_rename_uses_compiled_materializer()
    {
        // Confirm that a compiled materializer is registered for AttrRenameItem.
        Assert.True(CompiledMaterializerStore.TryGet(typeof(AttrRenameItem), out _),
            "Source generator must register a materializer for AttrRenameItem.");

        using var cn = CreateOpenDb(
            "CREATE TABLE attr_items (Id INTEGER PRIMARY KEY AUTOINCREMENT, display_name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0)");
        ExecNonQuery(cn, "INSERT INTO attr_items (display_name, Price) VALUES ('Bob', 5.50)");

        // No fluent config — purely attribute-driven.
        await using var ctx = CreateCtx(cn);

        var items = await ctx.Query<AttrRenameItem>().ToListAsync();

        Assert.Single(items);
        Assert.Equal("Bob", items[0].Name);
        Assert.Equal(5.50m, items[0].Price);
    }

    [Fact]
    public void Attribute_rename_entity_materializer_is_not_skipped()
    {
        // When there are no fluent renames, the compiled materializer should be
        // returned by CreateMaterializer. We verify indirectly: get the mapping
        // without fluent config, then check that HasFluentColumnRenames returns false
        // by confirming every column's Name matches either [Column] value or PropName.
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateCtx(cn);
        var mapping = ctx.GetMapping(typeof(AttrRenameItem));

        // Name property has [Column("display_name")] → col.Name="display_name", col.PropName="Name"
        // The source generator also sees [Column] so this is NOT a fluent rename.
        var nameCol = mapping.Columns.First(c => c.PropName == "Name");
        Assert.Equal("display_name", nameCol.Name);

        // Verify HasFluentColumnRenames returns false for this mapping by checking
        // that each column either has a [Column] attribute or Name==PropName.
        foreach (var col in mapping.Columns)
        {
            var prop = typeof(AttrRenameItem).GetProperty(col.PropName);
            if (prop == null) continue;
            var colAttr = prop.GetCustomAttribute<ColumnAttribute>();
            if (colAttr == null)
                Assert.Equal(col.PropName, col.Name); // No rename without [Column]
        }
    }

    // ── X1-3: Mixed attribute + fluent ───────────────────────────────────────
    // One property with [Column] and one with fluent-only rename → fallback.

    [Fact]
    public async Task Mixed_attribute_and_fluent_renames_fall_back()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE mixed_items (Id INTEGER PRIMARY KEY AUTOINCREMENT, display_name TEXT NOT NULL DEFAULT '', cat_name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0)");
        ExecNonQuery(cn, "INSERT INTO mixed_items (display_name, cat_name, Price) VALUES ('Charlie', 'Electronics', 12.00)");

        await using var ctx = CreateCtx(cn, mb =>
            mb.Entity<MixedRenameItem>()
              .Property(x => x.Category).HasColumnName("cat_name"));

        var items = await ctx.Query<MixedRenameItem>().ToListAsync();

        Assert.Single(items);
        Assert.Equal("Charlie", items[0].Name);
        Assert.Equal("Electronics", items[0].Category);
        Assert.Equal(12.00m, items[0].Price);
    }

    // ── X1-4: No renames at all ──────────────────────────────────────────────
    // Entity with no column renames → compiled materializer IS used.

    [Fact]
    public async Task No_renames_uses_compiled_materializer()
    {
        Assert.True(CompiledMaterializerStore.TryGet(typeof(PlainItem), out _),
            "Source generator must register a materializer for PlainItem.");

        using var cn = CreateOpenDb(
            "CREATE TABLE plain_items (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0)");
        ExecNonQuery(cn, "INSERT INTO plain_items (Name, Price) VALUES ('Dave', 3.25)");

        await using var ctx = CreateCtx(cn);

        var items = await ctx.Query<PlainItem>().ToListAsync();

        Assert.Single(items);
        Assert.Equal("Dave", items[0].Name);
        Assert.Equal(3.25m, items[0].Price);
    }

    [Fact]
    public void No_renames_entity_all_columns_match_propname()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateCtx(cn);
        var mapping = ctx.GetMapping(typeof(PlainItem));

        // All columns should have Name == PropName (no renames).
        foreach (var col in mapping.Columns)
        {
            Assert.Equal(col.PropName, col.Name);
        }
    }

    // ── X1-5: HasFluentColumnRenames detection (indirect) ────────────────────
    // We cannot call HasFluentColumnRenames directly (it is private static).
    // Instead we exercise the detection logic through mapping inspection:
    // build mappings with and without fluent renames, verify column Name vs PropName.

    [Fact]
    public void Fluent_rename_detected_via_column_name_divergence()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateCtx(cn, mb =>
            mb.Entity<FluentRenameItem>()
              .Property(x => x.Name).HasColumnName("display_name"));

        var mapping = ctx.GetMapping(typeof(FluentRenameItem));
        var nameCol = mapping.Columns.First(c => c.PropName == "Name");

        // Fluent rename: Name (runtime) is "display_name", PropName is "Name".
        Assert.Equal("display_name", nameCol.Name);
        Assert.NotEqual(nameCol.Name, nameCol.PropName);

        // And no [Column] attribute on the property.
        var prop = typeof(FluentRenameItem).GetProperty("Name")!;
        Assert.Null(prop.GetCustomAttribute<ColumnAttribute>());
    }

    [Fact]
    public void No_fluent_rename_columns_match_expected_names()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateCtx(cn);
        var mapping = ctx.GetMapping(typeof(FluentRenameItem));

        // Without fluent config, Name property has no [Column] attribute →
        // col.Name == col.PropName == "Name".
        var nameCol = mapping.Columns.First(c => c.PropName == "Name");
        Assert.Equal("Name", nameCol.Name);
        Assert.Equal(nameCol.Name, nameCol.PropName);
    }

    [Fact]
    public void Attribute_rename_does_not_trigger_fluent_detection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateCtx(cn);
        var mapping = ctx.GetMapping(typeof(AttrRenameItem));

        // Name property has [Column("display_name")] → col.Name="display_name" but
        // the source generator also sees this. HasFluentColumnRenames checks:
        //   columnAttr == null && col.Name != col.PropName → true (fluent rename)
        // Since columnAttr != null, it returns false for this property.
        var nameCol = mapping.Columns.First(c => c.PropName == "Name");
        Assert.Equal("display_name", nameCol.Name);
        Assert.NotEqual(nameCol.Name, nameCol.PropName);

        // But there IS a [Column] attribute:
        var prop = typeof(AttrRenameItem).GetProperty("Name")!;
        Assert.NotNull(prop.GetCustomAttribute<ColumnAttribute>());
    }

    // ── X1-6: Multiple rows with fluent rename ──────────────────────────────
    // Verify that the runtime materializer handles multiple rows correctly
    // when falling back due to fluent rename.

    [Fact]
    public async Task Fluent_rename_materializes_multiple_rows()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE fluent_items (Id INTEGER PRIMARY KEY AUTOINCREMENT, display_name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0)");
        ExecNonQuery(cn, "INSERT INTO fluent_items (display_name, Price) VALUES ('Alice', 1.00)");
        ExecNonQuery(cn, "INSERT INTO fluent_items (display_name, Price) VALUES ('Bob', 2.00)");
        ExecNonQuery(cn, "INSERT INTO fluent_items (display_name, Price) VALUES ('Charlie', 3.00)");

        await using var ctx = CreateCtx(cn, mb =>
            mb.Entity<FluentRenameItem>()
              .Property(x => x.Name).HasColumnName("display_name"));

        var items = (await ctx.Query<FluentRenameItem>().ToListAsync()).OrderBy(i => i.Id).ToList();

        Assert.Equal(3, items.Count);
        Assert.Equal("Alice", items[0].Name);
        Assert.Equal("Bob", items[1].Name);
        Assert.Equal("Charlie", items[2].Name);
        Assert.Equal(1.00m, items[0].Price);
        Assert.Equal(2.00m, items[1].Price);
        Assert.Equal(3.00m, items[2].Price);
    }

    // ── X1-7: Fluent rename with WHERE clause ────────────────────────────────
    // Verify LINQ translation also uses the fluent column name in SQL predicates.

    [Fact]
    public async Task Fluent_rename_works_with_where_clause()
    {
        using var cn = CreateOpenDb(
            "CREATE TABLE fluent_items (Id INTEGER PRIMARY KEY AUTOINCREMENT, display_name TEXT NOT NULL DEFAULT '', Price REAL NOT NULL DEFAULT 0)");
        ExecNonQuery(cn, "INSERT INTO fluent_items (display_name, Price) VALUES ('Alice', 1.00)");
        ExecNonQuery(cn, "INSERT INTO fluent_items (display_name, Price) VALUES ('Bob', 2.00)");

        await using var ctx = CreateCtx(cn, mb =>
            mb.Entity<FluentRenameItem>()
              .Property(x => x.Name).HasColumnName("display_name"));

        var items = await ctx.Query<FluentRenameItem>()
            .Where(x => x.Name == "Bob")
            .ToListAsync();

        Assert.Single(items);
        Assert.Equal("Bob", items[0].Name);
        Assert.Equal(2.00m, items[0].Price);
    }

    // ── X1-8: Compiled materializers are registered for all test entities ────

    [Fact]
    public void All_test_entities_have_compiled_materializers_registered()
    {
        Assert.True(CompiledMaterializerStore.TryGet(typeof(FluentRenameItem), out _),
            "FluentRenameItem must have a compiled materializer.");
        Assert.True(CompiledMaterializerStore.TryGet(typeof(AttrRenameItem), out _),
            "AttrRenameItem must have a compiled materializer.");
        Assert.True(CompiledMaterializerStore.TryGet(typeof(MixedRenameItem), out _),
            "MixedRenameItem must have a compiled materializer.");
        Assert.True(CompiledMaterializerStore.TryGet(typeof(PlainItem), out _),
            "PlainItem must have a compiled materializer.");
    }
}
