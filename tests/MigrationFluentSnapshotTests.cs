using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// M1: CLI migration generation must consume fluent model configuration, not just
/// assembly attributes. These tests verify that <see cref="SchemaSnapshotBuilder.Build(DbContext)"/>
/// correctly reflects fluent ToTable, HasColumnName, and HasKey overrides, whereas
/// <see cref="SchemaSnapshotBuilder.Build(System.Reflection.Assembly)"/> only sees attributes.
/// </summary>
public class MigrationFluentSnapshotTests
{
    // ── Entity with NO mapping attributes — fluent-only configuration ────────

    public class FluentUser
    {
        public int Id { get; set; }
        public string DisplayName { get; set; } = string.Empty;
        public string Email { get; set; } = string.Empty;
    }

    // ── Entity with [Table]+[Key] for attribute baseline ─────────────────────

    [Table("attr_widgets")]
    public class AttrWidget
    {
        [Key]
        public int WidgetId { get; set; }
        public string Label { get; set; } = string.Empty;
    }

    // ── Test context ─────────────────────────────────────────────────────────

    private class FluentSnapshotTestContext : DbContext
    {
        public FluentSnapshotTestContext(SqliteConnection cn, SqliteProvider p, DbContextOptions opts)
            : base(cn, p, opts) { }
        public FluentSnapshotTestContext(SqliteConnection cn, SqliteProvider p)
            : base(cn, p) { }
    }

    private static FluentSnapshotTestContext CreateCtx(System.Action<ModelBuilder>? configure = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        if (configure != null)
        {
            var opts = new DbContextOptions { OnModelCreating = configure };
            return new FluentSnapshotTestContext(cn, new SqliteProvider(), opts);
        }
        return new FluentSnapshotTestContext(cn, new SqliteProvider());
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  1. Build(DbContext) captures fluent HasColumnName
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void M1_Build_DbContext_Captures_Fluent_HasColumnName()
    {
        using var ctx = CreateCtx(mb =>
            mb.Entity<FluentUser>()
              .ToTable("fluent_users")
              .HasKey(u => u.Id)
              .Property(u => u.DisplayName).HasColumnName("display_name"));

        var snapshot = SchemaSnapshotBuilder.Build(ctx);
        var table = snapshot.Tables.FirstOrDefault(t => t.Name == "fluent_users");

        Assert.NotNull(table);

        // The column name should be the fluent override, not the property name
        Assert.Contains(table!.Columns, c => c.Name == "display_name");
        Assert.DoesNotContain(table.Columns, c => c.Name == "DisplayName");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  2. Build(DbContext) captures fluent ToTable
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void M1_Build_DbContext_Captures_Fluent_ToTable()
    {
        using var ctx = CreateCtx(mb =>
            mb.Entity<FluentUser>()
              .ToTable("custom_user_table")
              .HasKey(u => u.Id));

        var snapshot = SchemaSnapshotBuilder.Build(ctx);
        var table = snapshot.Tables.FirstOrDefault(t => t.Name == "custom_user_table");

        Assert.NotNull(table);
        // FluentUser has no [Table] attribute, so the table name must come from fluent config
        Assert.DoesNotContain(snapshot.Tables, t => t.Name == "FluentUser");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  3. Build(assembly) does NOT capture fluent renames
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void M1_Build_Assembly_DoesNot_Capture_Fluent_Renames()
    {
        // FluentUser has no [Table] or [Key] attributes, so it should not appear
        // in the assembly scan at all.
        var asmSnapshot = SchemaSnapshotBuilder.Build(typeof(FluentUser).Assembly);

        // FluentUser should not appear under any fluent-configured name
        var fluentTable = asmSnapshot.Tables.FirstOrDefault(t => t.Name == "fluent_users");
        Assert.Null(fluentTable);

        // For an entity that IS visible in the assembly scan (AttrWidget with [Table]+[Key]),
        // the column names should be property names (no fluent override applied)
        var widgetTable = asmSnapshot.Tables.FirstOrDefault(t => t.Name == "attr_widgets");
        if (widgetTable != null)
        {
            // Column name should be the property name "Label", not any fluent override
            Assert.Contains(widgetTable.Columns, c => c.Name == "Label");
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  4. Diff between Build(assembly) and Build(DbContext) detects fluent changes
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void M1_Diff_Assembly_Vs_Context_Detects_Fluent_Changes()
    {
        // Build a context with fluent config for AttrWidget that renames a column
        using var ctx = CreateCtx(mb =>
            mb.Entity<AttrWidget>()
              .Property(w => w.Label).HasColumnName("widget_label"));

        var asmSnapshot = SchemaSnapshotBuilder.Build(typeof(AttrWidget).Assembly);
        var ctxSnapshot = SchemaSnapshotBuilder.Build(ctx);

        // Both snapshots should have the attr_widgets table
        var asmTable = asmSnapshot.Tables.FirstOrDefault(t => t.Name == "attr_widgets");
        var ctxTable = ctxSnapshot.Tables.FirstOrDefault(t => t.Name == "attr_widgets");

        Assert.NotNull(asmTable);
        Assert.NotNull(ctxTable);

        // Assembly snapshot: column named "Label" (property name)
        Assert.Contains(asmTable!.Columns, c => c.Name == "Label");
        Assert.DoesNotContain(asmTable.Columns, c => c.Name == "widget_label");

        // Context snapshot: column named "widget_label" (fluent override)
        Assert.Contains(ctxTable!.Columns, c => c.Name == "widget_label");
        Assert.DoesNotContain(ctxTable.Columns, c => c.Name == "Label");

        // Diff should detect the rename as a dropped + added column
        var diff = SchemaDiffer.Diff(asmSnapshot, ctxSnapshot);
        Assert.True(diff.HasChanges,
            "Diff between assembly-scanned and fluent-configured snapshots should detect schema changes.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  5. Build(DbContext) reflects fluent HasKey override
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void M1_Build_DbContext_Captures_Fluent_HasKey()
    {
        // Configure Email as PK instead of Id
        using var ctx = CreateCtx(mb =>
            mb.Entity<FluentUser>()
              .ToTable("fluent_users")
              .HasKey(u => u.Email));

        var snapshot = SchemaSnapshotBuilder.Build(ctx);
        var table = snapshot.Tables.First(t => t.Name == "fluent_users");

        var emailCol = table.Columns.FirstOrDefault(c => c.Name == "Email");
        Assert.NotNull(emailCol);
        Assert.True(emailCol!.IsPrimaryKey, "Fluent HasKey(Email) should mark Email as PK.");

        // Id should NOT be PK when overridden by fluent config
        var idCol = table.Columns.FirstOrDefault(c => c.Name == "Id");
        Assert.NotNull(idCol);
        Assert.False(idCol!.IsPrimaryKey, "Id should not be PK when Email is configured as PK via fluent API.");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  6. Build(DbContext) with multiple fluent column renames
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void M1_Build_DbContext_Multiple_Fluent_ColumnRenames()
    {
        using var ctx = CreateCtx(mb =>
            mb.Entity<FluentUser>()
              .ToTable("users")
              .HasKey(u => u.Id)
              .Property(u => u.DisplayName).HasColumnName("display_name")
              .Property(u => u.Email).HasColumnName("email_address"));

        var snapshot = SchemaSnapshotBuilder.Build(ctx);
        var table = snapshot.Tables.First(t => t.Name == "users");

        Assert.Contains(table.Columns, c => c.Name == "display_name");
        Assert.Contains(table.Columns, c => c.Name == "email_address");
        Assert.DoesNotContain(table.Columns, c => c.Name == "DisplayName");
        Assert.DoesNotContain(table.Columns, c => c.Name == "Email");
    }

    // ─────────────────────────────────────────────────────────────────────────
    //  7. Fluent-only entity absent from assembly snapshot, present in context
    // ─────────────────────────────────────────────────────────────────────────

    [Fact]
    public void M1_FluentOnly_Entity_Present_In_Context_Snapshot_Not_Assembly()
    {
        using var ctx = CreateCtx(mb =>
            mb.Entity<FluentUser>()
              .ToTable("fluent_users")
              .HasKey(u => u.Id));

        var asmSnapshot = SchemaSnapshotBuilder.Build(typeof(FluentUser).Assembly);
        var ctxSnapshot = SchemaSnapshotBuilder.Build(ctx);

        // FluentUser has no [Table]/[Key] → invisible to assembly scan
        Assert.Null(asmSnapshot.Tables.FirstOrDefault(t => t.Name == "fluent_users"));
        // But visible through context snapshot via fluent registration
        Assert.NotNull(ctxSnapshot.Tables.FirstOrDefault(t => t.Name == "fluent_users"));
    }
}
