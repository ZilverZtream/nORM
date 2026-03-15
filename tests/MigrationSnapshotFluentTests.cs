using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Migration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// SchemaSnapshotBuilder.Build(DbContext) must reflect fluent model configuration.
/// </summary>
public class MigrationSnapshotFluentTests
{
    // Entity with no attributes — must be configured entirely via fluent API
    public class AppUser
    {
        public int Id { get; set; }
        public string FullName { get; set; } = string.Empty;
        public string Code { get; set; } = string.Empty;
    }

    // Entity with [Table] and [Key] for attribute-only test
    [Table("attr_products")]
    public class AttrProduct
    {
        [Key]
        public int ProductId { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    // Derived context so ctx.GetType().Assembly returns the test assembly
    private class SnapshotTestContext : DbContext
    {
        public SnapshotTestContext(SqliteConnection cn, SqliteProvider p, DbContextOptions opts)
            : base(cn, p, opts) { }
        public SnapshotTestContext(SqliteConnection cn, SqliteProvider p)
            : base(cn, p) { }
    }

    private static SnapshotTestContext CreateCtx(System.Action<ModelBuilder>? configure = null)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        if (configure != null)
        {
            var opts = new DbContextOptions { OnModelCreating = configure };
            return new SnapshotTestContext(cn, new SqliteProvider(), opts);
        }
        return new SnapshotTestContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Build_WithContext_ReflectsFluentTableName()
    {
        using var ctx = CreateCtx(mb =>
            mb.Entity<AppUser>()
              .ToTable("app_users")
              .HasKey(u => u.Id));

        var snapshot = SchemaSnapshotBuilder.Build(ctx);
        var table = snapshot.Tables.FirstOrDefault(t => t.Name == "app_users");

        Assert.NotNull(table);
    }

    [Fact]
    public void Build_WithContext_ReflectsFluentColumnName()
    {
        using var ctx = CreateCtx(mb =>
            mb.Entity<AppUser>()
              .ToTable("app_users")
              .HasKey(u => u.Id)
              .Property(u => u.FullName).HasColumnName("full_name"));

        var snapshot = SchemaSnapshotBuilder.Build(ctx);
        var table = snapshot.Tables.First(t => t.Name == "app_users");

        Assert.Contains(table.Columns, c => c.Name == "full_name");
        Assert.DoesNotContain(table.Columns, c => c.Name == "FullName");
    }

    [Fact]
    public void Build_WithContext_ReflectsFluentPrimaryKey()
    {
        using var ctx = CreateCtx(mb =>
            mb.Entity<AppUser>()
              .ToTable("app_users")
              .HasKey(u => u.Code));

        var snapshot = SchemaSnapshotBuilder.Build(ctx);
        var table = snapshot.Tables.First(t => t.Name == "app_users");

        var codeCol = table.Columns.FirstOrDefault(c => c.Name == "Code");
        Assert.NotNull(codeCol);
        Assert.True(codeCol!.IsPrimaryKey);

        // Id should NOT be PK (only Code is via fluent HasKey)
        var idCol = table.Columns.FirstOrDefault(c => c.Name == "Id");
        Assert.NotNull(idCol);
        Assert.False(idCol!.IsPrimaryKey);
    }

    [Fact]
    public void Build_Assembly_vs_Context_AttributeOnly_Parity()
    {
        // Register AttrProduct explicitly so it appears in GetAllMappings()
        // Both Build(Assembly) and Build(ctx) should produce the same schema for attribute-only entities
        using var ctx = CreateCtx(mb => mb.Entity<AttrProduct>());

        var asmSnapshot = SchemaSnapshotBuilder.Build(ctx.GetType().Assembly);
        var ctxSnapshot = SchemaSnapshotBuilder.Build(ctx);

        var asmTable = asmSnapshot.Tables.FirstOrDefault(t => t.Name == "attr_products");
        var ctxTable = ctxSnapshot.Tables.FirstOrDefault(t => t.Name == "attr_products");

        Assert.NotNull(asmTable);
        Assert.NotNull(ctxTable);

        var asmCols = asmTable!.Columns.OrderBy(c => c.Name).ToList();
        var ctxCols = ctxTable!.Columns.OrderBy(c => c.Name).ToList();

        Assert.Equal(asmCols.Count, ctxCols.Count);
        for (int i = 0; i < asmCols.Count; i++)
        {
            Assert.Equal(asmCols[i].Name, ctxCols[i].Name);
            Assert.Equal(asmCols[i].IsPrimaryKey, ctxCols[i].IsPrimaryKey);
        }
    }

    [Fact]
    public void Build_WithContext_FluentOnly_EntityAppears_InContextSnapshot()
    {
        // Fluent-only entity (no [Table] attr, no [Key]) appears in context snapshot but not assembly snapshot
        using var ctx = CreateCtx(mb =>
            mb.Entity<AppUser>()
              .ToTable("app_users")
              .HasKey(u => u.Id));

        var asmSnapshot = SchemaSnapshotBuilder.Build(ctx.GetType().Assembly);
        var ctxSnapshot = SchemaSnapshotBuilder.Build(ctx);

        // Assembly-only scan: AppUser has no [Table] and no [Key], so it won't appear
        var inAsm = asmSnapshot.Tables.FirstOrDefault(t => t.Name == "app_users");
        // Context scan: fluent config is visible
        var inCtx = ctxSnapshot.Tables.FirstOrDefault(t => t.Name == "app_users");

        Assert.Null(inAsm);    // no attribute → not in assembly snapshot
        Assert.NotNull(inCtx); // fluent config → visible via context snapshot
    }
}
