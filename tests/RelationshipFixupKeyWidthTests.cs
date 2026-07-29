using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

/// <summary>
/// Relationship fixup copies a principal key into a dependent FK. When the two differ in CLR integer width
/// (e.g. a long principal key and an int FK), the boxed value can't be unboxed by the compiled FK setter
/// (Expression.Convert = hard cast), which historically threw InvalidCastException on a perfectly valid model
/// shape EF Core handles silently. Column.SetCoerced now normalizes the box width first, so the correct key
/// is persisted into the FK. Each scenario saves via nORM and reads the RAW stored value with a plain
/// SqliteCommand, asserting the DB holds the correct link across the collection, reference, and alternate-key
/// fixup paths; the matching-width scenarios confirm no regression.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class RelationshipFixupKeyWidthTests
{
    private static SqliteConnection Open(string ddl)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = ddl;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static long? FkScalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        var v = cmd.ExecuteScalar();
        return v == null || v is DBNull ? (long?)null : Convert.ToInt64(v);
    }

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Scenario 1: store-generated LONG principal PK, INT child FK, COLLECTION nav.
    // Direct write mirror: parent.Id (long, DB-generated) must propagate into child.ParentId (int).
    // ════════════════════════════════════════════════════════════════════════════════════════
    [Table("W52a_Parent")]
    public sealed class S1Parent
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public long Id { get; set; }
        public string Name { get; set; } = "";
        public List<S1Child> Children { get; set; } = new();
    }

    [Table("W52a_Child")]
    public sealed class S1Child
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int ParentId { get; set; }   // FK is INT; principal key is LONG
        public string Tag { get; set; } = "";
    }

    [Fact]
    public async Task S1_LongParentPk_IntChildFk_CollectionNav_GeneratedKey()
    {
        using var cn = Open(
            "CREATE TABLE W52a_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE W52a_Child (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S1Parent>().HasKey(p => p.Id);
                mb.Entity<S1Child>().HasKey(c => c.Id);
                mb.Entity<S1Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var parent = new S1Parent { Name = "P" };
        parent.Children.Add(new S1Child { Tag = "c1" });
        ctx.Add(parent);

        // The int FK is coerced from the DB-generated long principal key; the child persists linked.
        await ctx.SaveChangesAsync();
        Assert.NotEqual(0L, parent.Id);
        Assert.Equal(parent.Id, FkScalar(cn, "SELECT ParentId FROM W52a_Child WHERE Tag='c1'"));
    }

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Scenario 2: explicit (already-assigned) LONG principal PK, INT child FK, REFERENCE nav.
    // ════════════════════════════════════════════════════════════════════════════════════════
    [Table("W52b_Parent")]
    public sealed class S2Parent
    {
        [Key] public long Id { get; set; }                         // client-assigned wide key
        public string Name { get; set; } = "";
        public List<S2Child> Children { get; set; } = new();
    }

    [Table("W52b_Child")]
    public sealed class S2Child
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int ParentId { get; set; }                          // FK int
        public S2Parent? Parent { get; set; }
        public string Tag { get; set; } = "";
    }

    [Fact]
    public async Task S2_LongParentPk_IntChildFk_ReferenceNav_ExistingPrincipal()
    {
        using var cn = Open(
            "CREATE TABLE W52b_Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "CREATE TABLE W52b_Child (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);" +
            // A wide parent key value that still fits int, to isolate the boxing (not overflow).
            "INSERT INTO W52b_Parent (Id, Name) VALUES (7, 'P');");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S2Parent>().HasKey(p => p.Id);
                mb.Entity<S2Child>().HasKey(c => c.Id);
                mb.Entity<S2Parent>().HasMany(p => p.Children).WithOne(c => c.Parent!).HasForeignKey(c => c.ParentId);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var parent = await ctx.Query<S2Parent>().FirstAsync(p => p.Id == 7L);
        var child = new S2Child { Tag = "c2", Parent = parent };   // link via reference nav only
        ctx.Add(child);

        // Reference-nav fixup coerces the int FK from the long principal key; the child persists linked.
        await ctx.SaveChangesAsync();
        Assert.Equal(7L, FkScalar(cn, "SELECT ParentId FROM W52b_Child WHERE Tag='c2'"));
    }

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Scenario 3: alternate-key FK, principal alt-key LONG, dependent FK INT, reference nav to NEW principal.
    // ════════════════════════════════════════════════════════════════════════════════════════
    [Table("W52c_Warehouse")]
    public sealed class S3Warehouse
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public long LocationCode { get; set; }                     // ALT key (long)
        public string Name { get; set; } = "";
        public List<S3Shipment> Shipments { get; set; } = new();
    }

    [Table("W52c_Shipment")]
    public sealed class S3Shipment
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int WarehouseLocation { get; set; }                 // FK int -> Warehouse.LocationCode (long)
        public S3Warehouse? Warehouse { get; set; }
        public string Tracking { get; set; } = "";
    }

    [Fact]
    public async Task S3_LongAltKey_IntFk_ReferenceNav()
    {
        using var cn = Open(
            "CREATE TABLE W52c_Warehouse (Id INTEGER PRIMARY KEY AUTOINCREMENT, LocationCode INTEGER NOT NULL UNIQUE, Name TEXT NOT NULL);" +
            "CREATE TABLE W52c_Shipment (Id INTEGER PRIMARY KEY AUTOINCREMENT, WarehouseLocation INTEGER NOT NULL, Tracking TEXT NOT NULL);");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S3Warehouse>().HasKey(w => w.Id);
                mb.Entity<S3Shipment>().HasKey(s => s.Id);
                mb.Entity<S3Warehouse>().HasMany(w => w.Shipments).WithOne(s => s.Warehouse!)
                    .HasForeignKey(s => s.WarehouseLocation, w => w.LocationCode);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var warehouse = new S3Warehouse { LocationCode = 777, Name = "New" };
        var shipment = new S3Shipment { Tracking = "T3", Warehouse = warehouse };
        ctx.Add(warehouse);
        ctx.Add(shipment);

        // Alternate-key fixup coerces the int FK from the long LocationCode; the shipment persists linked.
        await ctx.SaveChangesAsync();
        Assert.Equal(777L, FkScalar(cn, "SELECT WarehouseLocation FROM W52c_Shipment WHERE Tracking='T3'"));
    }

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Scenario 4: many-to-many, LEFT PK int, RIGHT PK long (both store-generated), NEW right added.
    // The join row must carry the right entity's generated long key.
    // ════════════════════════════════════════════════════════════════════════════════════════
    [Table("W52d_Post")]
    public sealed class S4Post
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<S4Tag> Tags { get; set; } = new();
    }

    [Table("W52d_Tag")]
    public sealed class S4Tag
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public long Id { get; set; }  // wide right key
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task S4_M2M_IntLeftPk_LongRightPk_NewRight()
    {
        using var cn = Open(
            "CREATE TABLE W52d_Post (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);" +
            "CREATE TABLE W52d_Tag (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE W52d_PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL, PRIMARY KEY(PostId, TagId));");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S4Post>().HasKey(p => p.Id);
                mb.Entity<S4Tag>().HasKey(t => t.Id);
                mb.Entity<S4Post>().HasMany(p => p.Tags).WithMany()
                    .UsingTable("W52d_PostTag", "PostId", "TagId");
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var post = new S4Post { Title = "Hello" };
        post.Tags.Add(new S4Tag { Name = "t1" });
        ctx.Add(post);
        await ctx.SaveChangesAsync();

        var tagId = post.Tags[0].Id;                               // long, generated
        var joinRight = FkScalar(cn, "SELECT TagId FROM W52d_PostTag");
        var joinLeft  = FkScalar(cn, "SELECT PostId FROM W52d_PostTag");
        Assert.Equal(post.Id, joinLeft);
        Assert.Equal(tagId, joinRight);
        Assert.NotEqual(0L, tagId);
    }

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Scenario 5: SHORT store-generated principal PK + SHORT child FK, collection nav.
    // SQLite returns Int64 for the generated key; SetPrimaryKey coerces to short. Verify the
    // propagated FK (short getter -> short setter) links correctly.
    // ════════════════════════════════════════════════════════════════════════════════════════
    [Table("W52e_Parent")]
    public sealed class S5Parent
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public short Id { get; set; }
        public string Name { get; set; } = "";
        public List<S5Child> Children { get; set; } = new();
    }

    [Table("W52e_Child")]
    public sealed class S5Child
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public short ParentId { get; set; }
        public string Tag { get; set; } = "";
    }

    [Fact]
    public async Task S5_ShortParentPk_ShortChildFk_CollectionNav()
    {
        using var cn = Open(
            "CREATE TABLE W52e_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE W52e_Child (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S5Parent>().HasKey(p => p.Id);
                mb.Entity<S5Child>().HasKey(c => c.Id);
                mb.Entity<S5Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var parent = new S5Parent { Name = "P" };
        parent.Children.Add(new S5Child { Tag = "c5" });
        ctx.Add(parent);
        await ctx.SaveChangesAsync();

        Assert.Equal((long)parent.Id, FkScalar(cn, "SELECT ParentId FROM W52e_Child WHERE Tag='c5'"));
    }

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Scenario 6: UINT store-generated principal PK, uint child FK, collection nav.
    // ════════════════════════════════════════════════════════════════════════════════════════
    [Table("W52f_Parent")]
    public sealed class S6Parent
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public uint Id { get; set; }
        public string Name { get; set; } = "";
        public List<S6Child> Children { get; set; } = new();
    }

    [Table("W52f_Child")]
    public sealed class S6Child
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public uint ParentId { get; set; }
        public string Tag { get; set; } = "";
    }

    [Fact]
    public async Task S6_UintParentPk_UintChildFk_CollectionNav()
    {
        using var cn = Open(
            "CREATE TABLE W52f_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
            "CREATE TABLE W52f_Child (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S6Parent>().HasKey(p => p.Id);
                mb.Entity<S6Child>().HasKey(c => c.Id);
                mb.Entity<S6Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var parent = new S6Parent { Name = "P" };
        parent.Children.Add(new S6Child { Tag = "c6" });
        ctx.Add(parent);
        await ctx.SaveChangesAsync();

        Assert.Equal((long)parent.Id, FkScalar(cn, "SELECT ParentId FROM W52f_Child WHERE Tag='c6'"));
    }

    // ════════════════════════════════════════════════════════════════════════════════════════
    // Scenario 7: editing a tracked dependent's FK to point at another TRACKED principal must
    // reconcile the reference nav to that principal — even across a long-PK / int-FK width gap.
    // The identity-map lookup keyed by the long PK must match the coerced int FK, not miss and
    // silently null the navigation (the FK itself always persists correctly).
    // ════════════════════════════════════════════════════════════════════════════════════════
    [Table("W52k_Prin")]
    public sealed class S7Prin
    {
        [Key] public long Id { get; set; }               // long PK
        public string Name { get; set; } = "";
        public List<S7Dep> Deps { get; set; } = new();
    }

    [Table("W52k_Dep")]
    public sealed class S7Dep
    {
        [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int PrinId { get; set; }                  // int FK vs long PK
        public S7Prin? Prin { get; set; }
        public string Tag { get; set; } = "";
    }

    [Fact]
    public async Task ReferenceNav_reconciles_to_tracked_principal_after_fk_edit_across_key_width()
    {
        using var cn = Open(
            "CREATE TABLE W52k_Prin (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "CREATE TABLE W52k_Dep (Id INTEGER PRIMARY KEY AUTOINCREMENT, PrinId INTEGER NOT NULL, Tag TEXT NOT NULL);" +
            "INSERT INTO W52k_Prin (Id,Name) VALUES (1,'p1'),(2,'p2');" +
            "INSERT INTO W52k_Dep (Id,PrinId,Tag) VALUES (10,1,'d');");
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<S7Prin>().HasKey(p => p.Id);
                mb.Entity<S7Dep>().HasKey(d => d.Id);
                mb.Entity<S7Prin>().HasMany(p => p.Deps).WithOne(d => d.Prin!).HasForeignKey(d => d.PrinId);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var dep = await ctx.Query<S7Dep>().Include(d => d.Prin).FirstAsync();
        var p2 = await ctx.Query<S7Prin>().FirstAsync(p => p.Id == 2L); // track principal 2
        Assert.Equal("p1", dep.Prin!.Name);

        dep.PrinId = 2;                                   // repoint the FK at principal 2
        await ctx.SaveChangesAsync();

        // FK persists correctly regardless of the nav reconciliation.
        Assert.Equal(2L, FkScalar(cn, "SELECT PrinId FROM W52k_Dep WHERE Tag='d'"));
        // Nav must resolve to the tracked principal 2, not be silently cleared to null.
        Assert.NotNull(dep.Prin);
        Assert.Same(p2, dep.Prin);
    }
}
