using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial audit: SaveChanges FK-dependency ordering + generated-key propagation across DIFFERENT
/// entity types (reference-direction, multi-level, diamond, delete-ordering, optional-FK, existing-parent).
/// All FK columns carry NO DB constraint, so a dangling/zero FK is a SILENT wrong stored value, not an error.
/// Every assertion reads the RAW column back through a keeper connection.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CrossTypeGraphOrderingAuditTests
{
    // ---- Single-level reference direction ----
    [Table("XdgA")] public class A { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Name { get; set; } = ""; }
    [Table("XdgB")] public class B { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public int AId { get; set; } public A? A { get; set; } public string Name { get; set; } = ""; }

    // ---- Optional (nullable) FK ----
    [Table("XdgOpt")] public class OptChild { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public int? AId { get; set; } public A? A { get; set; } public string Name { get; set; } = ""; }

    // ---- Multi-level A -> B -> C ----
    [Table("XdgGrand")] public class Grand { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Name { get; set; } = ""; }
    [Table("XdgParent")] public class Parent { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public int GrandId { get; set; } public Grand? Grand { get; set; } public string Name { get; set; } = ""; }
    [Table("XdgChild")] public class Child { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public int ParentId { get; set; } public Parent? Parent { get; set; } public string Name { get; set; } = ""; }

    // ---- Diamond, different principal types ----
    [Table("XdgLeft")] public class Left { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Name { get; set; } = ""; }
    [Table("XdgRight")] public class Right { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Name { get; set; } = ""; }
    [Table("XdgNode")] public class Node { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public int LeftId { get; set; } public Left? Left { get; set; } public int RightId { get; set; } public Right? Right { get; set; } public string Name { get; set; } = ""; }

    // ---- Diamond, SAME principal type via two navs ----
    [Table("XdgPerson")] public class Person { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Name { get; set; } = ""; }
    [Table("XdgDoc")] public class Doc { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public int AuthorId { get; set; } public Person? Author { get; set; } public int EditorId { get; set; } public Person? Editor { get; set; } public string Name { get; set; } = ""; }

    // ---- Delete ordering (collection nav so a relation + cascade is discovered) ----
    [Table("XdgPar")] public class Par { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public List<Kid> Kids { get; set; } = new(); public string Name { get; set; } = ""; }
    [Table("XdgKid")] public class Kid { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public int ParId { get; set; } public string Name { get; set; } = ""; }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup(string ddl, Action<ModelBuilder>? model = null, bool enforceFk = false)
    {
        var dbName = $"xdg_{Guid.NewGuid():N}";
        var keeper = new SqliteConnection($"Data Source=file:{dbName}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText = ddl;
            cmd.ExecuteNonQuery();
        }
        var connString = enforceFk
            ? $"Data Source=file:{dbName}?mode=memory&cache=shared;Foreign Keys=True"
            : keeper.ConnectionString;
        DbContext Make()
        {
            var cn = new SqliteConnection(connString);
            cn.Open();
            var opts = new DbContextOptions();
            if (model != null) opts.OnModelCreating = model;
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<object?[]> Rows(SqliteConnection k, string sql)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = sql;
        using var r = cmd.ExecuteReader();
        var list = new List<object?[]>();
        while (r.Read())
        {
            var row = new object?[r.FieldCount];
            r.GetValues(row!);
            list.Add(row);
        }
        return list;
    }

    private static object? Scalar(SqliteConnection k, string sql)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    // ============================================================
    // Scenario 1: ref-direction, both added, FK unset
    // ============================================================
    [Fact]
    public async Task RefDirection_bothAdded_childFkGetsGeneratedParentKey()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgA (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgB (Id INTEGER PRIMARY KEY AUTOINCREMENT, AId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var a = new A { Name = "parent" };
        var b = new B { Name = "child", A = a };   // reference nav set, AId left 0
        ctx.Add(a);
        ctx.Add(b);
        await ctx.SaveChangesAsync();

        Assert.True(a.Id > 0, "parent generated key");
        var rawFk = Convert.ToInt64(Scalar(keeper, "SELECT AId FROM XdgB")!);
        Assert.Equal((long)a.Id, rawFk);   // silent-wrong would be 0
        Assert.Equal(1L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgA")!));
        Assert.Equal(1L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgB")!));
    }

    // Same, but child added FIRST (attach order reversed).
    [Fact]
    public async Task RefDirection_childAddedFirst_childFkGetsGeneratedParentKey()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgA (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgB (Id INTEGER PRIMARY KEY AUTOINCREMENT, AId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var a = new A { Name = "parent" };
        var b = new B { Name = "child", A = a };
        ctx.Add(b);   // child first
        ctx.Add(a);
        await ctx.SaveChangesAsync();

        var rawFk = Convert.ToInt64(Scalar(keeper, "SELECT AId FROM XdgB")!);
        Assert.Equal((long)a.Id, rawFk);
    }

    // Only the child is Add()ed; the parent is discovered through the reference nav.
    [Fact]
    public async Task RefDirection_onlyChildAdded_parentDiscovered_fkPropagated()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgA (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgB (Id INTEGER PRIMARY KEY AUTOINCREMENT, AId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var a = new A { Name = "parent" };
        var b = new B { Name = "child", A = a };
        ctx.Add(b);   // only child added
        await ctx.SaveChangesAsync();

        Assert.True(a.Id > 0, "parent discovered + inserted");
        Assert.Equal(1L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgA")!));
        var rawFk = Convert.ToInt64(Scalar(keeper, "SELECT AId FROM XdgB")!);
        Assert.Equal((long)a.Id, rawFk);
    }

    // ============================================================
    // Scenario 2: multi-level A -> B -> C
    // ============================================================
    [Fact]
    public async Task MultiLevel_threeTypes_allFksPropagated()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgGrand (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, GrandId INTEGER NOT NULL, Name TEXT);" +
            "CREATE TABLE XdgChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var g = new Grand { Name = "g" };
        var p = new Parent { Name = "p", Grand = g };
        var c = new Child { Name = "c", Parent = p };
        // Add in a deliberately non-topological order.
        ctx.Add(c);
        ctx.Add(g);
        ctx.Add(p);
        await ctx.SaveChangesAsync();

        Assert.True(g.Id > 0 && p.Id > 0 && c.Id > 0);
        Assert.Equal((long)g.Id, Convert.ToInt64(Scalar(keeper, "SELECT GrandId FROM XdgParent")!));
        Assert.Equal((long)p.Id, Convert.ToInt64(Scalar(keeper, "SELECT ParentId FROM XdgChild")!));
    }

    // Multi-level, only the leaf added; middle + top discovered through reference navs.
    [Fact]
    public async Task MultiLevel_onlyLeafAdded_chainDiscovered_allFksPropagated()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgGrand (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, GrandId INTEGER NOT NULL, Name TEXT);" +
            "CREATE TABLE XdgChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var g = new Grand { Name = "g" };
        var p = new Parent { Name = "p", Grand = g };
        var c = new Child { Name = "c", Parent = p };
        ctx.Add(c);   // ONLY the leaf
        await ctx.SaveChangesAsync();

        Assert.Equal(1L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgGrand")!));
        Assert.Equal(1L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgParent")!));
        Assert.True(g.Id > 0 && p.Id > 0);
        Assert.Equal((long)g.Id, Convert.ToInt64(Scalar(keeper, "SELECT GrandId FROM XdgParent")!));
        Assert.Equal((long)p.Id, Convert.ToInt64(Scalar(keeper, "SELECT ParentId FROM XdgChild")!));
    }

    // ============================================================
    // Scenario 3: diamond, different principal types
    // ============================================================
    [Fact]
    public async Task Diamond_differentTypes_bothFksPropagated()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgLeft (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgRight (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgNode (Id INTEGER PRIMARY KEY AUTOINCREMENT, LeftId INTEGER NOT NULL, RightId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var l = new Left { Name = "l" };
        var r = new Right { Name = "r" };
        var n = new Node { Name = "n", Left = l, Right = r };
        ctx.Add(n);
        ctx.Add(l);
        ctx.Add(r);
        await ctx.SaveChangesAsync();

        Assert.True(l.Id > 0 && r.Id > 0);
        var row = Rows(keeper, "SELECT LeftId, RightId FROM XdgNode").Single();
        Assert.Equal((long)l.Id, Convert.ToInt64(row[0]!));
        Assert.Equal((long)r.Id, Convert.ToInt64(row[1]!));
    }

    // ============================================================
    // Scenario 3b: diamond, SAME principal type through two distinct navs
    // ============================================================
    [Fact]
    public async Task Diamond_sameType_twoNavs_bothFksBindCorrectColumns()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgPerson (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgDoc (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER NOT NULL, EditorId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var author = new Person { Name = "author" };
        var editor = new Person { Name = "editor" };
        var doc = new Doc { Name = "doc", Author = author, Editor = editor };
        ctx.Add(doc);
        ctx.Add(author);
        ctx.Add(editor);
        await ctx.SaveChangesAsync();

        Assert.True(author.Id > 0 && editor.Id > 0 && author.Id != editor.Id);
        var row = Rows(keeper, "SELECT AuthorId, EditorId FROM XdgDoc").Single();
        Assert.Equal((long)author.Id, Convert.ToInt64(row[0]!));   // must NOT swap
        Assert.Equal((long)editor.Id, Convert.ToInt64(row[1]!));
    }

    // ============================================================
    // Scenario 4: delete ordering (cross-type, cascade) — orphan check
    // ============================================================
    [Fact]
    public async Task Delete_parentWithLoadedChildren_cascades_noOrphans()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgPar (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgKid (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParId INTEGER NOT NULL, Name TEXT);" +
            "INSERT INTO XdgPar (Id, Name) VALUES (1, 'p');" +
            "INSERT INTO XdgKid (Id, ParId, Name) VALUES (10, 1, 'a'), (11, 1, 'b');");
        using var _ = keeper;
        await using var ctx = make();

        var par = ((INormQueryable<Par>)ctx.Query<Par>()).Include(p => p.Kids).ToList().Single();
        Assert.Equal(2, par.Kids.Count);
        ctx.Remove(par);
        await ctx.SaveChangesAsync();

        Assert.Equal(0L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgPar")!));
        Assert.Equal(0L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgKid")!));   // orphans would remain
    }

    // Delete both types explicitly in one save — children must delete before parent.
    [Fact]
    public async Task Delete_bothTypesExplicit_childBeforeParent_allGone()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgPar (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgKid (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParId INTEGER NOT NULL, Name TEXT);" +
            "INSERT INTO XdgPar (Id, Name) VALUES (1, 'p');" +
            "INSERT INTO XdgKid (Id, ParId, Name) VALUES (10, 1, 'a'), (11, 1, 'b');");
        using var _ = keeper;
        await using var ctx = make();

        var par = ((INormQueryable<Par>)ctx.Query<Par>()).Include(p => p.Kids).ToList().Single();
        foreach (var k in par.Kids.ToList()) ctx.Remove(k);
        ctx.Remove(par);
        await ctx.SaveChangesAsync();

        Assert.Equal(0L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgPar")!));
        Assert.Equal(0L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgKid")!));
    }

    // ============================================================
    // Scenario 5: optional (nullable) FK left null, no parent
    // ============================================================
    [Fact]
    public async Task OptionalFk_noParent_persistsNull()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgA (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgOpt (Id INTEGER PRIMARY KEY AUTOINCREMENT, AId INTEGER NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var oc = new OptChild { Name = "orphan", A = null, AId = null };
        ctx.Add(oc);
        await ctx.SaveChangesAsync();

        Assert.Equal(1L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgOpt")!));
        var raw = Scalar(keeper, "SELECT AId FROM XdgOpt");
        Assert.True(raw == null || raw == DBNull.Value, $"AId should be NULL, was '{raw}'");
    }

    // Optional FK with a NEW parent set via nav — still propagates.
    [Fact]
    public async Task OptionalFk_newParent_propagatesGeneratedKey()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgA (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgOpt (Id INTEGER PRIMARY KEY AUTOINCREMENT, AId INTEGER NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var a = new A { Name = "p" };
        var oc = new OptChild { Name = "c", A = a };
        ctx.Add(oc);
        ctx.Add(a);
        await ctx.SaveChangesAsync();

        Assert.True(a.Id > 0);
        var raw = Scalar(keeper, "SELECT AId FROM XdgOpt");
        Assert.Equal((long)a.Id, Convert.ToInt64(raw!));
    }

    // ============================================================
    // Scenario 6: nav points at an EXISTING (already-persisted, tracked) parent
    // ============================================================
    [Fact]
    public async Task RefDirection_existingTrackedParent_fkBindsExistingKey()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgA (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgB (Id INTEGER PRIMARY KEY AUTOINCREMENT, AId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;

        int existingAId;
        await using (var seed = make())
        {
            var a = new A { Name = "existing" };
            seed.Add(a);
            await seed.SaveChangesAsync();
            existingAId = a.Id;
        }
        Assert.True(existingAId > 0);

        await using (var ctx = make())
        {
            var a = ctx.Query<A>().ToList().Single();   // load + track existing parent
            var b = new B { Name = "child", A = a };
            ctx.Add(b);
            await ctx.SaveChangesAsync();
        }

        var rawFk = Convert.ToInt64(Scalar(keeper, "SELECT AId FROM XdgB")!);
        Assert.Equal((long)existingAId, rawFk);
    }

    // ============================================================
    // Scenario 7: heterogeneous reference-direction batch — 2 parents, 2 children,
    // each child points at its OWN new parent. Children insert in a single batch,
    // so a per-row FK desync would swap or zero the stored value.
    // ============================================================
    [Fact]
    public async Task RefDirection_heterogeneousBatch_eachChildGetsItsOwnParentKey()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgA (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgB (Id INTEGER PRIMARY KEY AUTOINCREMENT, AId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var a1 = new A { Name = "a1" };
        var a2 = new A { Name = "a2" };
        var b1 = new B { Name = "b1", A = a1 };
        var b2 = new B { Name = "b2", A = a2 };
        // interleave the add order
        ctx.Add(b1);
        ctx.Add(a2);
        ctx.Add(b2);
        ctx.Add(a1);
        await ctx.SaveChangesAsync();

        Assert.True(a1.Id > 0 && a2.Id > 0 && a1.Id != a2.Id);
        var rows = Rows(keeper, "SELECT Name, AId FROM XdgB ORDER BY Name")
            .ToDictionary(r => (string)r[0]!, r => Convert.ToInt64(r[1]!));
        Assert.Equal((long)a1.Id, rows["b1"]);   // must not be a2.Id or 0
        Assert.Equal((long)a2.Id, rows["b2"]);
    }

    // ============================================================
    // Scenario 8: collection-direction multi-parent batch — 2 parents each with 2 kids.
    // Parents insert in one batch, per-row key propagation must give each kid its
    // OWN parent's generated key. Kids then insert in one batch.
    // ============================================================
    [Fact]
    public async Task CollectionDirection_multiParentBatch_eachKidGetsItsOwnParentKey()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgPar (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgKid (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var p1 = new Par { Name = "p1" };
        p1.Kids.Add(new Kid { Name = "k1a" });
        p1.Kids.Add(new Kid { Name = "k1b" });
        var p2 = new Par { Name = "p2" };
        p2.Kids.Add(new Kid { Name = "k2a" });
        p2.Kids.Add(new Kid { Name = "k2b" });
        ctx.Add(p1);
        ctx.Add(p2);
        await ctx.SaveChangesAsync();

        Assert.True(p1.Id > 0 && p2.Id > 0 && p1.Id != p2.Id);
        var rows = Rows(keeper, "SELECT Name, ParId FROM XdgKid")
            .ToDictionary(r => (string)r[0]!, r => Convert.ToInt64(r[1]!));
        Assert.Equal((long)p1.Id, rows["k1a"]);
        Assert.Equal((long)p1.Id, rows["k1b"]);
        Assert.Equal((long)p2.Id, rows["k2a"]);
        Assert.Equal((long)p2.Id, rows["k2b"]);
    }

    // ---- Soft-edge-only ordering: FK column name does NOT encode the principal type,
    // only [ForeignKey] links it to the nav. Ordering must rely on the reference-nav
    // soft edge alone. ----
    [Table("XdgOwner")] public class Owner { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Name { get; set; } = ""; }
    [Table("XdgItem")] public class Item
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        [ForeignKey(nameof(Holder))] public int HolderRef { get; set; }   // name does not encode "Owner"
        public Owner? Holder { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task SoftEdgeOnly_ordering_propagatesGeneratedKey()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgOwner (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, HolderRef INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var o = new Owner { Name = "o" };
        var it = new Item { Name = "i", Holder = o };
        ctx.Add(it);   // dependent added first, only soft edge to order by
        ctx.Add(o);
        await ctx.SaveChangesAsync();

        Assert.True(o.Id > 0);
        var raw = Convert.ToInt64(Scalar(keeper, "SELECT HolderRef FROM XdgItem")!);
        Assert.Equal((long)o.Id, raw);
    }

    // ---- Reparent an EXISTING (Modified) dependent to a NEW principal via reference nav.
    // The FK is propagated post-insert (after change detection); a partial-column UPDATE
    // must still emit the FK column. ----
    [Table("XdgHome")] public class Home { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Name { get; set; } = ""; }
    [Table("XdgPet")] public class Pet
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int HomeId { get; set; }
        public Home? Home { get; set; }
        public string Name { get; set; } = "";
    }

    // Variant 1: only the reference nav changes (no other scalar edit).
    [Fact]
    public async Task Reparent_existingDependent_toNewPrincipal_navOnly_fkUpdated()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgHome (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgPet (Id INTEGER PRIMARY KEY AUTOINCREMENT, HomeId INTEGER NOT NULL, Name TEXT);" +
            "INSERT INTO XdgHome (Id, Name) VALUES (1, 'old');" +
            "INSERT INTO XdgPet (Id, HomeId, Name) VALUES (10, 1, 'rex');");
        using var _ = keeper;
        await using var ctx = make();

        var pet = ctx.Query<Pet>().ToList().Single();
        Assert.Equal(1, pet.HomeId);
        var newHome = new Home { Name = "new" };
        pet.Home = newHome;   // reparent via nav only; HomeId not touched by caller
        ctx.Add(newHome);
        await ctx.SaveChangesAsync();

        Assert.True(newHome.Id > 1);
        var raw = Convert.ToInt64(Scalar(keeper, "SELECT HomeId FROM XdgPet WHERE Id = 10")!);
        Assert.Equal((long)newHome.Id, raw);   // silent-wrong: stays 1
    }

    // Variant 2: the reference nav changes AND another scalar (Name) changes — the partial
    // UPDATE would carry only the value-detected column and could DROP the post-insert FK.
    [Fact]
    public async Task Reparent_existingDependent_toNewPrincipal_navPlusScalarEdit_fkUpdated()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgHome (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgPet (Id INTEGER PRIMARY KEY AUTOINCREMENT, HomeId INTEGER NOT NULL, Name TEXT);" +
            "INSERT INTO XdgHome (Id, Name) VALUES (1, 'old');" +
            "INSERT INTO XdgPet (Id, HomeId, Name) VALUES (10, 1, 'rex');");
        using var _ = keeper;
        await using var ctx = make();

        var pet = ctx.Query<Pet>().ToList().Single();
        var newHome = new Home { Name = "new" };
        pet.Home = newHome;      // reparent via nav
        pet.Name = "renamed";    // ALSO edit a plain scalar
        ctx.Add(newHome);
        await ctx.SaveChangesAsync();

        Assert.True(newHome.Id > 1);
        var row = Rows(keeper, "SELECT HomeId, Name FROM XdgPet WHERE Id = 10").Single();
        Assert.Equal("renamed", (string)row[1]!);
        Assert.Equal((long)newHome.Id, Convert.ToInt64(row[0]!));   // silent-wrong: stays 1 (FK dropped from partial UPDATE)
    }

    // ============================================================
    // Enforced-FK sensitivity tests: a wrong insert/delete order becomes a LOUD
    // SQLite constraint error, so passing proves the order is correct end-to-end.
    // ============================================================
    [Fact]
    public async Task InsertOrder_enforcedFk_refDirection_parentBeforeChild()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgA (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgB (Id INTEGER PRIMARY KEY AUTOINCREMENT, AId INTEGER NOT NULL, Name TEXT, FOREIGN KEY(AId) REFERENCES XdgA(Id));",
            enforceFk: true);
        using var _ = keeper;
        await using var ctx = make();

        var a = new A { Name = "p" };
        var b = new B { Name = "c", A = a };
        ctx.Add(b);   // child first in add order
        ctx.Add(a);
        await ctx.SaveChangesAsync();   // throws if child inserted before parent, or FK still 0

        var raw = Convert.ToInt64(Scalar(keeper, "SELECT AId FROM XdgB")!);
        Assert.Equal((long)a.Id, raw);
    }

    [Fact]
    public async Task InsertOrder_enforcedFk_multiLevel_chainOrder()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgGrand (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, GrandId INTEGER NOT NULL, Name TEXT, FOREIGN KEY(GrandId) REFERENCES XdgGrand(Id));" +
            "CREATE TABLE XdgChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Name TEXT, FOREIGN KEY(ParentId) REFERENCES XdgParent(Id));",
            enforceFk: true);
        using var _ = keeper;
        await using var ctx = make();

        var g = new Grand { Name = "g" };
        var p = new Parent { Name = "p", Grand = g };
        var c = new Child { Name = "c", Parent = p };
        ctx.Add(c);
        ctx.Add(p);
        ctx.Add(g);
        await ctx.SaveChangesAsync();

        Assert.Equal((long)g.Id, Convert.ToInt64(Scalar(keeper, "SELECT GrandId FROM XdgParent")!));
        Assert.Equal((long)p.Id, Convert.ToInt64(Scalar(keeper, "SELECT ParentId FROM XdgChild")!));
    }

    [Fact]
    public async Task DeleteOrder_enforcedFk_multiLevelCascade_childBeforeParent()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgPar (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgKid (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParId INTEGER NOT NULL, Name TEXT, FOREIGN KEY(ParId) REFERENCES XdgPar(Id));" +
            "INSERT INTO XdgPar (Id, Name) VALUES (1, 'p');" +
            "INSERT INTO XdgKid (Id, ParId, Name) VALUES (10, 1, 'a'), (11, 1, 'b');",
            enforceFk: true);
        using var _ = keeper;
        await using var ctx = make();

        var par = ((INormQueryable<Par>)ctx.Query<Par>()).Include(p => p.Kids).ToList().Single();
        ctx.Remove(par);
        await ctx.SaveChangesAsync();   // throws if parent deleted before its children under FK enforcement

        Assert.Equal(0L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgPar")!));
        Assert.Equal(0L, Convert.ToInt64(Scalar(keeper, "SELECT COUNT(*) FROM XdgKid")!));
    }

    // ---- Legal-in-EF chicken-and-egg: optional FK breaks the cycle. Characterize nORM:
    // silent-wrong (one FK stored 0/null) would be a finding; a loud config error is a
    // fail-loud limitation. ----
    [Table("XdgCycA")] public class CycA { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public int? CycBId { get; set; } public CycB? CycB { get; set; } public string Name { get; set; } = ""; }
    [Table("XdgCycB")] public class CycB { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public int CycAId { get; set; } public CycA? CycA { get; set; } public string Name { get; set; } = ""; }

    [Fact]
    public async Task LegalCycle_optionalFk_characterize()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgCycA (Id INTEGER PRIMARY KEY AUTOINCREMENT, CycBId INTEGER NULL, Name TEXT);" +
            "CREATE TABLE XdgCycB (Id INTEGER PRIMARY KEY AUTOINCREMENT, CycAId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var a = new CycA { Name = "a" };
        var b = new CycB { Name = "b", CycA = a };
        a.CycB = b;   // A optionally points at B; B required points at A
        ctx.Add(a);
        ctx.Add(b);

        Exception? thrown = null;
        try { await ctx.SaveChangesAsync(); } catch (Exception e) { thrown = e; }

        if (thrown != null)
        {
            // Fail-loud path — acceptable (record as limitation).
            Assert.IsType<NormConfigurationException>(thrown);
        }
        else
        {
            // If it succeeded, BOTH FKs must be correct — a stored 0/null would be silent-wrong.
            var bRow = Rows(keeper, "SELECT CycAId FROM XdgCycB").Single();
            Assert.Equal((long)a.Id, Convert.ToInt64(bRow[0]!));
            var aRow = Rows(keeper, "SELECT CycBId FROM XdgCycA").Single();
            Assert.Equal((long)b.Id, Convert.ToInt64(aRow[0]!));
        }
    }

    // ---- Mixed diamond: a node reached by a COLLECTION nav from one parent AND a
    // REFERENCE nav to another parent (both new, generated keys). ----
    [Table("XdgHub")] public class Hub { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public List<Spoke> Spokes { get; set; } = new(); public string Name { get; set; } = ""; }
    [Table("XdgSide")] public class Side { [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Name { get; set; } = ""; }
    [Table("XdgSpoke")] public class Spoke
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int HubId { get; set; }              // set by collection-direction fixup from Hub
        public int SideId { get; set; }             // set by reference-direction fixup from Side
        public Side? Side { get; set; }
        public string Name { get; set; } = "";
    }

    [Fact]
    public async Task MixedDiamond_collectionAndReference_bothFksPropagated()
    {
        var (keeper, make) = Setup(
            "CREATE TABLE XdgHub (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgSide (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);" +
            "CREATE TABLE XdgSpoke (Id INTEGER PRIMARY KEY AUTOINCREMENT, HubId INTEGER NOT NULL, SideId INTEGER NOT NULL, Name TEXT);");
        using var _ = keeper;
        await using var ctx = make();

        var hub = new Hub { Name = "hub" };
        var side = new Side { Name = "side" };
        var spoke = new Spoke { Name = "spoke", Side = side };
        hub.Spokes.Add(spoke);   // collection direction to hub
        ctx.Add(hub);
        ctx.Add(side);
        await ctx.SaveChangesAsync();

        Assert.True(hub.Id > 0 && side.Id > 0);
        var row = Rows(keeper, "SELECT HubId, SideId FROM XdgSpoke").Single();
        Assert.Equal((long)hub.Id, Convert.ToInt64(row[0]!));
        Assert.Equal((long)side.Id, Convert.ToInt64(row[1]!));
    }
}
