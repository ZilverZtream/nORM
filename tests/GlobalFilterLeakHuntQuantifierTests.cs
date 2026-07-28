using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Second adversarial leakage batch: quantifiers (All), nav scalar aggregates in a
/// predicate (Max), many-to-many aggregate with a filtered right entity, multiple
/// filtered entities joined, tenant nav-aggregate cross-poison in a predicate, a
/// self-referencing Include, and Concat (UNION ALL). Each seeds a hidden row and
/// asserts it never leaks / the visible row is never wrongly dropped.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GlobalFilterLeakHuntQuantifierTests
{
    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ── Parent / Kid model (All, tenant nav-aggregate) ────────────────────────
    [Table("QhPa")]
    private class QhPa
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public string TenantId { get; set; } = "";
        public List<QhKid> Kids { get; set; } = new();
    }

    [Table("QhKid")]
    private class QhKid
    {
        [Key] public int Id { get; set; }
        public int PaId { get; set; }
        public bool Active { get; set; }
        public bool IsDeleted { get; set; }
        public string TenantId { get; set; } = "";
    }

    private static DbContext SoftDeleteKidCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE QhPa  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);
                CREATE TABLE QhKid (Id INTEGER PRIMARY KEY, PaId INTEGER NOT NULL, Active INTEGER NOT NULL, IsDeleted INTEGER NOT NULL, TenantId TEXT NOT NULL);
                INSERT INTO QhPa VALUES (1,'all-visible-active','X'),(2,'has-visible-inactive','X'),(3,'no-kids','X');
                INSERT INTO QhKid VALUES
                    (10,1,1,0,'X'),   -- p1: visible active
                    (11,1,0,1,'X'),   -- p1: DELETED inactive (must be ignored by All)
                    (12,2,0,0,'X');   -- p2: visible inactive
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<QhPa>().HasKey(p => p.Id);
                mb.Entity<QhKid>().HasKey(k => k.Id);
                mb.Entity<QhPa>().HasMany(p => p.Kids).WithOne().HasForeignKey(k => k.PaId, p => p.Id);
            }
        };
        opts.AddGlobalFilter<QhKid>(k => !k.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Nav_All_ignores_softdeleted_children()
    {
        using var ctx = SoftDeleteKidCtx(out var cn);
        using var _cn = cn;

        // All(k => k.Active): p1's only VISIBLE kid is active (its inactive kid is soft-deleted
        // and must be ignored) -> p1 matches. p3 has no kids -> vacuously true. p2 has a visible
        // inactive kid -> excluded. A leak of the deleted inactive kid would wrongly drop p1.
        var names = (await ctx.Query<QhPa>().Where(p => p.Kids.All(k => k.Active))
            .Select(p => p.Name).ToListAsync()).OrderBy(n => n).ToList();

        Assert.Equal(new[] { "all-visible-active", "no-kids" }, names);
    }

    // ── Owner / Item model (Max in a predicate) ───────────────────────────────
    [Table("QhOwner")]
    private class QhOwner
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<QhItem> Items { get; set; } = new();
    }

    [Table("QhItem")]
    private class QhItem
    {
        [Key] public int Id { get; set; }
        public int OwnerId { get; set; }
        public int Val { get; set; }
        public bool IsDeleted { get; set; }
    }

    private static DbContext OwnerItemCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE QhOwner (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE QhItem  (Id INTEGER PRIMARY KEY, OwnerId INTEGER NOT NULL, Val INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO QhOwner VALUES (1,'o');
                INSERT INTO QhItem  VALUES (10,1,5,0),(11,1,999,1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<QhOwner>().HasKey(o => o.Id);
                mb.Entity<QhItem>().HasKey(i => i.Id);
                mb.Entity<QhOwner>().HasMany(o => o.Items).WithOne().HasForeignKey(i => i.OwnerId, o => o.Id);
            }
        };
        opts.AddGlobalFilter<QhItem>(i => !i.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Nav_Max_in_predicate_excludes_softdeleted()
    {
        using var ctx = OwnerItemCtx(out var cn);
        using var _cn = cn;

        // The deleted item's Val=999 must NOT enter Max; the visible max is 5, so no owner
        // has Max(Val) > 100. A leak would return owner 'o'.
        var matched = await ctx.Query<QhOwner>()
            .Where(o => o.Items.Max(i => i.Val) > 100)
            .Select(o => o.Name).ToListAsync();
        Assert.Empty(matched);

        // Sanity: with the real visible max (5), threshold 4 matches.
        var matchedLow = await ctx.Query<QhOwner>()
            .Where(o => o.Items.Max(i => i.Val) > 4)
            .Select(o => o.Name).ToListAsync();
        Assert.Equal(new[] { "o" }, matchedLow);
    }

    // ── Many-to-many with a filtered right entity ─────────────────────────────
    [Table("QhBlog")]
    private class QhBlog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<QhTag> Tags { get; set; } = new();
    }

    [Table("QhTag")]
    private class QhTag
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
        public bool IsDeleted { get; set; }
    }

    private static DbContext M2MCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE QhBlog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE QhTag  (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, IsDeleted INTEGER NOT NULL);
                CREATE TABLE QhBlogTag (BlogId INTEGER NOT NULL, TagId INTEGER NOT NULL, PRIMARY KEY (BlogId, TagId));
                INSERT INTO QhBlog VALUES (1,'only-deleted-tag'),(2,'has-live-tag');
                INSERT INTO QhTag  VALUES (100,'live',0),(101,'gone',1);
                INSERT INTO QhBlogTag VALUES (1,101),(2,100),(2,101);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<QhBlog>().HasKey(b => b.Id);
                mb.Entity<QhTag>().HasKey(t => t.Id);
                mb.Entity<QhBlog>().HasMany<QhTag>(b => b.Tags).WithMany().UsingTable("QhBlogTag", "BlogId", "TagId");
            }
        };
        opts.AddGlobalFilter<QhTag>(t => !t.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task M2M_Any_in_predicate_excludes_softdeleted_right()
    {
        using var ctx = M2MCtx(out var cn);
        using var _cn = cn;

        // Blog 1's only tag (101) is soft-deleted -> it has NO visible tags -> excluded.
        // Blog 2 has a live tag (100) -> included. A leak would return blog 1.
        var names = (await ctx.Query<QhBlog>().Where(b => b.Tags.Any())
            .Select(b => b.Name).ToListAsync()).OrderBy(n => n).ToList();
        Assert.Equal(new[] { "has-live-tag" }, names);
    }

    [Fact]
    public async Task M2M_Include_excludes_softdeleted_right()
    {
        using var ctx = M2MCtx(out var cn);
        using var _cn = cn;

        var blogs = (await ((INormQueryable<QhBlog>)ctx.Query<QhBlog>())
            .Include(b => b.Tags).ToListAsync()).OrderBy(b => b.Id).ToList();

        var b1 = blogs.Single(b => b.Id == 1);
        var b2 = blogs.Single(b => b.Id == 2);
        Assert.Empty(b1.Tags);                                  // only tag is soft-deleted
        Assert.Equal(new[] { 100 }, b2.Tags.Select(t => t.Id).OrderBy(i => i).ToArray());
        Assert.DoesNotContain(b1.Tags, t => t.IsDeleted);
        Assert.DoesNotContain(b2.Tags, t => t.IsDeleted);
    }

    // ── Multiple filtered entities in one INNER join ──────────────────────────
    [Table("QhOrder")]
    private class QhOrder
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public string Ref { get; set; } = "";
        public bool Cancelled { get; set; }
    }

    [Table("QhCust")]
    private class QhCust
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool IsDeleted { get; set; }
    }

    [Fact]
    public async Task Join_of_two_filtered_entities_applies_both_filters()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE QhOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Ref TEXT NOT NULL, Cancelled INTEGER NOT NULL);
                CREATE TABLE QhCust  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO QhCust  VALUES (1,'Live',0),(2,'DeadCust',1);
                INSERT INTO QhOrder VALUES
                    (10,1,'live-live',0),        -- visible order, visible cust  -> KEEP
                    (11,1,'cancelled-live',1),   -- cancelled order              -> DROP (order filter)
                    (12,2,'live-deadcust',0);    -- visible order, deleted cust  -> DROP (cust filter)
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<QhOrder>(); mb.Entity<QhCust>(); }
        };
        opts.AddGlobalFilter<QhOrder>(o => !o.Cancelled);
        opts.AddGlobalFilter<QhCust>(c => !c.IsDeleted);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var rows = await ctx.Query<QhOrder>()
            .Join(ctx.Query<QhCust>(), o => o.CustomerId, c => c.Id, (o, c) => new { o.Ref, c.Name })
            .ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal("live-live", row.Ref);
        Assert.Equal("Live", row.Name);
        Assert.DoesNotContain(rows, r => r.Name == "DeadCust");
    }

    // ── Tenant nav-aggregate cross-poison inside a predicate ──────────────────
    [Fact]
    public async Task Tenant_nav_Any_in_predicate_excludes_other_tenant_child()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE QhPa  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);
                CREATE TABLE QhKid (Id INTEGER PRIMARY KEY, PaId INTEGER NOT NULL, Active INTEGER NOT NULL, IsDeleted INTEGER NOT NULL, TenantId TEXT NOT NULL);
                -- Parent 1 belongs to tenant T1 and has NO T1 kids, only a cross-tenant poison kid.
                INSERT INTO QhPa  VALUES (1,'t1-owner','T1');
                INSERT INTO QhKid VALUES (11,1,1,0,'T2');  -- poison: T2 kid pointing at T1 parent
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider("T1"),
            TenantColumnName = "TenantId",
            OnModelCreating = mb =>
            {
                mb.Entity<QhPa>().HasKey(p => p.Id);
                mb.Entity<QhKid>().HasKey(k => k.Id);
                mb.Entity<QhPa>().HasMany(p => p.Kids).WithOne().HasForeignKey(k => k.PaId, p => p.Id);
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Parent 1's only kid belongs to tenant T2; under T1 it must read as childless.
        var withKids = await ctx.Query<QhPa>().Where(p => p.Kids.Any())
            .Select(p => p.Name).ToListAsync();
        Assert.Empty(withKids);

        var counts = await ctx.Query<QhPa>()
            .Select(p => new { p.Name, N = p.Kids.Count() }).ToListAsync();
        Assert.Equal(0, Assert.Single(counts).N);
    }

    // ── Self-referencing Include with soft-delete ─────────────────────────────
    [Table("QhCat")]
    private class QhCat
    {
        [Key] public int Id { get; set; }
        public int? ParentId { get; set; }
        public string Name { get; set; } = "";
        public bool IsDeleted { get; set; }
        public List<QhCat> Children { get; set; } = new();
    }

    [Fact]
    public async Task SelfRef_Include_excludes_softdeleted_children()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE QhCat (Id INTEGER PRIMARY KEY, ParentId INTEGER NULL, Name TEXT NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO QhCat VALUES (1,NULL,'root',0),(2,1,'live-child',0),(3,1,'deleted-child',1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<QhCat>().HasKey(c => c.Id);
                mb.Entity<QhCat>().HasMany(c => c.Children).WithOne().HasForeignKey(c => c.ParentId!, c => c.Id);
            }
        };
        opts.AddGlobalFilter<QhCat>(c => !c.IsDeleted);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var root = (await ((INormQueryable<QhCat>)ctx.Query<QhCat>())
            .Where(c => c.ParentId == null)
            .Include(c => c.Children).ToListAsync()).Single();

        Assert.Equal(new[] { 2 }, root.Children.Select(c => c.Id).OrderBy(i => i).ToArray());
        Assert.DoesNotContain(root.Children, c => c.IsDeleted);
    }

    // ── Concat (UNION ALL) applies the filter to BOTH arms ────────────────────
    [Fact]
    public async Task Concat_applies_filter_to_both_arms()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE QhItem (Id INTEGER PRIMARY KEY, OwnerId INTEGER NOT NULL, Val INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO QhItem VALUES (1,1,10,0),(2,1,20,1),(3,2,30,0),(4,2,40,1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<QhItem>().HasKey(i => i.Id)
        };
        opts.AddGlobalFilter<QhItem>(i => !i.IsDeleted);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        // Arm A: owner 1; Arm B: owner 2. Deleted rows (2,4) must never appear in either arm.
        var items = await ctx.Query<QhItem>().Where(i => i.OwnerId == 1)
            .Concat(ctx.Query<QhItem>().Where(i => i.OwnerId == 2))
            .ToListAsync();
        var ids = items.Select(i => i.Id).OrderBy(i => i).ToList();

        Assert.Equal(new[] { 1, 3 }, ids);
    }
}
