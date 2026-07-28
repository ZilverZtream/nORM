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
/// Adversarial leakage hunt for global query filters / soft-delete / multi-tenancy across
/// query shapes NOT already pinned by the existing suite: query-syntax LEFT JOIN inner,
/// method-syntax GroupJoin group aggregate, nested ThenInclude grandchild, reference-nav
/// Include, correlated subquery in a projection, root GroupBy Sum, and negated nav-Any.
/// Each test seeds a row that MUST be hidden (soft-deleted) alongside a visible row and
/// asserts the hidden row never leaks and the visible row is never wrongly dropped.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GlobalFilterLeakHuntAdversarialTests
{
    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Shape 1 + 2: JOIN with a soft-deleted inner customer.
    // ─────────────────────────────────────────────────────────────────────────
    [Table("LhOrder")]
    private class LhOrder
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public string Ref { get; set; } = "";
    }

    [Table("LhCustomer")]
    private class LhCustomer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public bool IsDeleted { get; set; }
    }

    private static DbContext JoinCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE LhOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Ref TEXT NOT NULL);
                CREATE TABLE LhCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO LhCustomer VALUES (1,'Live',0),(2,'Deleted',1);
                INSERT INTO LhOrder VALUES (10,1,'o-live'),(11,2,'o-orphan-deleted');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<LhOrder>(); mb.Entity<LhCustomer>(); }
        };
        opts.AddGlobalFilter<LhCustomer>(c => !c.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task QuerySyntax_LeftJoin_softdeleted_inner_keeps_outer_and_hides_customer()
    {
        using var ctx = JoinCtx(out var cn);
        using var _cn = cn;

        var rows = (await ctx.Query<LhOrder>()
            .GroupJoin(ctx.Query<LhCustomer>(), o => o.CustomerId, c => c.Id, (o, cs) => new { o, cs })
            .SelectMany(x => x.cs.DefaultIfEmpty(), (x, c) => new { x.o.Ref, CustName = c != null ? c.Name : null })
            .ToListAsync())
            .OrderBy(r => r.Ref).ToList();

        // Both orders must survive the LEFT JOIN. o-live keeps 'Live'; the order whose
        // customer is soft-deleted must read as an ORPHAN (null customer), never leak 'Deleted'.
        Assert.Equal(2, rows.Count);
        Assert.DoesNotContain(rows, r => r.CustName == "Deleted");
        var live = rows.Single(r => r.Ref == "o-live");
        Assert.Equal("Live", live.CustName);
        var orphan = rows.Single(r => r.Ref == "o-orphan-deleted");
        Assert.Null(orphan.CustName);
    }

    [Fact]
    public async Task MethodSyntax_GroupJoin_group_count_excludes_softdeleted_inner()
    {
        using var ctx = JoinCtx(out var cn);
        using var _cn = cn;

        // GroupJoin each order to its (0..1) customers; the deleted customer must not be
        // counted, so order 11's matched-customer count is 0, not 1.
        var rows = (await ctx.Query<LhOrder>()
            .GroupJoin(ctx.Query<LhCustomer>(), o => o.CustomerId, c => c.Id, (o, cs) => new { o.Ref, N = cs.Count() })
            .ToListAsync())
            .OrderBy(r => r.Ref).ToList();

        Assert.Equal(1, rows.Single(r => r.Ref == "o-live").N);
        Assert.Equal(0, rows.Single(r => r.Ref == "o-orphan-deleted").N);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Shape 3: nested Include -> ThenInclude with a soft-deleted GRANDCHILD.
    // ─────────────────────────────────────────────────────────────────────────
    [Table("LhP")]
    private class LhP
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<LhC> Children { get; set; } = new();
    }

    [Table("LhC")]
    private class LhC
    {
        [Key] public int Id { get; set; }
        public int PId { get; set; }
        public string Label { get; set; } = "";
        public List<LhGc> GrandChildren { get; set; } = new();
    }

    [Table("LhGc")]
    private class LhGc
    {
        [Key] public int Id { get; set; }
        public int CId { get; set; }
        public bool IsDeleted { get; set; }
        public string Label { get; set; } = "";
    }

    private static DbContext NestedIncludeCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE LhP  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE LhC  (Id INTEGER PRIMARY KEY, PId INTEGER NOT NULL, Label TEXT NOT NULL);
                CREATE TABLE LhGc (Id INTEGER PRIMARY KEY, CId INTEGER NOT NULL, IsDeleted INTEGER NOT NULL, Label TEXT NOT NULL);
                INSERT INTO LhP  VALUES (1,'p');
                INSERT INTO LhC  VALUES (10,1,'c');
                INSERT INTO LhGc VALUES (100,10,0,'gc-live'),(101,10,1,'gc-deleted');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<LhP>().HasKey(p => p.Id);
                mb.Entity<LhC>().HasKey(c => c.Id);
                mb.Entity<LhGc>().HasKey(g => g.Id);
                mb.Entity<LhP>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.PId, p => p.Id);
                mb.Entity<LhC>().HasMany(c => c.GrandChildren).WithOne().HasForeignKey(g => g.CId, c => c.Id);
            }
        };
        opts.AddGlobalFilter<LhGc>(g => !g.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task ThenInclude_grandchild_softdelete_excluded()
    {
        using var ctx = NestedIncludeCtx(out var cn);
        using var _cn = cn;

        var p = (await ((INormQueryable<LhP>)ctx.Query<LhP>())
            .Include(x => x.Children).ThenInclude(c => c.GrandChildren)
            .ToListAsync()).Single();

        var child = Assert.Single(p.Children);
        Assert.Equal(new[] { 100 }, child.GrandChildren.Select(g => g.Id).OrderBy(i => i).ToArray());
        Assert.DoesNotContain(child.GrandChildren, g => g.IsDeleted);
    }

    [Fact]
    public async Task ThenInclude_grandchild_softdelete_excluded_splitquery()
    {
        using var ctx = NestedIncludeCtx(out var cn);
        using var _cn = cn;

        var p = (await ((INormQueryable<LhP>)ctx.Query<LhP>())
            .Include(x => x.Children).ThenInclude(c => c.GrandChildren)
            .AsSplitQuery()
            .ToListAsync()).Single();

        var child = Assert.Single(p.Children);
        Assert.Equal(new[] { 100 }, child.GrandChildren.Select(g => g.Id).OrderBy(i => i).ToArray());
        Assert.DoesNotContain(child.GrandChildren, g => g.IsDeleted);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Shape 5: explicit correlated subquery in a PROJECTION (not a nav member).
    // ─────────────────────────────────────────────────────────────────────────
    [Table("LhOwner")]
    private class LhOwner
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("LhItem")]
    private class LhItem
    {
        [Key] public int Id { get; set; }
        public int OwnerId { get; set; }
        public bool IsDeleted { get; set; }
        public int Value { get; set; }
    }

    private static DbContext OwnerItemCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE LhOwner (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE LhItem  (Id INTEGER PRIMARY KEY, OwnerId INTEGER NOT NULL, IsDeleted INTEGER NOT NULL, Value INTEGER NOT NULL);
                INSERT INTO LhOwner VALUES (1,'o');
                INSERT INTO LhItem  VALUES (10,1,0,100),(11,1,1,999);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<LhOwner>().HasKey(o => o.Id); mb.Entity<LhItem>().HasKey(i => i.Id); }
        };
        opts.AddGlobalFilter<LhItem>(i => !i.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task CorrelatedSubquery_in_projection_Count_excludes_softdeleted()
    {
        using var ctx = OwnerItemCtx(out var cn);
        using var _cn = cn;

        var rows = await ctx.Query<LhOwner>()
            .Select(o => new { o.Name, N = ctx.Query<LhItem>().Count(i => i.OwnerId == o.Id) })
            .ToListAsync();

        var row = Assert.Single(rows);
        // Item 11 is soft-deleted; the correlated Count must exclude it → N == 1, never 2.
        Assert.Equal(1, row.N);
    }

    [Fact]
    public async Task CorrelatedSubquery_in_projection_Sum_excludes_softdeleted()
    {
        using var ctx = OwnerItemCtx(out var cn);
        using var _cn = cn;

        var rows = await ctx.Query<LhOwner>()
            .Select(o => new { o.Name, Total = ctx.Query<LhItem>().Where(i => i.OwnerId == o.Id).Sum(i => i.Value) })
            .ToListAsync();

        var row = Assert.Single(rows);
        // Only the live item's 100 must be summed; the deleted item's 999 must NOT leak.
        Assert.Equal(100, row.Total);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Shape 6: root GroupBy producing a Sum over a soft-delete-filtered set.
    // ─────────────────────────────────────────────────────────────────────────
    [Fact]
    public async Task Root_GroupBy_Sum_excludes_softdeleted_rows()
    {
        using var ctx = OwnerItemCtx(out var cn);
        using var _cn = cn;

        var rows = await ctx.Query<LhItem>()
            .GroupBy(i => i.OwnerId)
            .Select(g => new { OwnerId = g.Key, Total = g.Sum(x => x.Value), N = g.Count() })
            .ToListAsync();

        var row = Assert.Single(rows);
        // Soft-deleted item 11 (999) must not enter the group's Sum or Count.
        Assert.Equal(100, row.Total);
        Assert.Equal(1, row.N);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Shape 7: negated nav-Any — a parent whose ONLY child is soft-deleted must
    // read as having no children.
    // ─────────────────────────────────────────────────────────────────────────
    [Table("LhPa")]
    private class LhPa
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<LhKid> Kids { get; set; } = new();
    }

    [Table("LhKid")]
    private class LhKid
    {
        [Key] public int Id { get; set; }
        public int PaId { get; set; }
        public bool IsDeleted { get; set; }
    }

    private static DbContext PaKidCtx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE LhPa  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE LhKid (Id INTEGER PRIMARY KEY, PaId INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO LhPa  VALUES (1,'has-live-kid'),(2,'only-deleted-kid'),(3,'no-kids');
                INSERT INTO LhKid VALUES (10,1,0),(11,1,1),(12,2,1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<LhPa>().HasKey(p => p.Id);
                mb.Entity<LhKid>().HasKey(k => k.Id);
                mb.Entity<LhPa>().HasMany(p => p.Kids).WithOne().HasForeignKey(k => k.PaId, p => p.Id);
            }
        };
        opts.AddGlobalFilter<LhKid>(k => !k.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Nav_Any_treats_softdeleted_only_parent_as_childless()
    {
        using var ctx = PaKidCtx(out var cn);
        using var _cn = cn;

        // Parents WITH at least one visible kid: only parent 1 (its kid 11 is deleted, kid 10 lives).
        var withKids = (await ctx.Query<LhPa>().Where(p => p.Kids.Any()).Select(p => p.Name).ToListAsync())
            .OrderBy(n => n).ToList();
        Assert.Equal(new[] { "has-live-kid" }, withKids);

        // Parents with NO visible kids: parent 2 (only deleted kid) and parent 3 (no kids).
        var childless = (await ctx.Query<LhPa>().Where(p => !p.Kids.Any()).Select(p => p.Name).ToListAsync())
            .OrderBy(n => n).ToList();
        Assert.Equal(new[] { "no-kids", "only-deleted-kid" }, childless);
    }
}
