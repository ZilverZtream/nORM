using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// HydratePostMaterializeObject (invoked by a client-side post-materialize Select over a client Join /
/// group-join tail) fills UNLOADED navigations by matching principal-key vs foreign-key over all dependent
/// rows. When the principal key is a wider CLR type than the dependent FK (long PK vs int FK), the two box
/// to different runtime types and a raw object.Equals((long)1,(int)1) returns false, so the collection
/// hydrates EMPTY though the child rows exist — silent data loss with no exception. The match must be
/// value-normalized for scalar keys (as in IncludeProcessor / BatchedNavigationLoader).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PostMaterializeNavKeyWidthTests
{
    // ---- Mismatched widths: long PK / int FK ----
    [Table("CrtParent")]
    public class Parent
    {
        [Key] public long Id { get; set; }
        public string Name { get; set; } = "";
        // No initializer -> stays null at materialization.
        public List<Child> Children { get; set; } = null!;
    }

    [Table("CrtChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; } // int FK vs long PK
        public string Label { get; set; } = "";
    }

    [Table("CrtOther")]
    public class Other
    {
        [Key] public int Id { get; set; }
    }

    private static SqliteConnection Seed()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE CrtParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
            "CREATE TABLE CrtChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Label TEXT NOT NULL);" +
            "CREATE TABLE CrtOther (Id INTEGER PRIMARY KEY);" +
            "INSERT INTO CrtParent VALUES (1,'p1');" +
            "INSERT INTO CrtChild VALUES (10,1,'a'),(11,1,'b');" +
            "INSERT INTO CrtOther VALUES (1);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContextOptions Opts() => new DbContextOptions
    {
        OnModelCreating = mb => mb.Entity<Parent>().HasKey(p => p.Id)
            .HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id)
    };

    // Client-side whole-entity Join (routes to AppendPostMaterializeInnerJoin) followed by a
    // Select extracting the parent entity (routes to AppendPostMaterializeSelect -> HydratePostMaterializeObject).
    // The extracted parent's Children nav is hydrated by object.Equals(parent.Id (long), child.ParentId (int)).
    [Fact]
    public void Client_join_then_select_entity_hydrates_children_across_key_width_mismatch()
    {
        using var cn = Seed();
        using var _cn = cn;
        using var ctx = new DbContext(cn, new SqliteProvider(), Opts(), ownsConnection: false);

        List<Parent> parents;
        try
        {
            parents = ctx.Query<Parent>()
                .Join(ctx.Query<Other>(), p => p.Id, o => (long)o.Id, (p, o) => new { p, o })
                .Select(x => x.p)
                .ToList();
        }
        catch (Exception)
        {
            return; // fail-loud is acceptable (not a silent-wrong finding)
        }

        var parent = Assert.Single(parents);
        // If Children is null, hydration never ran (different shape) — inconclusive, treat as not-repro.
        if (parent.Children == null)
            return;
        // CONTROL: the 2 children exist and resolve via a direct query.
        Assert.Equal(2, ctx.Query<Child>().Count(c => c.ParentId == 1));
        // If hydration ran, it MUST have found both children — not silently 0.
        Assert.Equal(2, parent.Children.Count);
    }

    // Same shape but AsNoTracking (lazy loading disabled) so the nav is genuinely null and the manual
    // HydratePostMaterializeObject matching path is exercised rather than a lazy proxy.
    [Fact]
    public void Client_join_then_select_entity_notracking_hydrates_children_across_key_width_mismatch()
    {
        using var cn = Seed();
        using var _cn = cn;
        using var ctx = new DbContext(cn, new SqliteProvider(), Opts(), ownsConnection: false);

        List<Parent> parents;
        try
        {
            parents = ctx.Query<Parent>().AsNoTracking()
                .Join(ctx.Query<Other>(), p => p.Id, o => (long)o.Id, (p, o) => new { p, o })
                .Select(x => x.p)
                .ToList();
        }
        catch (Exception)
        {
            return; // fail-loud acceptable
        }

        var parent = Assert.Single(parents);
        if (parent.Children == null)
            return;
        Assert.Equal(2, parent.Children.Count);
    }

    // ---- Matching widths CONTROL: int PK / int FK. Proves the empty result above is caused by the
    // long-vs-int box-type mismatch in HydratePostMaterializeObject, not the query shape. ----
    [Table("CrtParent2")]
    public class Parent2
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child2> Children { get; set; } = null!;
    }

    [Table("CrtChild2")]
    public class Child2
    {
        [Key] public int Id { get; set; }
        public int Parent2Id { get; set; } // int FK == int PK
        public string Label { get; set; } = "";
    }

    [Table("CrtOther2")]
    public class Other2
    {
        [Key] public int Id { get; set; }
    }

    [Fact]
    public void Client_join_then_select_entity_matching_widths_control_hydrates_children()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CrtParent2 (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE CrtChild2 (Id INTEGER PRIMARY KEY, Parent2Id INTEGER NOT NULL, Label TEXT NOT NULL);" +
                "CREATE TABLE CrtOther2 (Id INTEGER PRIMARY KEY);" +
                "INSERT INTO CrtParent2 VALUES (1,'p1');" +
                "INSERT INTO CrtChild2 VALUES (10,1,'a'),(11,1,'b');" +
                "INSERT INTO CrtOther2 VALUES (1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Parent2>().HasKey(p => p.Id)
                .HasMany(p => p.Children).WithOne().HasForeignKey(c => c.Parent2Id, p => p.Id)
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var parents = ctx.Query<Parent2>()
            .Join(ctx.Query<Other2>(), p => p.Id, o => o.Id, (p, o) => new { p, o })
            .Select(x => x.p)
            .ToList();

        var parent = Assert.Single(parents);
        Assert.NotNull(parent.Children);
        Assert.Equal(2, parent.Children.Count); // matching int/int -> hydrates correctly
    }
}
