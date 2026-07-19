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
/// <see cref="ChangeTracker.TrackGraph(object, Action{EntityEntryGraphNode})"/> (EF Core parity): the callback
/// assigns each discovered entity's state, the traversal descends only through entities the callback tracks,
/// already-tracked entities are skipped, and a cyclic graph terminates.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ChangeTrackerTrackGraphTests
{
    [Table("TgParent")]
    public class Parent
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("TgChild")]
    public class Child
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Name { get; set; } = "";
        public Parent? Parent { get; set; }
    }

    [Table("TgNode")]
    public class Node
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Name { get; set; } = "";
        public List<Node> Children { get; set; } = new();
        public Node? Parent { get; set; }
    }

    private static void CreateSchema(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE TgParent (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);
            CREATE TABLE TgChild (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL, Name TEXT NOT NULL);
            CREATE TABLE TgNode (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NULL, Name TEXT NOT NULL);
            """;
        cmd.ExecuteNonQuery();
    }

    private static DbContext NewContext(SqliteConnection cn)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne(c => c.Parent).HasForeignKey(c => c.ParentId, p => p.Id);
                mb.Entity<Node>().HasKey(n => n.Id);
                mb.Entity<Node>().HasMany(n => n.Children).WithOne(n => n.Parent).HasForeignKey(n => n.ParentId, n => n.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static DbContext Boot(SqliteConnection cn)
    {
        CreateSchema(cn);
        return NewContext(cn);
    }

    private static Action<EntityEntryGraphNode> AddOrUpdate =>
        node => { node.Entry.State = node.Entry.IsKeySet ? EntityState.Modified : EntityState.Added; };

    [Fact]
    public async Task inserts_a_disconnected_graph_as_added()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; await using var ctx = Boot(cn);
        var parent = new Parent { Name = "P", Children = { new Child { Name = "A" }, new Child { Name = "B" } } };

        ctx.ChangeTracker.TrackGraph(parent, AddOrUpdate);
        await ctx.SaveChangesAsync();

        Assert.True(parent.Id > 0);
        var children = await ctx.Query<Child>().ToListAsync();
        Assert.Equal(2, children.Count);
        Assert.All(children, c => Assert.Equal(parent.Id, c.ParentId));   // FK fixup wired the generated key
    }

    [Fact]
    public async Task mixed_add_and_update_by_key_set()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn;
        CreateSchema(cn);
        int pid, cid;
        await using (var seed = NewContext(cn))
        {
            var p = new Parent { Name = "orig", Children = { new Child { Name = "c-orig" } } };
            seed.Add(p);
            await seed.SaveChangesAsync();
            pid = p.Id; cid = p.Children[0].Id;
        }

        await using (var ctx = NewContext(cn))
        {
            // Disconnected graph: existing parent + existing child (edited) + a brand-new child.
            var graph = new Parent
            {
                Id = pid,
                Name = "changed",
                Children =
                {
                    new Child { Id = cid, ParentId = pid, Name = "c-changed" },
                    new Child { Name = "c-new" }
                }
            };
            ctx.ChangeTracker.TrackGraph(graph, AddOrUpdate);
            await ctx.SaveChangesAsync();
        }

        await using (var verify = NewContext(cn))
        {
            Assert.Equal("changed", (await verify.Query<Parent>().ToListAsync()).Single(p => p.Id == pid).Name);
            var children = (await verify.Query<Child>().ToListAsync()).OrderBy(c => c.Id).ToList();
            Assert.Equal(2, children.Count);
            Assert.Equal("c-changed", children.Single(c => c.Id == cid).Name);   // existing row updated
            Assert.Contains(children, c => c.Name == "c-new" && c.Id != cid);    // new row inserted
        }
    }

    [Fact]
    public void a_node_left_detached_is_not_tracked_and_its_subtree_is_not_visited()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);
        var root = new Node { Name = "root" };
        var mid = new Node { Name = "mid" };
        var leaf = new Node { Name = "leaf" };
        root.Children.Add(mid); mid.Children.Add(leaf);

        var visited = new List<string>();
        ctx.ChangeTracker.TrackGraph(root, node =>
        {
            var n = (Node)node.Entry.Entity!;
            visited.Add(n.Name);
            if (n.Name == "root")
                node.Entry.State = EntityState.Added;   // mid is left Detached → traversal stops before leaf
        });

        Assert.Equal(new[] { "mid", "root" }, visited.OrderBy(x => x).ToArray());   // leaf never visited
        Assert.Equal(EntityState.Added, ctx.Entry(root).State);
        Assert.Throws<InvalidOperationException>(() => ctx.Entry(mid));    // Detached node not tracked
        Assert.Throws<InvalidOperationException>(() => ctx.Entry(leaf));   // subtree not tracked
    }

    [Fact]
    public void already_tracked_entities_are_skipped_by_the_callback()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);
        var sharedChild = new Child { Id = 7, ParentId = 1, Name = "already" };
        ctx.Attach(sharedChild);   // pre-tracked as Unchanged

        var parent = new Parent { Id = 1, Name = "P", Children = { sharedChild, new Child { Name = "fresh" } } };
        var visited = new List<object>();
        ctx.ChangeTracker.TrackGraph(parent, node =>
        {
            visited.Add(node.Entry.Entity!);
            node.Entry.State = node.Entry.IsKeySet ? EntityState.Modified : EntityState.Added;
        });

        Assert.Contains(parent, visited);
        Assert.DoesNotContain(sharedChild, visited);                 // already tracked → no callback
        Assert.Contains(visited, o => o is Child { Name: "fresh" }); // the untracked sibling still visited
        Assert.Equal(EntityState.Unchanged, ctx.Entry(sharedChild).State);  // its state untouched
    }

    [Fact]
    public void cyclic_graph_terminates_and_visits_each_entity_once()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);
        var parent = new Parent { Name = "P" };
        var child = new Child { Name = "A", Parent = parent };   // back-reference closes the cycle
        parent.Children.Add(child);

        var count = 0;
        ctx.ChangeTracker.TrackGraph(parent, node => { count++; node.Entry.State = EntityState.Added; });

        Assert.Equal(2, count);   // parent + child, each once — no infinite loop
    }

    [Fact]
    public void node_exposes_source_entry_and_inbound_navigation()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn; using var ctx = Boot(cn);
        var parent = new Parent { Name = "P", Children = { new Child { Name = "A" } } };

        EntityEntryGraphNode? childNode = null;
        ctx.ChangeTracker.TrackGraph(parent, node =>
        {
            node.Entry.State = EntityState.Added;
            if (node.Entry.Entity is Child)
                childNode = node;
        });

        Assert.NotNull(childNode);
        Assert.Same(parent, childNode!.SourceEntry!.Entity);
        Assert.Equal(nameof(Parent.Children), childNode.InboundNavigation);
    }
}
