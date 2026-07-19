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
/// The stateful <see cref="ChangeTracker.TrackGraph{TState}(object, TState, Func{EntityEntryGraphNode{TState}, bool})"/>
/// (EF Core parity): the state value is threaded to every node, the callback's bool return controls descent
/// (returning false prunes the subtree), and the callback assigns each entity's tracked state.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ChangeTrackerTrackGraphStateTests
{
    [Table("TgsParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("TgsChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext Ctx(SqliteConnection cn) => new(cn, new SqliteProvider(), new DbContextOptions
    {
        OnModelCreating = mb =>
        {
            mb.Entity<Parent>().HasKey(p => p.Id);
            mb.Entity<Child>().HasKey(c => c.Id);
            mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
        }
    }, ownsConnection: false);

    private static (SqliteConnection, DbContext) New()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        return (cn, Ctx(cn));
    }

    private static Parent Graph() => new()
    {
        Id = 1, Name = "p",
        Children = { new Child { Id = 10, ParentId = 1 }, new Child { Id = 11, ParentId = 1 } }
    };

    [Fact]
    public void State_is_threaded_to_every_node_and_entities_are_tracked()
    {
        var (cn, ctx) = New(); using var _cn = cn; using var _ctx = ctx;
        var parent = Graph();
        var seen = new List<string>();

        ctx.ChangeTracker.TrackGraph(parent, "MARK", node =>
        {
            seen.Add(node.NodeState);
            node.Entry.State = EntityState.Modified;
            return true;   // descend into children
        });

        Assert.Equal(3, seen.Count);                 // parent + 2 children visited
        Assert.All(seen, s => Assert.Equal("MARK", s));
        Assert.Equal(3, ctx.ChangeTracker.Entries.Count());
        Assert.Equal(EntityState.Modified, ctx.Entry(parent).State);
        Assert.Equal(EntityState.Modified, ctx.Entry(parent.Children[0]).State);
    }

    [Fact]
    public void Returning_false_prunes_the_subtree()
    {
        var (cn, ctx) = New(); using var _cn = cn; using var _ctx = ctx;
        var parent = Graph();

        ctx.ChangeTracker.TrackGraph(parent, 0, node =>
        {
            node.Entry.State = EntityState.Modified;
            return false;   // do not descend
        });

        Assert.Single(ctx.ChangeTracker.Entries);    // only the root tracked; children pruned
        Assert.Equal(EntityState.Modified, ctx.Entry(parent).State);
        Assert.Throws<InvalidOperationException>(() => ctx.Entry(parent.Children[0]));
    }

    [Fact]
    public void Node_state_can_drive_the_tracked_state_per_node()
    {
        var (cn, ctx) = New(); using var _cn = cn; using var _ctx = ctx;
        var parent = Graph();

        // Thread a state object the callback consults to decide how to track.
        ctx.ChangeTracker.TrackGraph(parent, EntityState.Added, node =>
        {
            node.Entry.State = node.NodeState;   // track everything as the threaded state
            return true;
        });

        Assert.Equal(EntityState.Added, ctx.Entry(parent).State);
        Assert.Equal(EntityState.Added, ctx.Entry(parent.Children[1]).State);
    }

    [Fact]
    public void Null_arguments_throw()
    {
        var (cn, ctx) = New(); using var _cn = cn; using var _ctx = ctx;
        Assert.Throws<ArgumentNullException>(() => ctx.ChangeTracker.TrackGraph<int>(null!, 0, _ => true));
        Assert.Throws<ArgumentNullException>(() => ctx.ChangeTracker.TrackGraph<int>(Graph(), 0, null!));
    }
}
