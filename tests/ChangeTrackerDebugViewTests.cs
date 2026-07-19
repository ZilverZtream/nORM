using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// <see cref="ChangeTracker.DebugView"/> (EF Core parity) renders the tracked entities: ShortView is one line
/// per entity (type, key, state); LongView adds each property's value, marks the key with PK, and shows the
/// original value of a modified property.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ChangeTrackerDebugViewTests
{
    [Table("DbgGadget")]
    private class Gadget { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE DbgGadget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                INSERT INTO DbgGadget VALUES (1,'orig1'),(2,'orig2');
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new nORM.Providers.SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Gadget>().HasKey(g => g.Id)
        }, ownsConnection: false);
    }

    [Fact]
    public void ShortView_lists_each_tracked_entity_with_type_key_and_state()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var gadgets = ctx.Query<Gadget>().ToList();          // both tracked, Unchanged
        gadgets.Single(g => g.Id == 1).Name = "changed";     // -> Modified
        ctx.Add(new Gadget { Id = 99, Name = "new" });        // -> Added
        ctx.ChangeTracker.DetectChanges();

        var view = ctx.ChangeTracker.DebugView.ShortView;
        var lines = view.Split('\n');
        Assert.Equal(3, lines.Length);
        Assert.Contains("Gadget {Id: 1} Modified", view, StringComparison.Ordinal);
        Assert.Contains("Gadget {Id: 2} Unchanged", view, StringComparison.Ordinal);
        Assert.Contains("Gadget {Id: 99} Added", view, StringComparison.Ordinal);
    }

    [Fact]
    public void LongView_shows_property_values_pk_marker_and_original_of_modified()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var g1 = ctx.Query<Gadget>().Single(g => g.Id == 1);
        g1.Name = "changed";
        ctx.ChangeTracker.DetectChanges();

        var view = ctx.ChangeTracker.DebugView.LongView;
        Assert.Contains("Id: 1 PK", view, StringComparison.Ordinal);
        Assert.Contains("Name: 'changed' Modified Originally 'orig1'", view, StringComparison.Ordinal);
    }

    [Fact]
    public void DebugView_of_an_empty_tracker_is_empty()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        Assert.Equal(string.Empty, ctx.ChangeTracker.DebugView.ShortView);
        Assert.Equal(string.Empty, ctx.ChangeTracker.DebugView.LongView);
    }
}
