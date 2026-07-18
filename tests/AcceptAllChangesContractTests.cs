using System;
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
/// Pins ChangeTracker.AcceptAllChanges() (EF-parity): after running change detection, Added and Modified
/// entries become Unchanged with their current values as the new baseline, and Deleted entries are
/// detached. Afterwards HasChanges() is false. It captures even an as-yet-undetected edit as the baseline,
/// since it detects first.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class AcceptAllChangesContractTests
{
    [Table("AacWidget")]
    public class Widget
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE AacWidget (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                INSERT INTO AacWidget VALUES (1, 'a'), (2, 'b');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Widget>().HasKey(w => w.Id) };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void Modified_becomes_unchanged_with_current_values_as_baseline()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var w = ctx.Query<Widget>().First(x => x.Id == 1);
        w.Name = "changed";
        ctx.ChangeTracker.DetectChanges();
        Assert.Equal(EntityState.Modified, ctx.Entry(w).State);

        ctx.ChangeTracker.AcceptAllChanges();

        Assert.Equal(EntityState.Unchanged, ctx.Entry(w).State);
        Assert.False(ctx.ChangeTracker.HasChanges());
        Assert.Equal("changed", ctx.Entry(w).OriginalValues["Name"]);   // current value is the new baseline
    }

    [Fact]
    public void Added_becomes_unchanged()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var w = new Widget { Id = 99, Name = "new" };
        ctx.Add(w);
        Assert.Equal(EntityState.Added, ctx.Entry(w).State);

        ctx.ChangeTracker.AcceptAllChanges();

        Assert.Equal(EntityState.Unchanged, ctx.Entry(w).State);
        Assert.False(ctx.ChangeTracker.HasChanges());
    }

    [Fact]
    public void Deleted_is_detached()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var w = ctx.Query<Widget>().First(x => x.Id == 1);
        ctx.Remove(w);
        Assert.Equal(EntityState.Deleted, ctx.Entry(w).State);

        ctx.ChangeTracker.AcceptAllChanges();

        Assert.DoesNotContain(ctx.ChangeTracker.Entries, e => ReferenceEquals(e.Entity, w));
        Assert.False(ctx.ChangeTracker.HasChanges());
    }

    [Fact]
    public void Accepts_an_undetected_edit_as_the_baseline()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var w = ctx.Query<Widget>().First(x => x.Id == 2);
        w.Name = "edited";   // no explicit DetectChanges — AcceptAllChanges must detect first

        ctx.ChangeTracker.AcceptAllChanges();

        Assert.Equal(EntityState.Unchanged, ctx.Entry(w).State);
        Assert.False(ctx.ChangeTracker.HasChanges());
        Assert.Equal("edited", ctx.Entry(w).OriginalValues["Name"]);
    }

    [Fact]
    public void Mixed_states_are_each_accepted_by_their_own_state()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var modified = ctx.Query<Widget>().First(x => x.Id == 1);
        modified.Name = "mod";
        var deleted = ctx.Query<Widget>().First(x => x.Id == 2);
        ctx.Remove(deleted);
        var added = new Widget { Id = 50, Name = "add" };
        ctx.Add(added);

        ctx.ChangeTracker.AcceptAllChanges();

        Assert.Equal(EntityState.Unchanged, ctx.Entry(modified).State);
        Assert.Equal(EntityState.Unchanged, ctx.Entry(added).State);
        Assert.DoesNotContain(ctx.ChangeTracker.Entries, e => ReferenceEquals(e.Entity, deleted));
        Assert.False(ctx.ChangeTracker.HasChanges());
    }
}
