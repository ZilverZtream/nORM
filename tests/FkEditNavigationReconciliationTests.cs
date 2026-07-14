using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;
using Xunit.Abstractions;

#nullable enable

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class FkEditNavigationReconciliationTests
{
    private readonly ITestOutputHelper _output;
    public FkEditNavigationReconciliationTests(ITestOutputHelper output) => _output = output;

    [System.ComponentModel.DataAnnotations.Schema.Table("FkNav_Parent")]
    public class FkNavParent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<FkNavChild> Children { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("FkNav_Child")]
    public class FkNavChild
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
        public FkNavParent? Parent { get; set; }
    }

    [Fact]
    public async Task Fk_edit_outranks_stale_nav_after_a_nav_driven_move()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FkNav_Parent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE FkNav_Child (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL)
                """;
            cmd.ExecuteNonQuery();
        }

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<FkNavParent>().HasKey(p => p.Id);
                mb.Entity<FkNavChild>().HasKey(c => c.Id);
                mb.Entity<FkNavParent>().HasMany(p => p.Children).WithOne()
                                        .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var p17 = new FkNavParent { Id = 17, Name = "seventeen" };
        var p5 = new FkNavParent { Id = 5, Name = "five" };
        var child = new FkNavChild { Id = 71, ParentId = 17, Val = -37 };
        ctx.Add(p17);
        ctx.Add(p5);
        ctx.Add(child);
        await ctx.SaveChangesAsync();

        // Re-track everything lazily (as after a context discard + requery).
        ctx.ChangeTracker.Clear();
        var parents = await ctx.Query<FkNavParent>().ToListAsync();
        child = await ctx.Query<FkNavChild>().FirstAsync(c => c.Id == 71);
        p5 = parents.First(p => p.Id == 5);

        // Nav-driven move: Parent nav points at 5, FK scalar is stale (17) -> nav wins.
        child.Parent = p5;
        await ctx.SaveChangesAsync();
        _output.WriteLine($"after nav move: ParentId={child.ParentId}");
        Assert.Equal(5, child.ParentId);

        // Deliberate FK edit back to 17; the nav still points at 5 (stale).
        child.ParentId = 17;
        await ctx.SaveChangesAsync();
        _output.WriteLine($"after fk edit: ParentId={child.ParentId}");
        Assert.Equal(17, child.ParentId);

        // The navigation must have been reconciled to the FK's principal — if it
        // still pointed at 5, the NEXT save would silently drag the FK back (the
        // accepted baseline now equals the edited value, hiding the edit).
        Assert.NotNull(child.Parent);
        Assert.Equal(17, child.Parent!.Id);

        // An unrelated later save must not revert the FK.
        var unrelated = new FkNavChild { Id = 99, ParentId = 5, Val = 1 };
        ctx.Add(unrelated);
        await ctx.SaveChangesAsync();

        ctx.ChangeTracker.Clear();
        var reloaded = await ctx.Query<FkNavChild>().FirstAsync(c => c.Id == 71);
        _output.WriteLine($"db: ParentId={reloaded.ParentId}");
        Assert.Equal(17, reloaded.ParentId);
    }
}

