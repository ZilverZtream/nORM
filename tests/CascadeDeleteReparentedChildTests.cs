using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

using RelParent = CrudStateMachineFuzzTests.RelParent;
using RelChild = CrudStateMachineFuzzTests.RelChild;

/// <summary>
/// Cascade delete must follow the relationship a re-parented child actually
/// belongs to. A child moved onto a new principal by reference navigation (its FK
/// scalar left stale, because fixup skips a principal being deleted) cascades with
/// that new principal; a child moved OFF a principal by a deliberate FK edit does
/// not cascade with the former principal, even though it can linger in the old
/// parent's navigation collection.
/// </summary>
public class CascadeDeleteReparentedChildTests
{
    private static (Func<DbContext> open, SqliteConnection keeper) MakeRelDb(string dbName)
    {
        var cs = $"Data Source=file:{dbName}?mode=memory&cache=shared";
        var keeper = new SqliteConnection(cs);
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE RelParent_Test (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE RelChild_Test (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        Func<DbContext> open = () =>
        {
            var cn = new SqliteConnection(cs);
            cn.Open();
            return new DbContext(cn, new SqliteProvider(), CrudStateMachineFuzzTests.RelOptions());
        };
        return (open, keeper);
    }

    [Trait("Category", TestCategory.Fast)]
    [Fact]
    public async Task Child_reparented_by_navigation_cascades_with_the_deleted_new_principal()
    {
        var (open, keeper) = MakeRelDb($"nav_{Guid.NewGuid():N}");
        try
        {
            var ctx = open();
            var p7 = new RelParent { Id = 7, Name = "p7" };
            var p8 = new RelParent { Id = 8, Name = "p8" };
            var c = new RelChild { Id = 100, ParentId = 7, Val = 1 };
            ctx.Add(p7);
            ctx.Add(p8);
            ctx.Add(c);
            await ctx.SaveChangesAsync();

            // Re-parent the committed child onto p8 via navigation (FK scalar stays 7),
            // then delete p8 in the same save window. The child is now p8's dependent
            // and must cascade with it — not survive under its stale FK.
            c.Parent = p8;
            ctx.Remove(p8);
            await ctx.SaveChangesAsync();
            ctx.Dispose();

            using var fresh = open();
            var kids = await fresh.Query<RelChild>().ToListAsync();
            var parents = (await fresh.Query<RelParent>().ToListAsync()).OrderBy(p => p.Id).ToList();
            Assert.Empty(kids);
            Assert.Single(parents);
            Assert.Equal(7, parents[0].Id);
        }
        finally
        {
            keeper.Dispose();
        }
    }

    [Trait("Category", TestCategory.Fast)]
    [Fact]
    public async Task Child_moved_off_by_fk_edit_survives_deletion_of_its_former_principal()
    {
        var (open, keeper) = MakeRelDb($"fk_{Guid.NewGuid():N}");
        try
        {
            var ctx = open();
            var p7 = new RelParent { Id = 7, Name = "p7" };
            var p8 = new RelParent { Id = 8, Name = "p8" };
            var c = new RelChild { Id = 100, Val = 1, Parent = p7 };   // sits in p7.Children
            ctx.Add(p7);
            ctx.Add(p8);
            ctx.Add(c);
            await ctx.SaveChangesAsync();

            // Deliberate FK edit moves the child off p7 onto p8. The child can linger in
            // p7's navigation collection (a stale membership), but the edited FK outranks
            // it: deleting p7 must NOT cascade the child.
            c.ParentId = 8;
            await ctx.SaveChangesAsync();
            ctx.Remove(p7);
            await ctx.SaveChangesAsync();
            ctx.Dispose();

            using var fresh = open();
            var kids = (await fresh.Query<RelChild>().ToListAsync()).OrderBy(k => k.Id).ToList();
            Assert.Single(kids);
            Assert.Equal(8, kids[0].ParentId);
        }
        finally
        {
            keeper.Dispose();
        }
    }

    [Trait("Category", TestCategory.Fast)]
    [Fact]
    public async Task Child_reparented_by_fk_edit_stays_tracked_when_former_principal_is_deleted()
    {
        var (open, keeper) = MakeRelDb($"track_{Guid.NewGuid():N}");
        try
        {
            var ctx = open();
            var p6 = new RelParent { Id = 6, Name = "p6" };
            var p5 = new RelParent { Id = 5, Name = "p5" };
            var p7 = new RelParent { Id = 7, Name = "p7" };
            var c = new RelChild { Id = 100, Val = 1 };
            p6.Children.Add(c);                 // graph child: sits in p6.Children, FK fixed to 6
            ctx.Add(p6);
            ctx.Add(p5);
            ctx.Add(p7);
            await ctx.SaveChangesAsync();

            c.ParentId = 5;                     // FK edit off p6 -> p5; c still lingers in p6.Children
            await ctx.SaveChangesAsync();

            // Deleting the FORMER principal must not detach c — it belongs to p5 now. Detaching it
            // (because it lingers in p6's collection) would silently strip its change tracking and
            // drop the later reparent below.
            ctx.Remove(p6);
            await ctx.SaveChangesAsync();

            c.Parent = p7;                      // reparent to p7 — only persists if c stayed tracked
            await ctx.SaveChangesAsync();
            ctx.Dispose();

            using var fresh = open();
            var kids = (await fresh.Query<RelChild>().ToListAsync()).OrderBy(k => k.Id).ToList();
            Assert.Single(kids);
            Assert.Equal(7, kids[0].ParentId);  // reparent took effect → c was still tracked
        }
        finally
        {
            keeper.Dispose();
        }
    }
}
