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
/// Clearing a REQUIRED (non-nullable FK) reference navigation must not crash. A
/// required relationship cannot be severed by nulling its FK — the column cannot
/// hold null — so the still-valid FK is left intact (the child keeps its principal)
/// rather than unboxing null into the value-type column and throwing.
/// </summary>
public class RequiredReferenceNavClearTests
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
    public async Task Clearing_a_required_reference_navigation_leaves_the_foreign_key_intact()
    {
        var (open, keeper) = MakeRelDb($"reqnav_{Guid.NewGuid():N}");
        try
        {
            var ctx = open();
            var p = new RelParent { Id = 1, Name = "p1" };
            var c = new RelChild { Id = 10, Val = 5, Parent = p };  // reference nav set → captured as loaded on save
            ctx.Add(p);
            ctx.Add(c);
            await ctx.SaveChangesAsync();

            // Clear the required reference navigation. The FK is non-nullable, so the
            // relationship cannot be severed this way — the save must not throw, and the
            // child keeps its principal.
            c.Parent = null;
            await ctx.SaveChangesAsync();
            ctx.Dispose();

            using var fresh = open();
            var kids = (await fresh.Query<RelChild>().ToListAsync()).OrderBy(k => k.Id).ToList();
            Assert.Single(kids);
            Assert.Equal(10, kids[0].Id);
            Assert.Equal(1, kids[0].ParentId);   // FK left intact
        }
        finally
        {
            keeper.Dispose();
        }
    }
}
