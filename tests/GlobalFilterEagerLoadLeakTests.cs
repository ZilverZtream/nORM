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
/// A global filter (soft-delete !IsDeleted) on a child entity must also apply when that child is
/// eager-loaded through Include()/AsSplitQuery(). The root query excludes soft-deleted rows, but the
/// hand-built dependent-query SQL applied only the tenant predicate, not the general global filter —
/// so soft-deleted children leaked into the navigation collection.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GlobalFilterEagerLoadLeakTests
{
    [Table("GfeParent")]
    private class GfeParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<GfeChild> Children { get; set; } = new();
    }

    [Table("GfeChild")]
    private class GfeChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public bool IsDeleted { get; set; }
        public string Label { get; set; } = "";
    }

    private static DbContext CreateContext(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE GfeParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE GfeChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, IsDeleted INTEGER NOT NULL, Label TEXT NOT NULL);" +
                "INSERT INTO GfeParent VALUES (1,'p');" +
                "INSERT INTO GfeChild VALUES (10,1,0,'live'),(11,1,1,'deleted');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GfeParent>()
                .HasKey(p => p.Id)
                .HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id)
        };
        opts.AddGlobalFilter<GfeChild>(c => !c.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Include_split_query_excludes_soft_deleted_children()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateContext(cn);

        var parent = (await ((INormQueryable<GfeParent>)ctx.Query<GfeParent>())
            .Include(p => p.Children)
            .AsSplitQuery()
            .ToListAsync())
            .Single();

        // Only the non-deleted child (Id 10) — Id 11 is soft-deleted and must not leak.
        Assert.Equal(new[] { 10 }, parent.Children.Select(c => c.Id).OrderBy(i => i));
        Assert.DoesNotContain(parent.Children, c => c.IsDeleted);
    }
}
