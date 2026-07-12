using System;
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
/// SaveChanges topologically sorts entity MAPPINGS (types) so principals insert
/// before dependents. A self-referential table (Category → ParentCategory) is a
/// single mapping, so the intra-group ROW order must also honour the dependency:
/// a child row referencing a parent row inserted in the same batch must not be
/// written before its parent, or the foreign key fails.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SelfReferentialSaveOrderingTests
{
    [Table("SrCategory")]
    private class SrCategory
    {
        [Key] public int Id { get; set; }
        public int? ParentId { get; set; }
        public SrCategory Parent { get; set; } = null!;
        public System.Collections.Generic.List<SrCategory> Children { get; set; } = new();
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            // Enforce foreign keys so an out-of-order insert actually fails.
            cmd.CommandText = "PRAGMA foreign_keys = ON;" +
                "CREATE TABLE SrCategory (Id INTEGER PRIMARY KEY, ParentId INTEGER NULL REFERENCES SrCategory(Id), Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SrCategory>()
                .HasKey(c => c.Id)
                .HasMany(c => c.Children).WithOne(c => c.Parent).HasForeignKey(c => c.ParentId!, c => c.Id)
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Child_added_before_self_referencing_parent_persists_without_fk_violation()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Add the CHILD (references parent Id=1) BEFORE the parent — the wrong order
        // for a naive row-by-row insert. Explicit keys so ParentId is known up front.
        var child = new SrCategory { Id = 2, ParentId = 1, Name = "child" };
        var parent = new SrCategory { Id = 1, ParentId = null, Name = "root" };
        ctx.Add(child);
        ctx.Add(parent);

        await ctx.SaveChangesAsync();

        var all = await ctx.Query<SrCategory>().OrderBy(c => c.Id).ToListAsync();
        Assert.Equal(2, all.Count);
        Assert.Null(all[0].ParentId);
        Assert.Equal(1, all[1].ParentId);
    }

    [Fact]
    public async Task Deep_self_referential_chain_added_in_reverse_persists()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // A 4-level chain added leaf-first (worst case for intra-group ordering).
        ctx.Add(new SrCategory { Id = 4, ParentId = 3, Name = "d" });
        ctx.Add(new SrCategory { Id = 3, ParentId = 2, Name = "c" });
        ctx.Add(new SrCategory { Id = 2, ParentId = 1, Name = "b" });
        ctx.Add(new SrCategory { Id = 1, ParentId = null, Name = "a" });

        await ctx.SaveChangesAsync();

        Assert.Equal(4, await ctx.Query<SrCategory>().CountAsync());
    }
}
