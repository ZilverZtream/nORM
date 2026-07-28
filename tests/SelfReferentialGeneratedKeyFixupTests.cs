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
/// A self-referential graph (parent + child through the SAME mapping) saved with a
/// generated key must persist the child's foreign key as the parent's generated key.
/// Parent and child land in one Added batch; the child's FK parameter was bound before
/// the parent's INSERT hydrated its key, so the child row was written with FK = 0/NULL —
/// the in-memory entity was fixed up afterwards, but the persisted row was silently wrong.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SelfReferentialGeneratedKeyFixupTests
{
    [Table("SrGenNode")]
    private class Node
    {
        [Key] public int Id { get; set; }   // convention store-generated key
        public int? ParentId { get; set; }
        public Node? Parent { get; set; }
        public List<Node> Children { get; set; } = new();
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "PRAGMA foreign_keys = ON;" +
                "CREATE TABLE SrGenNode (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NULL REFERENCES SrGenNode(Id), Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Node>()
                .HasKey(c => c.Id)
                .HasMany(c => c.Children).WithOne(c => c.Parent!).HasForeignKey(c => c.ParentId!, c => c.Id)
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static long? ScalarNullable(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        var v = cmd.ExecuteScalar();
        return v is null or DBNull ? null : Convert.ToInt64(v);
    }

    [Fact]
    public async Task Self_referential_child_fk_persists_the_generated_parent_key()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        var root = new Node { Name = "root" };
        root.Children.Add(new Node { Name = "child" });
        ctx.Add(root);

        await ctx.SaveChangesAsync();

        Assert.NotEqual(0, root.Id);

        // Read the RAW persisted FK for the child — the silent-wrong bug leaves it 0/NULL
        // while the in-memory child.ParentId reads correctly.
        var rootKey = ScalarNullable(cn, "SELECT Id FROM SrGenNode WHERE Name = 'root'");
        var childFk = ScalarNullable(cn, "SELECT ParentId FROM SrGenNode WHERE Name = 'child'");
        Assert.Equal(rootKey, childFk);
    }

    [Fact]
    public async Task Self_referential_reference_nav_persists_the_generated_parent_key()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Only the reference direction (child.Parent) is set; the parent's Children collection is empty.
        var root = new Node { Name = "root" };
        var child = new Node { Name = "child", Parent = root };
        ctx.Add(child);

        await ctx.SaveChangesAsync();

        var rootKey = ScalarNullable(cn, "SELECT Id FROM SrGenNode WHERE Name = 'root'");
        var childFk = ScalarNullable(cn, "SELECT ParentId FROM SrGenNode WHERE Name = 'child'");
        Assert.NotNull(rootKey);
        Assert.Equal(rootKey, childFk);
    }

    [Fact]
    public async Task Deep_self_referential_chain_persists_generated_keys()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // A 3-level chain built parent-first through the collection nav; every FK must resolve.
        var a = new Node { Name = "a" };
        var b = new Node { Name = "b" };
        var c = new Node { Name = "c" };
        a.Children.Add(b);
        b.Children.Add(c);
        ctx.Add(a);

        await ctx.SaveChangesAsync();

        var aKey = ScalarNullable(cn, "SELECT Id FROM SrGenNode WHERE Name = 'a'");
        var bKey = ScalarNullable(cn, "SELECT Id FROM SrGenNode WHERE Name = 'b'");
        var bFk = ScalarNullable(cn, "SELECT ParentId FROM SrGenNode WHERE Name = 'b'");
        var cFk = ScalarNullable(cn, "SELECT ParentId FROM SrGenNode WHERE Name = 'c'");
        Assert.Equal(aKey, bFk);
        Assert.Equal(bKey, cFk);
    }
}
