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
/// Surfaces 7 (navigation member + same-typed scalar) and 8 (join transparent-identifier) of the projection
/// materialization hunt. Distinct values so a positional swap is observable.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ProjectionMaterializationNavJoinTests
{
    // ---------- Surface 7: navigation member + a scalar of the SAME type ----------
    [Table("Pm56Parent_Test")]
    public sealed class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("Pm56Child_Test")]
    public sealed class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Label { get; set; } = "";
        public Parent? Parent { get; set; }
    }

    private static (SqliteConnection, DbContext) CreateNav()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Pm56Parent_Test (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE Pm56Child_Test (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Label TEXT NOT NULL);" +
                // Child.Id=100 distinct from Parent.Id=1 so Id vs Parent.Id swap is observable.
                "INSERT INTO Pm56Parent_Test VALUES (1, 'p1');" +
                "INSERT INTO Pm56Child_Test VALUES (100, 1, 'c1');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Parent>().HasKey(p => p.Id)
                .HasMany(p => p.Children).WithOne(c => c.Parent!).HasForeignKey(c => c.ParentId, p => p.Id)
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false));
    }

    [Fact]
    public void Nav_member_and_same_typed_scalar_not_swapped()
    {
        var (cn, ctx) = CreateNav();
        using var _cn = cn; using var _ctx = ctx;

        // x.Id = 100 (child pk); x.Parent.Id = 1 (parent pk via FK). Both int.
        var r = ctx.Query<Child>().Select(x => new { x.Id, ParentId = x.Parent!.Id }).First();

        Assert.Equal(100, r.Id);      // child's own Id
        Assert.Equal(1, r.ParentId);  // parent's Id
    }

    [Fact]
    public void Nav_member_first_scalar_second_not_swapped()
    {
        var (cn, ctx) = CreateNav();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<Child>().Select(x => new { PId = x.Parent!.Id, MyId = x.Id }).First();

        Assert.Equal(1, r.PId);    // parent's Id
        Assert.Equal(100, r.MyId); // child's own Id
    }

    // ---------- Surface 8: join transparent identifier, same-named/same-typed members ----------
    [Table("Pm56JA_Test")]
    public sealed class EntA
    {
        [Key] public int Id { get; set; }
        public int Ref { get; set; }
        public string AName { get; set; } = "";
    }

    [Table("Pm56JB_Test")]
    public sealed class EntB
    {
        [Key] public int Id { get; set; }
        public string BName { get; set; } = "";
    }

    private static (SqliteConnection, DbContext) CreateJoin()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Pm56JA_Test (Id INTEGER PRIMARY KEY, Ref INTEGER NOT NULL, AName TEXT NOT NULL);" +
                "CREATE TABLE Pm56JB_Test (Id INTEGER PRIMARY KEY, BName TEXT NOT NULL);" +
                // A.Id=100, A.Ref=5 -> joins B.Id=5. So a.Id=100, b.Id=5 distinct.
                "INSERT INTO Pm56JA_Test VALUES (100, 5, 'a1');" +
                "INSERT INTO Pm56JB_Test VALUES (5, 'b1');";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider(), new DbContextOptions()));
    }

    [Fact]
    public void Join_transparent_identifier_same_typed_ids_not_swapped()
    {
        var (cn, ctx) = CreateJoin();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<EntA>()
            .Join(ctx.Query<EntB>(), a => a.Ref, b => b.Id, (a, b) => new { AId = a.Id, BId = b.Id })
            .First();

        Assert.Equal(100, r.AId); // a.Id
        Assert.Equal(5, r.BId);   // b.Id
    }

    // Reversed member order in the result selector to stress alignment.
    [Fact]
    public void Join_transparent_identifier_reversed_member_order_not_swapped()
    {
        var (cn, ctx) = CreateJoin();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<EntA>()
            .Join(ctx.Query<EntB>(), a => a.Ref, b => b.Id, (a, b) => new { BId = b.Id, AId = a.Id })
            .First();

        Assert.Equal(5, r.BId);    // b.Id
        Assert.Equal(100, r.AId);  // a.Id
    }

    // Join projecting BOTH ids AND names crossed to catch a broader permutation.
    [Fact]
    public void Join_projection_mixed_ids_and_names_align()
    {
        var (cn, ctx) = CreateJoin();
        using var _cn = cn; using var _ctx = ctx;

        var r = ctx.Query<EntA>()
            .Join(ctx.Query<EntB>(), a => a.Ref, b => b.Id, (a, b) => new
            {
                BName = b.BName,
                AId = a.Id,
                AName = a.AName,
                BId = b.Id
            })
            .First();

        Assert.Equal("b1", r.BName);
        Assert.Equal(100, r.AId);
        Assert.Equal("a1", r.AName);
        Assert.Equal(5, r.BId);
    }
}
