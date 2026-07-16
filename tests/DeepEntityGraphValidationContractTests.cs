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
/// Pins entity-graph validation on legal DEEP graphs: nORM's relationship fixup deliberately
/// leaves stale collection membership behind when a child is re-parented, so a long-running
/// tracked graph develops chains of DISTINCT parent/child instances (parent1.Children holds a
/// child whose Parent nav points at parent2, whose Children holds a child pointing at
/// parent3, ...). Add/Remove/Update validation must accept such finite, cycle-safe graphs of
/// any length — a fixed depth cap crashed Remove() at chain length ~4 (found by the CRUD
/// relationship machine, seed 602775). Cycle safety comes from the visited set, not a cap.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class DeepEntityGraphValidationContractTests
{
    [Table("DegParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("DegChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public Parent? Parent { get; set; }
    }

    [Fact]
    public async System.Threading.Tasks.Task Remove_accepts_a_long_reparenting_chain_of_distinct_entities()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE DegParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE DegChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne(c => c.Parent!).HasForeignKey(c => c.ParentId);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        // Build the shape fixup leaves behind after repeated re-parenting: each parent's
        // Children still CONTAINS a child whose Parent nav already points at the NEXT parent.
        const int chainLength = 16;
        var parents = Enumerable.Range(1, chainLength)
            .Select(i => new Parent { Id = i, Name = $"p{i}" }).ToArray();
        for (var i = 0; i < chainLength - 1; i++)
        {
            var child = new Child { Id = i + 1, ParentId = parents[i + 1].Id, Parent = parents[i + 1] };
            parents[i].Children.Add(child);        // stale membership in the FORMER parent
            parents[i + 1].Children.Add(child);    // real membership in the new parent
        }

        foreach (var p in parents)
            ctx.Add(p);                            // graph validation runs here
        await ctx.SaveChangesAsync();

        ctx.Remove(parents[0]);                    // and here — this crashed at chain ~4
        await ctx.SaveChangesAsync();

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM DegParent";
        Assert.Equal(chainLength - 1, Convert.ToInt32(check.ExecuteScalar()));
    }
}
