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

[Trait("Category", "Fast")]
public class SplitQueryIncludePagingTests
{
    [Table("SqpParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("SqpChild")]
    public class Child { [Key] public int Id { get; set; } public int ParentId { get; set; } public int Val { get; set; } }

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SqpParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE SqpChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO SqpParent VALUES (1,'a'),(2,'b'),(3,'c'),(4,'d');
                """;
            cmd.ExecuteNonQuery();
            // Each parent p gets 2 children with Val = p*10 + k.
            var id = 1;
            for (var p = 1; p <= 4; p++)
                for (var k = 0; k < 2; k++)
                {
                    using var ins = cn.CreateCommand();
                    ins.CommandText = $"INSERT INTO SqpChild VALUES ({id++}, {p}, {p * 10 + k})";
                    ins.ExecuteNonQuery();
                }
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Split_include_with_parent_paging_attaches_correct_children()
    {
        await using var ctx = Make();
        // Page the PARENTS (skip 1, take 2 => parents 2 and 3), split-load their children.
        var parents = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking()
            .Include(p => p.Children)
            .AsSplitQuery()
            .OrderBy(p => p.Id)
            .Skip(1).Take(2)
            .ToListAsync();

        Assert.Equal(new[] { 2, 3 }, parents.Select(p => p.Id).ToArray());
        foreach (var p in parents)
        {
            // Each paged parent has exactly its own 2 children (Val = p*10 + {0,1}), no others.
            Assert.Equal(2, p.Children.Count);
            Assert.All(p.Children, c => Assert.Equal(p.Id, c.ParentId));
            Assert.Equal(new[] { p.Id * 10, p.Id * 10 + 1 }, p.Children.Select(c => c.Val).OrderBy(v => v).ToArray());
        }
    }

    [Fact]
    public async Task Split_include_without_paging_loads_all_correctly()
    {
        await using var ctx = Make();
        var parents = await ((INormQueryable<Parent>)ctx.Query<Parent>())
            .AsNoTracking()
            .Include(p => p.Children)
            .AsSplitQuery()
            .OrderBy(p => p.Id)
            .ToListAsync();

        Assert.Equal(4, parents.Count);
        foreach (var p in parents)
        {
            Assert.Equal(2, p.Children.Count);
            Assert.All(p.Children, c => Assert.Equal(p.Id, c.ParentId));
        }
    }
}
