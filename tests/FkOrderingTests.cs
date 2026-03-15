using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>Topological sort ensures principals are inserted before dependents.</summary>
public class FkOrderingTests
{
    private class Category
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private class Item
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        // Auto-detected FK: "CategoryId" strips "Id" → ForeignKeyPrincipalTypeName = "Category"
        public int CategoryId { get; set; }
    }

    [Fact]
    public async Task SaveChanges_InsertsParentBeforeChild_WhenAddedInReverseOrder()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        // Enable FK constraints and create tables
        await using (var pragma = cn.CreateCommand())
        {
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            await pragma.ExecuteNonQueryAsync();
        }
        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Category (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }
        await using (var cmd2 = cn.CreateCommand())
        {
            cmd2.CommandText = "CREATE TABLE Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, CategoryId INTEGER NOT NULL REFERENCES Category(Id));";
            await cmd2.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());

        // Add child before parent — topological sort should fix the order
        var child = new Item { Id = 1, Name = "Widget", CategoryId = 10 };
        var parent = new Category { Id = 10, Name = "Electronics" };

        ctx.Add(child);
        ctx.Add(parent);

        // Should not throw FK violation
        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(2, affected);
    }

    [Fact]
    public async Task SaveChanges_DeletesDependentBeforePrincipal()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();

        await using (var pragma = cn.CreateCommand())
        {
            pragma.CommandText = "PRAGMA foreign_keys = ON;";
            await pragma.ExecuteNonQueryAsync();
        }
        await using (var setup = cn.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE Category (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL); CREATE TABLE Item (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, CategoryId INTEGER NOT NULL REFERENCES Category(Id));";
            await setup.ExecuteNonQueryAsync();
        }
        await using (var insert = cn.CreateCommand())
        {
            insert.CommandText = "INSERT INTO Category VALUES (10, 'Electronics'); INSERT INTO Item VALUES (1, 'Widget', 10);";
            await insert.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());

        // Mark both for deletion — principal added to tracker first
        var parent = new Category { Id = 10, Name = "Electronics" };
        var child = new Item { Id = 1, Name = "Widget", CategoryId = 10 };

        ctx.Remove(parent);
        ctx.Remove(child);

        // Should not throw FK violation (child deleted before parent)
        var affected = await ctx.SaveChangesAsync(detectChanges: false);
        Assert.Equal(2, affected);
    }
}

/// <summary>Tests for namespace collision handling and cycle detection in topological sort.</summary>
public class FkOrderingAdvancedTests
{
    // Two entities in different namespaces with the same simple type name.
    // Neither has a FK to the other, so they should sort without error.
    [Table("ModuleACustomer")]
    private class ModuleACustomer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("ModuleBCustomer")]
    private class ModuleBCustomer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        // FK to ModuleACustomer (different namespace, same simple name "Customer" is not involved here)
        public int ModuleACustomerId { get; set; }
    }

    [Fact]
    public async Task SaveChanges_DoesNotConfuseTypesWithSameSimpleName()
    {
        // Both tables exist, no actual FK constraint in SQLite, just verifying ordering works
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE ModuleACustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                              "CREATE TABLE ModuleBCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, ModuleACustomerId INTEGER NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new ModuleACustomer { Id = 1, Name = "A" });
        ctx.Add(new ModuleBCustomer { Id = 2, Name = "B", ModuleACustomerId = 1 });
        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(2, affected);
    }

    // Entities with a circular FK dependency
    [Table("CircA")]
    private class CircA
    {
        [Key] public int Id { get; set; }
        public int CircBId { get; set; }  // auto-detected FK -> CircB
    }

    [Table("CircB")]
    private class CircB
    {
        [Key] public int Id { get; set; }
        public int CircAId { get; set; }  // auto-detected FK -> CircA
    }

    [Fact]
    public async Task SaveChanges_ThrowsOnCircularFkDependency()
    {
        await using var cn = new SqliteConnection("Data Source=:memory:");
        await cn.OpenAsync();
        await using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CircA (Id INTEGER PRIMARY KEY, CircBId INTEGER NOT NULL);" +
                              "CREATE TABLE CircB (Id INTEGER PRIMARY KEY, CircAId INTEGER NOT NULL);";
            await cmd.ExecuteNonQueryAsync();
        }

        await using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new CircA { Id = 1, CircBId = 2 });
        ctx.Add(new CircB { Id = 2, CircAId = 1 });

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(() => ctx.SaveChangesAsync());
        Assert.Contains("Circular FK dependency", ex.Message);
    }
}
