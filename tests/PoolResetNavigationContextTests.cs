using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Entities read through a context are registered in the process-wide navigation-context table pointing back
/// at that context, so lazy/explicit navigation loads know which context to use. When a pooled context is
/// reset for reuse, those registrations must be removed: otherwise an entity that outlived the request scope
/// still resolves to the context after it is re-leased to another request (another tenant), and a later
/// navigation load reads the new lease's tenant and races its connection. Clearing the change tracker alone
/// left the registrations behind.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class PoolResetNavigationContextTests
{
    [Table("PrncParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [Table("PrncChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
    }

    private static DbContext Make(SqliteConnection cn)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Parent>()
                .HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id)
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public void Pool_reset_removes_the_navigation_context_registration_for_tracked_entities()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE PrncParent (Id INTEGER PRIMARY KEY);
                CREATE TABLE PrncChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);
                INSERT INTO PrncParent VALUES (1);
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = Make(cn);

        var parent = ctx.Query<Parent>().First();   // tracked read registers a navigation context
        Assert.True(NavigationPropertyExtensions._navigationContexts.TryGetValue(parent, out _),
            "querying a tracked entity should register its navigation context");

        var reset = ctx.TryResetForPooling();
        Assert.True(reset, "a context with no live transaction must be poolable");

        Assert.False(NavigationPropertyExtensions._navigationContexts.TryGetValue(parent, out _),
            "pool reset must remove the navigation-context registration so the escaped entity cannot reach the re-leased context");
    }
}
