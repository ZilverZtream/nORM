using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A SavingChanges interceptor may edit the entity graph through navigations — add a child to a collection
/// nav, assign a reference nav to a new principal, or Remove a principal with loaded children — exactly as
/// code can before SaveChanges (the documented contract). nORM re-ran only DetectAllChanges after the hook,
/// not relationship fixup / cascade, so those graph edits were silently lost: a nav-added child never
/// inserted, a hook-deleted principal's loaded children orphaned.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SavingChangesGraphEditTests
{
    [Table("ScgOrder")]
    public class Order
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    [Table("ScgLine")]
    public class Line
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Product { get; set; } = "";
    }

    private sealed class AddLineInterceptor : ISaveChangesInterceptor
    {
        private readonly Order _order;
        private readonly string _product;
        public AddLineInterceptor(Order order, string product) { _order = order; _product = product; }
        public Task SavingChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, CancellationToken ct)
        {
            _order.Lines.Add(new Line { Product = _product });   // graph edit via collection nav — no ctx.Add
            return Task.CompletedTask;
        }
        public Task SavedChangesAsync(DbContext context, IReadOnlyList<EntityEntry> entries, int result, CancellationToken ct)
            => Task.CompletedTask;
    }

    private static (SqliteConnection Keeper, Func<ISaveChangesInterceptor?, DbContext> Make) Setup()
    {
        var keeper = new SqliteConnection($"Data Source=file:scg_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE ScgOrder (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL);" +
                "CREATE TABLE ScgLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, OrderId INTEGER NOT NULL, Product TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        DbContext Make(ISaveChangesInterceptor? interceptor)
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Order>()
                    .HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id)
            };
            if (interceptor != null) opts.SaveChangesInterceptors.Add(interceptor);
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static long LineCount(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM ScgLine";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Interceptor_adding_a_child_via_collection_nav_persists_it()
    {
        var (keeper, make) = Setup();
        using var _keeper = keeper;

        var order = new Order { Name = "o1" };
        await using var ctx = make(new AddLineInterceptor(order, "widget"));
        ctx.Add(order);
        await ctx.SaveChangesAsync();

        // The interceptor's nav-added line must be inserted, with the order's generated FK.
        Assert.Equal(1, LineCount(keeper));
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT OrderId FROM ScgLine WHERE Product = 'widget'";
        Assert.Equal((long)order.Id, Convert.ToInt64(cmd.ExecuteScalar()));
    }
}
