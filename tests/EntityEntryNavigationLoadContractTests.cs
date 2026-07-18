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
/// Pins the EF-parity explicit-loading surface on <see cref="EntityEntry"/>:
/// <c>Entry(e).Collection(name).Load()</c> and <c>Entry(e).Reference(name).Load()</c> populate a
/// navigation on demand, <c>IsLoaded</c> reflects the state, and the reference/collection kind is validated.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class EntityEntryNavigationLoadContractTests
{
    [Table("EenOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    [Table("EenLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
        public Order? Order { get; set; }
    }

    private static DbContext NewCtx(SqliteConnection cn)
    {
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne(l => l.Order).HasForeignKey(l => l.OrderId, o => o.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static async Task<SqliteConnection> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE EenOrder (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
                CREATE TABLE EenLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL);
                INSERT INTO EenOrder VALUES (1, 'o1');
                INSERT INTO EenLine VALUES (1, 1, 'a'), (2, 1, 'b');
                """;
            cmd.ExecuteNonQuery();
        }
        await Task.CompletedTask;
        return cn;
    }

    [Fact]
    public async Task Collection_Load_populates_the_navigation_and_sets_IsLoaded()
    {
        using var cn = await SeedAsync();
        await using var ctx = NewCtx(cn);

        var order = (await ctx.Query<Order>().FirstOrDefaultAsync(o => o.Id == 1))!;
        var nav = ctx.Entry(order).Collection("Lines");

        nav.Load();

        Assert.True(nav.IsLoaded);
        Assert.Equal(new[] { "a", "b" }, order.Lines.OrderBy(l => l.Id).Select(l => l.Sku).ToArray());
    }

    [Fact]
    public async Task Reference_LoadAsync_populates_the_to_one_navigation()
    {
        using var cn = await SeedAsync();
        await using var ctx = NewCtx(cn);

        var line = (await ctx.Query<Line>().FirstOrDefaultAsync(l => l.Id == 1))!;
        Assert.Null(line.Order);

        await ctx.Entry(line).Reference("Order").LoadAsync();

        Assert.NotNull(line.Order);
        Assert.Equal(1, line.Order!.Id);
        Assert.True(ctx.Entry(line).Reference("Order").IsLoaded);
    }

    [Fact]
    public async Task Navigation_kind_is_validated()
    {
        using var cn = await SeedAsync();
        await using var ctx = NewCtx(cn);

        var order = (await ctx.Query<Order>().FirstOrDefaultAsync(o => o.Id == 1))!;
        var entry = ctx.Entry(order);

        Assert.Throws<ArgumentException>(() => entry.Reference("Lines"));   // Lines is a collection
        Assert.Throws<ArgumentException>(() => entry.Collection("Code"));   // Code is a scalar
        Assert.Throws<ArgumentException>(() => entry.Navigation("Nope"));   // no such property
    }
}
