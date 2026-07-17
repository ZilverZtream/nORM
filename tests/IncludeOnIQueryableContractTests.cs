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
/// Pins that Include composes off the plain IQueryable&lt;T&gt; returned by Query&lt;T&gt;()/Set&lt;T&gt;()
/// and off standard operators (Where/OrderBy) WITHOUT the (INormQueryable&lt;T&gt;) cast — i.e. the
/// exact shape the README's flagship example documents. Before the IQueryable&lt;T&gt; Include
/// extension existed this file would not compile, which is the point: it's a compile-time
/// contract as much as a runtime one, and it eager-loads the collection either way.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class IncludeOnIQueryableContractTests
{
    [Table("IoqUser")]
    public class User
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Order> Orders { get; set; } = new();
    }

    [Table("IoqOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int UserId { get; set; }
        public string Product { get; set; } = "";
    }

    private static async Task<DbContext> BootstrapAsync(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE IoqUser (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE IoqOrder (Id INTEGER PRIMARY KEY, UserId INTEGER NOT NULL, Product TEXT NOT NULL);
                INSERT INTO IoqUser VALUES (1, 'John Smith');
                INSERT INTO IoqUser VALUES (2, 'Jane Doe');
                INSERT INTO IoqOrder VALUES (1, 1, 'Book');
                INSERT INTO IoqOrder VALUES (2, 1, 'Pen');
                INSERT INTO IoqOrder VALUES (3, 2, 'Lamp');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<User>().HasKey(u => u.Id);
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<User>().HasMany(u => u.Orders).WithOne().HasForeignKey(o => o.UserId, u => u.Id);
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        await Task.CompletedTask;
        return ctx;
    }

    [Fact]
    public async Task Include_composes_off_query_without_a_cast()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        // The exact README flagship shape: Query<T>().Where(...).Include(...).OrderBy(...).
        // No (INormQueryable<User>) cast anywhere — this is the compile-time contract.
        var users = await ctx.Query<User>()
            .Where(u => u.Name.StartsWith("John"))
            .Include(u => u.Orders)
            .OrderBy(u => u.Name)
            .ToListAsync();

        var john = Assert.Single(users);
        Assert.Equal("John Smith", john.Name);
        Assert.Equal(new[] { "Book", "Pen" }, john.Orders.OrderBy(o => o.Id).Select(o => o.Product).ToArray());
    }

    [Fact]
    public async Task Include_composes_directly_off_query_and_set()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await using var ctx = await BootstrapAsync(cn);

        // Directly off Query<T>() (no intervening operator) — also plain IQueryable<T>.
        var viaQuery = await ctx.Query<User>().Include(u => u.Orders).ToListAsync();
        Assert.Equal(3, viaQuery.Sum(u => u.Orders.Count));

        // And off the Set<T>() EF-parity alias.
        var viaSet = await ctx.Set<User>().Include(u => u.Orders).Where(u => u.Id == 1).ToListAsync();
        Assert.Equal(2, Assert.Single(viaSet).Orders.Count);
    }
}
