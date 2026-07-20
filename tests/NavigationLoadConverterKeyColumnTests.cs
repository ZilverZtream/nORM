using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// When the principal-key / foreign-key column carries a value converter (e.g. a strongly-typed id or an
/// enum-name key), every navigation-loading path builds a `FK IN (…)` / `FK = @p` predicate. The bound key
/// value must be converted to the PROVIDER representation the child column actually stores — otherwise the
/// comparison pits the model value against the stored provider value, matches nothing, and the navigation
/// silently comes back EMPTY (total, silent loss of the related rows). The scalar `Where` path already
/// converts; eager Include, split-query, and lazy/explicit collection loads must too. Uses an order-preserving
/// offset converter (stored = model + 1000) so the model key (1) and the stored key (1001) are distinct.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationLoadConverterKeyColumnTests
{
    [Table("NlckOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    [Table("NlckLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public Order? Order { get; set; }
    }

    // Order-preserving: model N stored as N + 1000.
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            // Stored provider values: Order.Id = 1001 (model 1); the two lines' OrderId = 1001 (model 1).
            cmd.CommandText = """
                CREATE TABLE NlckOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE NlckLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL);
                INSERT INTO NlckOrder VALUES (1001);
                INSERT INTO NlckLine VALUES (1, 1001), (2, 1001);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Order>().Property<int>(o => o.Id).HasConversion(new OffsetConverter());
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Line>().Property<int>(l => l.OrderId).HasConversion(new OffsetConverter());
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne(l => l.Order!).HasForeignKey(l => l.OrderId, o => o.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public async Task Eager_include_of_a_converter_key_collection_loads_the_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);
        var order = (await ctx.Query<Order>().Include(o => o.Lines).ToListAsync()).Single();
        Assert.Equal(1, order.Id);                     // converter applied on read
        Assert.Equal(new[] { 1, 2 }, order.Lines.OrderBy(l => l.Id).Select(l => l.Id).ToArray());
    }

    [Fact]
    public void Sync_eager_include_of_a_converter_key_collection_loads_the_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);
        var order = ctx.Query<Order>().Include(o => o.Lines).ToList().Single();
        Assert.Equal(new[] { 1, 2 }, order.Lines.OrderBy(l => l.Id).Select(l => l.Id).ToArray());
    }

    [Fact]
    public async Task Split_query_include_of_a_converter_key_collection_loads_the_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);
        var order = (await ctx.Query<Order>().AsSplitQuery().Include(o => o.Lines).ToListAsync()).Single();
        Assert.Equal(new[] { 1, 2 }, order.Lines.OrderBy(l => l.Id).Select(l => l.Id).ToArray());
    }

    [Fact]
    public async Task Explicit_collection_load_of_a_converter_key_loads_the_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Bootstrap(cn);
        var order = await ctx.Query<Order>().FirstAsync();
        // ProcessEntity attaches a NavigationContext typed as object; replace it with the correct
        // entity type so the Lines relation resolves for the batched navigation loader.
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(order, new NavigationContext(ctx, typeof(Order)));
        await order.LoadAsync(o => o.Lines);
        Assert.Equal(new[] { 1, 2 }, order.Lines.OrderBy(l => l.Id).Select(l => l.Id).ToArray());
    }
}
