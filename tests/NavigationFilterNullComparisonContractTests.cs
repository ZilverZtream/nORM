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
/// Pins null comparisons in the navigation-filter grammar (filtered Include, shaped-collection filters,
/// global filters): <c>c.DeletedAt == null</c> lowers to <c>IS NULL</c> and <c>!= null</c> to <c>IS NOT
/// NULL</c>. Previously the grammar emitted <c>col = NULL</c> / <c>col &lt;&gt; NULL</c>, which are always
/// unknown, so every child was silently dropped — breaking, most importantly, the common
/// <c>DeletedAt == null</c> soft-delete global filter.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationFilterNullComparisonContractTests
{
    [Table("NncOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    [Table("NncLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public int? DeletedAt { get; set; }
    }

    private static DbContext Bootstrap(SqliteConnection cn, bool softDeleteGlobalFilter = false)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE NncOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE NncLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, DeletedAt INTEGER NULL);
                INSERT INTO NncOrder VALUES (1);
                INSERT INTO NncLine VALUES (1,1,NULL),(2,1,100),(3,1,NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
        }};
        if (softDeleteGlobalFilter)
            opts.AddGlobalFilter<Line>(l => l.DeletedAt == null);
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static int[] Load(DbContext ctx, Func<INormQueryable<Order>, IQueryable<Order>> shape)
        => shape((INormQueryable<Order>)ctx.Query<Order>()).ToList().Single()
            .Lines.OrderBy(l => l.Id).Select(l => l.Id).ToArray();

    [Fact]
    public void Is_null_filter_matches_null_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { 1, 3 }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.DeletedAt == null))));
    }

    [Fact]
    public void Is_not_null_filter_matches_non_null_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { 2 }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.DeletedAt != null))));
    }

    [Fact]
    public void Soft_delete_null_global_filter_hides_deleted_children()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn, softDeleteGlobalFilter: true);
        // The global filter DeletedAt == null must show the non-deleted children, not drop them all.
        Assert.Equal(new[] { 1, 3 }, Load(ctx, q => q.Include(o => o.Lines)));
    }

    [Fact]
    public void Null_check_composes_with_conjunction()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); using var ctx = Bootstrap(cn);
        Assert.Equal(new[] { 3 }, Load(ctx, q => q.Include(o => o.Lines.Where(l => l.DeletedAt == null && l.Id > 1))));
    }
}
