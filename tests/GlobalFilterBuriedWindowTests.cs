using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// A global (soft-delete / tenant) filter must be injected at the entity root — BEFORE any
/// window/dedup operator (Take/Skip/DistinctBy) — because those operators do not commute with a filter.
/// The top-level Take/Skip case is handled; when the window sits UNDER a trailing same-element-type
/// operator (Where/OrderBy), the filter was wrapped as an OUTER Where instead, applied AFTER the window,
/// so the caller silently lost its own visible/own-tenant rows (Take(2).Where -> empty; Skip(1).Where ->
/// off-by-one; DistinctBy(key).OrderBy -> a whole key vanishes when its lowest-PK row is filtered out).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GlobalFilterBuriedWindowTests
{
    [Table("GfbwOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public int Amount { get; set; }
        public bool IsDeleted { get; set; }
    }

    private static DbContext Make(SqliteConnection cn, string seed)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE GfbwOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Amount INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);" +
                seed;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Order>().HasKey(o => o.Id) };
        opts.AddGlobalFilter<Order>(o => !o.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    // Rows 1,2 soft-deleted; 3,4,5 visible.
    private const string PageSeed =
        "INSERT INTO GfbwOrder VALUES (1,0,5,1),(2,0,5,1),(3,0,5,0),(4,0,5,0),(5,0,5,0);";

    [Fact]
    public async Task TopLevel_take_filters_before_window()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Make(cn, PageSeed);
        var ids = (await ctx.Query<Order>().OrderBy(o => o.Id).Take(2).ToListAsync()).Select(o => o.Id).ToArray();
        Assert.Equal(new[] { 3, 4 }, ids);   // filter before Take: first 2 visible
    }

    [Fact]
    public async Task Take_then_where_filters_before_window()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Make(cn, PageSeed);
        var ids = (await ctx.Query<Order>().OrderBy(o => o.Id).Take(2).Where(o => o.Amount >= 0).ToListAsync())
            .Select(o => o.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 3, 4 }, ids);   // was []: Take grabbed soft-deleted 1,2, then filter erased both
    }

    [Fact]
    public async Task Skip_then_where_filters_before_window()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var ctx = Make(cn, PageSeed);
        var ids = (await ctx.Query<Order>().OrderBy(o => o.Id).Skip(1).Where(o => o.Amount >= 0).ToListAsync())
            .Select(o => o.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 4, 5 }, ids);   // filter first (3,4,5) then Skip(1) => 4,5; was [3,4,5]
    }

    [Fact]
    public async Task DistinctBy_then_orderby_filters_before_dedup()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        // cust 100: lowest-PK row (1) soft-deleted, visible sibling (2); cust 200: single visible (3).
        using var ctx = Make(cn, "INSERT INTO GfbwOrder VALUES (1,100,5,1),(2,100,7,0),(3,200,9,0);");
        var ids = (await ctx.Query<Order>().DistinctBy(o => o.CustomerId).OrderBy(o => o.Id).ToListAsync())
            .Select(o => o.Id).ToArray();
        Assert.Equal(new[] { 2, 3 }, ids);   // was [3]: dedup picked deleted row 1 for cust100, filter dropped it
    }
}
