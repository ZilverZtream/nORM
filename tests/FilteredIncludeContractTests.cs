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
/// Pins filtered Include (EF Core 5+): <c>Include(o =&gt; o.Lines.Where(pred))</c> eager-loads only the
/// children matching the predicate, and <c>ThenInclude(l =&gt; l.SubLines.Where(pred))</c> filters the next
/// level. The predicate renders to SQL against the eager-load level's alias; a closure capture flows through
/// the compiled-parameter channel so it re-binds per execution instead of freezing into the cached plan.
/// A filtered Include on a many-to-many navigation fails loud (its join-table load path can't yet apply the
/// predicate) rather than silently returning the whole set.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class FilteredIncludeContractTests
{
    [Table("FiOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public string Code { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("FiLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public bool Active { get; set; }
        public string Sku { get; set; } = "";
        public List<SubLine> SubLines { get; set; } = new();
    }

    [Table("FiSubLine")]
    public class SubLine
    {
        [Key] public int Id { get; set; }
        public int LineId { get; set; }
        public string Tag { get; set; } = "";
    }

    [Table("FiTag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
    }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE FiOrder (Id INTEGER PRIMARY KEY, Code TEXT NOT NULL);
                CREATE TABLE FiLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Active INTEGER NOT NULL, Sku TEXT NOT NULL);
                CREATE TABLE FiSubLine (Id INTEGER PRIMARY KEY, LineId INTEGER NOT NULL, Tag TEXT NOT NULL);
                CREATE TABLE FiTag (Id INTEGER PRIMARY KEY);
                CREATE TABLE FiOrderTag (OrderId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO FiOrder VALUES (1, 'o1'), (2, 'o2'), (3, 'o3');
                INSERT INTO FiLine VALUES (1, 1, 1, 'a'), (2, 1, 0, 'b'), (3, 1, 1, 'c');
                INSERT INTO FiLine VALUES (4, 2, 0, 'd');
                INSERT INTO FiSubLine VALUES (1, 1, 'x'), (2, 1, 'y'), (3, 3, 'x');
                INSERT INTO FiTag VALUES (1), (2);
                INSERT INTO FiOrderTag VALUES (1, 1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<SubLine>().HasKey(s => s.Id);
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
                mb.Entity<Line>().HasMany(l => l.SubLines).WithOne().HasForeignKey(s => s.LineId, l => l.Id);
                mb.Entity<Order>().HasMany<Tag>(o => o.Tags).WithMany().UsingTable("FiOrderTag", "OrderId", "TagId");
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    private static string Shape(Order o) =>
        $"{o.Id}:[{string.Join(",", o.Lines.OrderBy(l => l.Id).Select(l => l.Sku))}]";

    [Fact]
    public void Filtered_include_loads_only_matching_children()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var orders = ctx.Query<Order>()
            .Include(o => o.Lines.Where(l => l.Active))
            .OrderBy(o => o.Id)
            .ToList();

        Assert.Equal(new[] { "1:[a,c]", "2:[]", "3:[]" }, orders.Select(Shape).ToArray());
        // Empty matches are non-null empty collections, not null navigations.
        Assert.All(orders, o => Assert.NotNull(o.Lines));
    }

    [Fact]
    public async Task Filtered_include_loads_only_matching_children_async()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var orders = await ctx.Query<Order>()
            .Include(o => o.Lines.Where(l => l.Active))
            .OrderBy(o => o.Id)
            .ToListAsync();

        Assert.Equal(new[] { "1:[a,c]", "2:[]", "3:[]" }, orders.Select(Shape).ToArray());
    }

    [Fact]
    public void Filtered_include_composes_with_conjunction()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var orders = ctx.Query<Order>()
            .Include(o => o.Lines.Where(l => l.Active && l.Sku == "c"))
            .OrderBy(o => o.Id)
            .ToList();

        Assert.Equal(new[] { "1:[c]", "2:[]", "3:[]" }, orders.Select(Shape).ToArray());
    }

    [Fact]
    public void Filtered_include_rebinds_closure_values_across_cache_hits()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        string[] RunFor(string sku)
        {
            var orders = ctx.Query<Order>()
                .Include(o => o.Lines.Where(l => l.Sku == sku))
                .OrderBy(o => o.Id)
                .ToList();
            return orders.Select(Shape).ToArray();
        }

        // Same query shape twice with different closure values — the second execution reuses the cached
        // plan and must re-bind the new value, not replay the first run's frozen literal.
        var first = RunFor("a");
        var second = RunFor("c");

        Assert.Equal(new[] { "1:[a]", "2:[]", "3:[]" }, first);
        Assert.Equal(new[] { "1:[c]", "2:[]", "3:[]" }, second);
    }

    [Fact]
    public void Then_include_filter_applies_at_the_second_level()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var orders = ctx.Query<Order>()
            .Include(o => o.Lines)
            .ThenInclude(l => l.SubLines.Where(s => s.Tag == "x"))
            .OrderBy(o => o.Id)
            .ToList();

        // Every line loads (unfiltered first level), but each line's SubLines keep only Tag == "x".
        var order1 = orders.Single(o => o.Id == 1);
        var line1 = order1.Lines.Single(l => l.Id == 1);
        Assert.Equal(new[] { "x" }, line1.SubLines.Select(s => s.Tag).ToArray());
        var line2 = order1.Lines.Single(l => l.Id == 2);
        Assert.Empty(line2.SubLines);
    }

    [Fact]
    public void Filtered_include_and_then_include_filter_compose()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var orders = ctx.Query<Order>()
            .Include(o => o.Lines.Where(l => l.Active))
            .ThenInclude(l => l.SubLines.Where(s => s.Tag == "x"))
            .OrderBy(o => o.Id)
            .ToList();

        var order1 = orders.Single(o => o.Id == 1);
        // Only active lines (a, c) survive the first filter; the inactive 'b' is gone.
        Assert.Equal(new[] { "a", "c" }, order1.Lines.OrderBy(l => l.Id).Select(l => l.Sku).ToArray());
        // Line 1 (Sku a) keeps only its Tag=="x" sub-line; Line 3 (Sku c) keeps its single Tag=="x".
        Assert.Equal(new[] { "x" }, order1.Lines.Single(l => l.Sku == "a").SubLines.Select(s => s.Tag).ToArray());
        Assert.Equal(new[] { "x" }, order1.Lines.Single(l => l.Sku == "c").SubLines.Select(s => s.Tag).ToArray());
    }

    [Fact]
    public void Bare_include_still_returns_the_whole_set()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        var orders = ctx.Query<Order>()
            .Include(o => o.Lines)
            .OrderBy(o => o.Id)
            .ToList();

        Assert.Equal(new[] { "1:[a,b,c]", "2:[d]", "3:[]" }, orders.Select(Shape).ToArray());
    }

    [Fact]
    public void Filtered_include_on_many_to_many_fails_loud()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Bootstrap(cn);

        // A filtered Include on an m2m navigation must fail loud rather than silently ignore the
        // predicate and return the whole set — the join-table load path can't apply it yet.
        var ex = Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>()
               .Include(o => o.Tags.Where(t => t.Id == 1))
               .ToList());
        Assert.Contains("many-to-many", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
