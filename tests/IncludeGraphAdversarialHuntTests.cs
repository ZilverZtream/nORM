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
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Adversarial hunt for SILENT-WRONG Include / ThenInclude eager-loading results on SQLite:
/// wrong/missing/duplicated/mis-associated children in the loaded object graph. Each test asserts
/// the graph against a hand-built oracle from the seed data.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class IncludeGraphAdversarialHuntTests
{
    // ─────────────────────────────── converters ───────────────────────────────
    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -Convert.ToInt32(v);
    }

    // ─────────────────────────────── entities ─────────────────────────────────
    [Table("IghCustomer")]
    public sealed class Customer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Order> Orders { get; set; } = new();
        public List<Phone> Phones { get; set; } = new();
    }

    [Table("IghOrder")]
    public sealed class Order
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public decimal Amount { get; set; }
        public int Score { get; set; }            // stored negated via converter
        public List<Line> Lines { get; set; } = new();
    }

    [Table("IghLine")]
    public sealed class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
    }

    [Table("IghPhone")]
    public sealed class Phone
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public string Number { get; set; } = "";
    }

    private static DbContext Ctx(out SqliteConnection cn, bool converter = false)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE IghCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE IghOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Amount TEXT NOT NULL, Score INTEGER NOT NULL);
                CREATE TABLE IghLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL);
                CREATE TABLE IghPhone (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Number TEXT NOT NULL);
                -- Customers 1..4
                INSERT INTO IghCustomer VALUES (1,'c1'),(2,'c2'),(3,'c3'),(4,'c4');
                -- Orders: c1 has 2 (amount 50,150), c2 has 0, c3 has 1 (amount 300), c4 has 1 (amount 20)
                INSERT INTO IghOrder VALUES (10,1,'50.00',3),(11,1,'150.00',7),(30,3,'300.00',1),(40,4,'20.00',5);
                -- Lines: order 10 -> 2 lines; order 11 -> 0 lines; order 30 -> 1 line; order 40 -> 1 line
                INSERT INTO IghLine VALUES (100,10,'a'),(101,10,'b'),(300,30,'z'),(400,40,'q');
                -- Phones: c1 has 0, c2 has 1, c3 has 0, c4 has 2
                INSERT INTO IghPhone VALUES (500,2,'p2'),(600,4,'p4a'),(601,4,'p4b');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Customer>().HasKey(c => c.Id);
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Phone>().HasKey(p => p.Id);
                if (converter)
                    mb.Entity<Order>().Property(o => o.Score).HasConversion(new NegatingConverter());
                mb.Entity<Customer>().HasMany(c => c.Orders).WithOne().HasForeignKey(o => o.CustomerId, c => c.Id);
                mb.Entity<Customer>().HasMany(c => c.Phones).WithOne().HasForeignKey(p => p.CustomerId, c => c.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    // ══════════════════════════════ CLEAN-BILL tests ══════════════════════════════

    // ── 1. Two collection Includes on one root: no cross-contamination, empty vs populated ──
    [Fact]
    public void TwoCollectionIncludes_NoCrossContamination()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var custs = ctx.Query<Customer>()
            .Include(c => c.Orders)
            .Include(c => c.Phones)
            .ToList().OrderBy(c => c.Id).ToList();

        Assert.Equal(4, custs.Count);
        Assert.Equal(new[] { 10, 11 }, custs[0].Orders.Select(o => o.Id).OrderBy(i => i));
        Assert.Empty(custs[0].Phones);
        Assert.Empty(custs[1].Orders);
        Assert.Equal(new[] { 500 }, custs[1].Phones.Select(p => p.Id));
        Assert.Equal(new[] { 30 }, custs[2].Orders.Select(o => o.Id));
        Assert.Empty(custs[2].Phones);
        Assert.Equal(new[] { 40 }, custs[3].Orders.Select(o => o.Id));
        Assert.Equal(new[] { 600, 601 }, custs[3].Phones.Select(p => p.Id).OrderBy(i => i));
    }

    // ── 2. Collection Include + root OrderBy + Skip + Take: children of the PAGED roots only ──
    [Fact]
    public void CollectionInclude_RootPaging_LoadsChildrenForPagedRootsOnly()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var custs = ctx.Query<Customer>()
            .Include(c => c.Orders)
            .OrderBy(c => c.Id).Skip(1).Take(2)
            .ToList();

        Assert.Equal(new[] { 2, 3 }, custs.Select(c => c.Id));
        Assert.Empty(custs[0].Orders);
        Assert.Equal(new[] { 30 }, custs[1].Orders.Select(o => o.Id));
    }

    // ── 3. ThenInclude collection->collection: a middle child (order 11) has ZERO grandchildren ──
    [Fact]
    public void ThenInclude_MiddleChildWithZeroGrandchildren_Correct()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var custs = ctx.Query<Customer>()
            .Include(c => c.Orders).ThenInclude(o => o.Lines)
            .ToList().OrderBy(c => c.Id).ToList();

        var c1 = custs[0];
        var order10 = c1.Orders.Single(o => o.Id == 10);
        var order11 = c1.Orders.Single(o => o.Id == 11);
        Assert.Equal(new[] { 100, 101 }, order10.Lines.Select(l => l.Id).OrderBy(i => i));
        Assert.Empty(order11.Lines);
        Assert.Equal(new[] { 300 }, custs[2].Orders.Single().Lines.Select(l => l.Id));
    }

    // ── 4. Collection Include child with a VALUE-CONVERTER column: ConvertFromProvider IS applied ──
    [Fact]
    public void CollectionInclude_ChildConverterColumn_ConvertFromProviderApplied()
    {
        using var ctx = Ctx(out var cn, converter: true);
        using var _cn = cn;
        var c1 = ctx.Query<Customer>()
            .Include(c => c.Orders)
            .ToList().Single(c => c.Id == 1);

        // Stored Score is 3,7 (raw seed). ConvertFromProvider(v) => -v, so the model values MUST be -3,-7.
        // Seeing 3,7 would mean the converter was skipped on the Include child. Seeing -3,-7 proves it applied.
        Assert.Equal(new[] { -3, -7 }, c1.Orders.OrderBy(o => o.Id).Select(o => o.Score));
        Assert.Equal(new[] { 50.00m, 150.00m }, c1.Orders.OrderBy(o => o.Id).Select(o => o.Amount));
    }

    // ── 5. Collection Include + root Where using nav Any(): main-path decimal comparison is numeric ──
    [Fact]
    public void CollectionInclude_RootWhereOnNavAny_IncludesAllChildren()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // Main query path CASTs the decimal-as-TEXT column to REAL: amount > 100 => c1(150) and c3(300).
        var custs = ctx.Query<Customer>()
            .Include(c => c.Orders)
            .Where(c => c.Orders.Any(o => o.Amount > 100m))
            .ToList().OrderBy(c => c.Id).ToList();

        Assert.Equal(new[] { 1, 3 }, custs.Select(c => c.Id));
        Assert.Equal(new[] { 10, 11 }, custs[0].Orders.Select(o => o.Id).OrderBy(i => i));
        Assert.Equal(new[] { 30 }, custs[1].Orders.Select(o => o.Id));
    }

    // ── 6. Ordered/top-N Include on a decimal key orders NUMERICALLY (not lexically) ──
    [Fact]
    public void OrderedInclude_DecimalKey_OrdersNumerically()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var c1 = ctx.Query<Customer>()
            .Include(c => c.Orders.OrderByDescending(o => o.Amount))
            .ToList().Single(c => c.Id == 1);

        // Numeric desc: 150 (id 11) then 50 (id 10). Lexical desc of '50.00','150.00' would give 10,11.
        Assert.Equal(new[] { 11, 10 }, c1.Orders.Select(o => o.Id));
    }

    // ── 7. Include collection with an explicit AsSplitQuery is identical to the default ──
    [Fact]
    public void CollectionInclude_SplitAndDefault_SameGraph()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var def = ctx.Query<Customer>().Include(c => c.Orders)
            .ToList().OrderBy(c => c.Id)
            .Select(c => (c.Id, c.Orders.Select(o => o.Id).OrderBy(i => i).ToArray())).ToList();
        var split = ((INormQueryable<Customer>)ctx.Query<Customer>()).AsSplitQuery().Include(c => c.Orders)
            .ToList().OrderBy(c => c.Id)
            .Select(c => (c.Id, c.Orders.Select(o => o.Id).OrderBy(i => i).ToArray())).ToList();

        Assert.Equal(def.Count, split.Count);
        for (int i = 0; i < def.Count; i++)
        {
            Assert.Equal(def[i].Id, split[i].Id);
            Assert.Equal(def[i].Item2, split[i].Item2);
        }
    }

    // ── 8. Filtered Include on a NON-decimal (int) column: parent kept with an empty collection ──
    [Fact]
    public void FilteredInclude_IntColumn_ParentKeptWithEmptyCollection()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        // No order has Id > 1000, so every customer must be returned with an EMPTY Orders collection.
        var custs = ctx.Query<Customer>()
            .Include(c => c.Orders.Where(o => o.Id > 1000))
            .ToList().OrderBy(c => c.Id).ToList();

        Assert.Equal(new[] { 1, 2, 3, 4 }, custs.Select(c => c.Id));
        Assert.All(custs, c => Assert.Empty(c.Orders));
    }

    // ═══════════════════════ SILENT-WRONG repro (nORM-seeded, airtight) ═══════════════════════

    [Table("IghDecCustomer")]
    public sealed class DecCustomer
    {
        [Key] public int Id { get; set; }
        public List<DecOrder> Orders { get; set; } = new();
    }

    [Table("IghDecOrder")]
    public sealed class DecOrder
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public decimal Amount { get; set; }
    }

    private static async Task<DbContext> DecCtxAsync(SqliteConnection cn)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE IghDecCustomer (Id INTEGER PRIMARY KEY);
                CREATE TABLE IghDecOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Amount TEXT NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<DecCustomer>().HasKey(c => c.Id);
                mb.Entity<DecOrder>().HasKey(o => o.Id);
                mb.Entity<DecCustomer>().HasMany(c => c.Orders).WithOne().HasForeignKey(o => o.CustomerId, c => c.Id);
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        // Seed via nORM so the decimal column is stored in nORM's OWN canonical TEXT format (no hand-written
        // format that could be blamed). Amounts 9 and 100 diverge maximally between numeric and lexical order:
        // numeric 9 < 100, but lexical '9' > '100'.
        await ctx.InsertAsync(new DecCustomer { Id = 1 });
        await ctx.InsertAsync(new DecOrder { Id = 1, CustomerId = 1, Amount = 9m });
        await ctx.InsertAsync(new DecOrder { Id = 2, CustomerId = 1, Amount = 100m });
        return ctx;
    }

    /// <summary>
    /// CONTROL (must PASS): the main query path compares the decimal-as-TEXT column NUMERICALLY.
    /// Amount > 50 selects only order 2 (100), never order 1 (9).
    /// </summary>
    [Fact]
    public async Task RootWhere_DecimalColumn_ComparesNumerically_Control()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await DecCtxAsync(cn);
        var ids = (await ctx.Query<DecOrder>()
            .Where(o => o.Amount > 50m)
            .ToListAsync()).Select(o => o.Id).OrderBy(i => i).ToArray();

        Assert.Equal(new[] { 2 }, ids);   // numeric: 100 > 50, 9 is not
    }

    /// <summary>
    /// SILENT-WRONG (currently FAILS): a filtered Include compares the decimal-as-TEXT column
    /// LEXICOGRAPHICALLY instead of numerically. Expected {order 2 (amount 100)}; actual is {order 1 (amount 9)}
    /// because '9' > '50' lexically while '100' is not. The filtered-Include predicate rendering
    /// (SelectClauseVisitor.RenderNavigationFilterBody / RenderFilterSide) never wraps the decimal column in
    /// the provider's NormalizeDecimalForCompare (CAST AS REAL) the main comparison paths apply.
    /// </summary>
    [Fact]
    public async Task FilteredInclude_DecimalColumn_ComparesNumerically()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await DecCtxAsync(cn);
        var c1 = (await ((INormQueryable<DecCustomer>)ctx.Query<DecCustomer>())
            .Include(c => c.Orders.Where(o => o.Amount > 50m))
            .ToListAsync()).Single(c => c.Id == 1);

        // The filtered navigation must match the SAME rows the main WHERE would: only order 2 (amount 100).
        Assert.Equal(new[] { 2 }, c1.Orders.Select(o => o.Id).OrderBy(i => i).ToArray());
    }

    /// <summary>
    /// SILENT-WRONG (currently FAILS): filtered-Include decimal EQUALITY is also lexical. Order 2 has amount
    /// exactly 100; a filter `== 100m` must match it. nORM stores decimals as canonical TEXT and its main-path
    /// equality uses an exact-decimal key, but this navigation-filter grammar renders a bare `col = 100`,
    /// which on a TEXT column is a text compare that misses any non-identical stored form.
    /// </summary>
    [Fact]
    public async Task FilteredInclude_DecimalColumn_EqualityMatches()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await DecCtxAsync(cn);
        var c1 = (await ((INormQueryable<DecCustomer>)ctx.Query<DecCustomer>())
            .Include(c => c.Orders.Where(o => o.Amount == 100m))
            .ToListAsync()).Single(c => c.Id == 1);

        Assert.Equal(new[] { 2 }, c1.Orders.Select(o => o.Id).OrderBy(i => i).ToArray());
    }

    /// <summary>
    /// BLAST-RADIUS probe: a projection navigation-aggregate with a decimal Where uses the SAME
    /// RenderNavigationFilterBody grammar as filtered Include. Count of orders with Amount &gt; 95 must be 1
    /// (only order 2, amount 100). A lexical compare ('9' &gt; '95' false, '100' &gt; '95' false) yields 0.
    /// </summary>
    [Fact]
    public async Task ProjectionNavAggregate_DecimalFilter_ComparesNumerically()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await DecCtxAsync(cn);
        var counts = ctx.Query<DecCustomer>()
            .Select(c => c.Orders.Count(o => o.Amount > 95m))
            .ToList().Cast<int>().ToArray();

        Assert.Equal(new[] { 1 }, counts);
    }
}
