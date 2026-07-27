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
/// A collection-navigation <c>Sum(...)</c> inside a WHERE predicate must return 0 for an empty child
/// collection, exactly as <see cref="System.Linq.Enumerable.Sum(System.Collections.Generic.IEnumerable{int})"/>
/// does. SQL <c>SUM</c> over no rows is NULL, so a bare <c>SUM(...)</c> makes a predicate like
/// <c>p.Children.Sum(c =&gt; c.Price) == 0</c> evaluate as <c>NULL == 0</c> (UNKNOWN) and the parent with no
/// children is silently dropped — a row-loss bug on a query that "succeeds". The aggregate must be
/// <c>COALESCE(SUM(...), 0)</c>, mirroring the projection nav-aggregate path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class WhereCollectionSumEmptyNullSemanticsTests
{
    [Table("WcsParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [Table("WcsChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Price { get; set; }
        public decimal Cost { get; set; }
    }

    // Parent 1 has one child (Price=7, Cost=7.50); Parent 2 has NO children (empty collection → Sum should be 0).
    private static DbContext CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE WcsParent (Id INTEGER PRIMARY KEY);
                CREATE TABLE WcsChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Price INTEGER NOT NULL, Cost TEXT NOT NULL);
                INSERT INTO WcsParent (Id) VALUES (1), (2);
                INSERT INTO WcsChild (Id, ParentId, Price, Cost) VALUES (1, 1, 7, '7.50');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    // Empty-collection parent (Id=2): Sum == 0, so it must be included; Parent 1 (Sum=7) excluded.
    [Fact]
    public void Where_int_collection_sum_equals_zero_includes_empty_collection_parent()
    {
        using var ctx = CreateDb();
        var actual = ctx.Query<Parent>().Where(p => p.Children.Sum(c => c.Price) == 0).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2 }, actual);
    }

    [Fact]
    public void Where_int_collection_sum_lessThanOrEqual_zero_includes_empty_collection_parent()
    {
        using var ctx = CreateDb();
        var actual = ctx.Query<Parent>().Where(p => p.Children.Sum(c => c.Price) <= 0).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2 }, actual);
    }

    [Fact]
    public void Where_decimal_collection_sum_equals_zero_includes_empty_collection_parent()
    {
        using var ctx = CreateDb();
        var actual = ctx.Query<Parent>().Where(p => p.Children.Sum(c => c.Cost) == 0m).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2 }, actual);
    }

    // Control: Sum > 0 selects only the non-empty parent (agrees with or without the fix).
    [Fact]
    public void Where_int_collection_sum_greaterThan_zero_selects_nonempty_parent()
    {
        using var ctx = CreateDb();
        var actual = ctx.Query<Parent>().Where(p => p.Children.Sum(c => c.Price) > 0).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 1 }, actual);
    }

    // Select-then-Sum over the relation collection routes through the same emit as the direct selector.
    [Fact]
    public void Where_int_collection_select_then_sum_equals_zero_includes_empty_collection_parent()
    {
        using var ctx = CreateDb();
        var actual = ctx.Query<Parent>().Where(p => p.Children.Select(c => c.Price).Sum() == 0).Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2 }, actual);
    }

    // Explicit correlated subquery Sum (ctx.Query<Child>().Where(fk).Sum(...)) — BuildScalarAggregateSubquery.
    [Fact]
    public void Where_correlated_subquery_sum_equals_zero_includes_empty_collection_parent()
    {
        using var ctx = CreateDb();
        var actual = ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).Sum(c => c.Price) == 0)
            .Select(p => p.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2 }, actual);
    }

    [Table("WcsOwner")]
    public class Owner
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    public class Line
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public int Amount { get; set; }
    }

    // Owned (OwnsMany) collection: Owner 1 has a line (Amount=7); Owner 2 has none.
    [Fact]
    public void Where_owned_collection_sum_equals_zero_includes_empty_collection_owner()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE WcsOwner (Id INTEGER PRIMARY KEY);
                CREATE TABLE WcsLine (Id INTEGER PRIMARY KEY AUTOINCREMENT, OwnerId INTEGER NOT NULL, Amount INTEGER NOT NULL);
                INSERT INTO WcsOwner (Id) VALUES (1), (2);
                INSERT INTO WcsLine (OwnerId, Amount) VALUES (1, 7);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Owner>().HasKey(o => o.Id);
                mb.Entity<Owner>().OwnsMany<Line>(o => o.Lines, tableName: "WcsLine", foreignKey: "OwnerId");
            }
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var actual = ctx.Query<Owner>().Where(o => o.Lines.Sum(l => l.Amount) == 0).Select(o => o.Id).OrderBy(i => i).ToList();
        Assert.Equal(new[] { 2 }, actual);
    }

}
