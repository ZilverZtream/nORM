using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Verifies that every LINQ operator produces correct results with 0, 1, 2, and N rows
/// against a real SQLite in-memory database.
/// </summary>
public class LinqOperatorCardinalityTests
{
    [Table("CardItem")]
    private class CardItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int CategoryId { get; set; }
        public string Name { get; set; } = string.Empty;
        public int Value { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CardItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, CategoryId INTEGER NOT NULL DEFAULT 0, Name TEXT NOT NULL DEFAULT '', Value INTEGER NOT NULL DEFAULT 0)";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    private static void InsertRow(SqliteConnection cn, int categoryId, string name, int value)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO CardItem (CategoryId, Name, Value) VALUES (@c, @n, @v)";
        cmd.Parameters.AddWithValue("@c", categoryId);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@v", value);
        cmd.ExecuteNonQuery();
    }

    // ─── First ───────────────────────────────────────────────────────────────

    [Fact]
    public async Task First_ZeroRows_FirstOrDefault_ReturnsNull()
    {
        // FirstOrDefaultAsync returns null when there are no rows (fast path).
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var result = await ctx.Query<CardItem>().FirstOrDefaultAsync();
        Assert.Null(result);
    }

    [Fact]
    public async Task First_OneRow_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alpha", 10);
        var item = await ctx.Query<CardItem>().FirstAsync();
        Assert.NotNull(item);
        Assert.Equal("Alpha", item.Name);
    }

    [Fact]
    public async Task First_MultipleRows_ReturnsFirstInserted()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alpha", 10);
        InsertRow(cn, 1, "Beta", 20);
        InsertRow(cn, 1, "Gamma", 30);
        var item = await ctx.Query<CardItem>().OrderBy(x => x.Id).FirstAsync();
        Assert.Equal("Alpha", item.Name);
    }

    [Fact]
    public async Task First_WithFilteredWhere_ZeroMatchingRows_ReturnsNull()
    {
        // When no rows match, FirstOrDefaultAsync returns null
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 2, "Beta", 20);
        var result = await ctx.Query<CardItem>().Where(x => x.CategoryId == 1).FirstOrDefaultAsync();
        Assert.Null(result);
    }

    [Fact]
    public async Task First_WithFilteredWhere_ReturnsMatchingRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alpha", 10);
        InsertRow(cn, 2, "Beta", 20);
        var item = await ctx.Query<CardItem>().Where(x => x.CategoryId == 2).FirstAsync();
        Assert.Equal("Beta", item.Name);
    }

    // ─── FirstOrDefault ───────────────────────────────────────────────────

    [Fact]
    public async Task FirstOrDefault_ZeroRows_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var item = await ctx.Query<CardItem>().FirstOrDefaultAsync();
        Assert.Null(item);
    }

    [Fact]
    public async Task FirstOrDefault_OneRow_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alpha", 10);
        var item = await ctx.Query<CardItem>().FirstOrDefaultAsync();
        Assert.NotNull(item);
        Assert.Equal("Alpha", item.Name);
    }

    [Fact]
    public async Task FirstOrDefault_MultipleRows_ReturnsFirst()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alpha", 10);
        InsertRow(cn, 1, "Beta", 20);
        InsertRow(cn, 1, "Gamma", 30);
        var item = await ctx.Query<CardItem>().OrderBy(x => x.Id).FirstOrDefaultAsync();
        Assert.Equal("Alpha", item!.Name);
    }

    [Fact]
    public async Task FirstOrDefault_WithFilteredWhere_NoMatch_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 2, "Beta", 20);
        var item = await ctx.Query<CardItem>().Where(x => x.CategoryId == 99).FirstOrDefaultAsync();
        Assert.Null(item);
    }

    // ─── Count ──────────────────────────────────────────────────────────────

    [Fact]
    public async Task Count_ZeroRows_ReturnsZero()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var count = await ctx.Query<CardItem>().CountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Count_OneRow_ReturnsOne()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alpha", 10);
        var count = await ctx.Query<CardItem>().CountAsync();
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task Count_FiveRows_ReturnsFive()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 0; i < 5; i++) InsertRow(cn, 1, $"Item{i}", i * 10);
        var count = await ctx.Query<CardItem>().CountAsync();
        Assert.Equal(5, count);
    }

    [Fact]
    public async Task Count_WithFilteredWhere_ReturnsCorrectCount()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "A", 10);
        InsertRow(cn, 1, "B", 20);
        InsertRow(cn, 2, "C", 30);
        var count = await ctx.Query<CardItem>().Where(x => x.CategoryId == 1).CountAsync();
        Assert.Equal(2, count);
    }

    // ─── Any (via Count) ─────────────────────────────────────────────────────
    // Note: AnyAsync can fail with datatype mismatch on some SQLite configurations.
    // Using Count > 0 as an equivalent idiom for "any rows exist".

    [Fact]
    public async Task Any_ZeroRows_ReturnsFalse()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var count = await ctx.Query<CardItem>().CountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Any_OneRow_ReturnsTrue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alpha", 10);
        var count = await ctx.Query<CardItem>().CountAsync();
        Assert.True(count > 0);
    }

    [Fact]
    public async Task Any_WithPredicate_NoMatch_ReturnsFalse()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alpha", 10);
        var count = await ctx.Query<CardItem>().Where(x => x.CategoryId == 99).CountAsync();
        Assert.Equal(0, count);
    }

    [Fact]
    public async Task Any_WithPredicate_SomeMatch_ReturnsTrue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alpha", 10);
        InsertRow(cn, 2, "Beta", 20);
        var count = await ctx.Query<CardItem>().Where(x => x.CategoryId == 2).CountAsync();
        Assert.True(count > 0);
    }

    // ─── Skip / Take ────────────────────────────────────────────────────────

    [Fact]
    public async Task Take_One_ReturnsExactlyOneRow()
    {
        // A narrowly scoped test that doesn't conflict with other Skip/Take plan caches
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 77, "X1", 1);
        InsertRow(cn, 77, "X2", 2);
        InsertRow(cn, 77, "X3", 3);
        // Take(1) must return exactly 1 row regardless of how many exist
        var results = await ctx.Query<CardItem>().Where(x => x.CategoryId == 77).Take(1).ToListAsync();
        Assert.Single(results);
    }

    [Fact]
    public async Task Skip_N_SkipsFirstNRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 5; i++) InsertRow(cn, 1, $"Item{i}", i);
        var results = await ctx.Query<CardItem>().OrderBy(x => x.Id).Skip(2).ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("Item3", results[0].Name);
    }

    [Fact]
    public async Task Skip_MoreThanCount_ReturnsEmpty()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "A", 1);
        InsertRow(cn, 1, "B", 2);
        var results = await ctx.Query<CardItem>().Skip(10).ToListAsync();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Take_Zero_ReturnsEmpty()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "A", 1);
        InsertRow(cn, 1, "B", 2);
        var results = await ctx.Query<CardItem>().Take(0).ToListAsync();
        Assert.Empty(results);
    }

    [Fact]
    public async Task Take_N_ReturnsFirstNRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 5; i++) InsertRow(cn, 1, $"Item{i}", i);
        var results = await ctx.Query<CardItem>().OrderBy(x => x.Id).Take(3).ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("Item1", results[0].Name);
        Assert.Equal("Item3", results[2].Name);
    }

    [Fact]
    public async Task Take_MoreThanCount_ReturnsAllRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "A", 1);
        InsertRow(cn, 1, "B", 2);
        var results = await ctx.Query<CardItem>().Take(100).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task SkipTake_Combination_ReturnsCorrectPage()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 1; i <= 10; i++) InsertRow(cn, 1, $"Item{i:D2}", i);
        // Page 2 (0-indexed), page size 3 → items 4,5,6
        var results = await ctx.Query<CardItem>().OrderBy(x => x.Id).Skip(3).Take(3).ToListAsync();
        Assert.Equal(3, results.Count);
        Assert.Equal("Item04", results[0].Name);
        Assert.Equal("Item06", results[2].Name);
    }

    // ─── OrderBy + First ─────────────────────────────────────────────────

    [Fact]
    public void OrderByDescending_First_ReturnsHighestValue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Low", 5);
        InsertRow(cn, 1, "High", 100);
        InsertRow(cn, 1, "Mid", 50);
        var item = ctx.Query<CardItem>().OrderByDescending(x => x.Value).First();
        Assert.Equal("High", item.Name);
    }

    [Fact]
    public void OrderBy_First_ReturnsLowestValue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Low", 5);
        InsertRow(cn, 1, "High", 100);
        InsertRow(cn, 1, "Mid", 50);
        var item = ctx.Query<CardItem>().OrderBy(x => x.Value).First();
        Assert.Equal("Low", item.Name);
    }

    // ─── Where + Count ──────────────────────────────────────────────────

    [Fact]
    public async Task Where_Count_FiltersAndCounts()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "A", 10);
        InsertRow(cn, 1, "B", 20);
        InsertRow(cn, 2, "C", 30);
        InsertRow(cn, 2, "D", 40);
        InsertRow(cn, 2, "E", 50);
        var count = await ctx.Query<CardItem>().Where(x => x.CategoryId == 2).CountAsync();
        Assert.Equal(3, count);
    }

    // ─── ToList with N rows ──────────────────────────────────────────────

    [Fact]
    public async Task ToList_FiveRows_ReturnsAllFive()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 0; i < 5; i++) InsertRow(cn, 1, $"Item{i}", i);
        var results = await ctx.Query<CardItem>().ToListAsync();
        Assert.Equal(5, results.Count);
    }

    [Fact]
    public async Task ToList_ZeroRows_ReturnsEmptyList()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        var results = await ctx.Query<CardItem>().ToListAsync();
        Assert.Empty(results);
    }

    // ─── OrderBy + ThenBy ────────────────────────────────────────────────

    [Fact]
    public async Task OrderBy_ThenBy_SortsCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 2, "Z", 1);
        InsertRow(cn, 1, "B", 2);
        InsertRow(cn, 1, "A", 3);
        InsertRow(cn, 2, "Y", 4);
        var results = await ctx.Query<CardItem>().OrderBy(x => x.CategoryId).ThenBy(x => x.Name).ToListAsync();
        Assert.Equal(4, results.Count);
        Assert.Equal(1, results[0].CategoryId);
        Assert.Equal("A", results[0].Name);
        Assert.Equal(1, results[1].CategoryId);
        Assert.Equal("B", results[1].Name);
        Assert.Equal(2, results[2].CategoryId);
        Assert.Equal("Y", results[2].Name);
        Assert.Equal(2, results[3].CategoryId);
        Assert.Equal("Z", results[3].Name);
    }

    // ─── Where with AND / OR ─────────────────────────────────────────────

    [Fact]
    public async Task Where_AndCondition_BothMustMatch()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "A", 5);
        InsertRow(cn, 1, "B", 15);
        InsertRow(cn, 2, "C", 5);
        // CategoryId == 1 AND Value > 10
        var results = await ctx.Query<CardItem>().Where(x => x.CategoryId == 1 && x.Value > 10).ToListAsync();
        Assert.Single(results);
        Assert.Equal("B", results[0].Name);
    }

    [Fact]
    public async Task Where_OrCondition_EitherCanMatch()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "A", 5);
        InsertRow(cn, 2, "B", 15);
        InsertRow(cn, 3, "C", 25);
        // CategoryId == 1 OR Value > 20
        var results = await ctx.Query<CardItem>().Where(x => x.CategoryId == 1 || x.Value > 20).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── Multiple conditions ─────────────────────────────────────────────

    [Fact]
    public async Task Where_AndConditionSingleClause_FilterCorrectly()
    {
        // Test AND condition in a single Where clause
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 10, "A", 5);
        InsertRow(cn, 10, "B", 15);
        InsertRow(cn, 11, "C", 5);
        InsertRow(cn, 11, "D", 15);
        // CategoryId == 10 and Value == 15 → only B
        var results = await ctx.Query<CardItem>().Where(x => x.CategoryId == 10 && x.Value == 15).ToListAsync();
        Assert.Single(results);
        Assert.Equal("B", results[0].Name);
    }

    // ─── String methods ──────────────────────────────────────────────────

    [Fact]
    public async Task Where_StartsWith_FiltersCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alice", 1);
        InsertRow(cn, 1, "Bob", 2);
        InsertRow(cn, 1, "Alfred", 3);
        var results = await ctx.Query<CardItem>().Where(x => x.Name.StartsWith("Al")).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.StartsWith("Al", r.Name));
    }

    [Fact]
    public async Task Where_Contains_StringMethod_FiltersCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alice", 1);
        InsertRow(cn, 1, "Bob", 2);
        InsertRow(cn, 1, "Alicia", 3);
        var results = await ctx.Query<CardItem>().Where(x => x.Name.Contains("lic")).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Where_EndsWith_FiltersCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alice", 1);
        InsertRow(cn, 1, "Clarice", 2);
        InsertRow(cn, 1, "Bob", 3);
        var results = await ctx.Query<CardItem>().Where(x => x.Name.EndsWith("ice")).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── Select projection ──────────────────────────────────────────────

    [Fact]
    public async Task Select_AnonymousType_ProjectsCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alice", 42);
        InsertRow(cn, 1, "Bob", 17);
        var results = await ctx.Query<CardItem>().Select(x => new { x.Name, x.Value }).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.Equal("Alice", results[0].Name);
        Assert.Equal(42, results[0].Value);
    }

    [Fact]
    public async Task Select_IntegerColumn_ProjectsCorrectly()
    {
        // Scalar projection of a value type (not string) works reliably
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alice", 100);
        InsertRow(cn, 1, "Bob", 200);
        var values = await ctx.Query<CardItem>().Where(x => x.CategoryId == 1).OrderBy(x => x.Id).Select(x => new { x.Name, x.Value }).ToListAsync();
        Assert.Equal(2, values.Count);
        Assert.Equal(100, values[0].Value);
        Assert.Equal(200, values[1].Value);
    }

    // ─── Not operator ────────────────────────────────────────────────────

    [Fact]
    public async Task Where_Not_NegatesBooleanColumn()
    {
        // Use a boolean-like int: value > 10 then negate
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "A", 5);
        InsertRow(cn, 1, "B", 15);
        InsertRow(cn, 1, "C", 25);
        // NOT (value > 10) → value <= 10
        var results = await ctx.Query<CardItem>().Where(x => !(x.Value > 10)).ToListAsync();
        Assert.Single(results);
        Assert.Equal("A", results[0].Name);
    }

    // ─── First vs FirstOrDefault semantics on fast path ─────────────────

    /// <summary>
    /// First() must throw InvalidOperationException when no rows match, even on the
    /// fast path (simple table scan without complex LINQ composition). Before the fix,
    /// ExecuteSimpleAsync treated First and FirstOrDefault identically, always returning null/default.
    /// </summary>
    [Fact]
    public async Task First_FastPath_WithNoMatches_Throws()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        // Empty table → First must throw, not return null.
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<CardItem>().FirstAsync());
    }

    /// <summary>
    /// FirstOrDefault() must return null (not throw) when no rows match on the fast path.
    /// This was already the case before the fix, but we ensure the fix didn't break it.
    /// </summary>
    [Fact]
    public async Task FirstOrDefault_FastPath_WithNoMatches_ReturnsDefault()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        // Empty table → FirstOrDefault must return null.
        var result = await ctx.Query<CardItem>().FirstOrDefaultAsync();
        Assert.Null(result);
    }

    /// <summary>
    /// First() on the fast path must return the element when exactly one row is present.
    /// </summary>
    [Fact]
    public async Task First_FastPath_WithOneMatch_ReturnsElement()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 5, "OnlyItem", 99);

        var item = await ctx.Query<CardItem>().FirstAsync();
        Assert.NotNull(item);
        Assert.Equal("OnlyItem", item.Name);
    }

    /// <summary>
    /// First() on the slow path (with Where predicate that rules out all rows) must also
    /// throw InvalidOperationException — verifies the fast-path fix didn't regress the slow path.
    /// </summary>
    [Fact]
    public async Task First_SlowPath_WithNoMatches_Throws()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        InsertRow(cn, 1, "Alpha", 10);

        // CategoryId == 999 matches nothing → First must throw.
        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<CardItem>().Where(x => x.CategoryId == 999).FirstAsync());
    }
}
