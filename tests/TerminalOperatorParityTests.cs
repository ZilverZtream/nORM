using System;
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
/// Blocker 8 — Terminal operator parity.
/// Covers: First, FirstOrDefault, Single, SingleOrDefault, Last, LastOrDefault,
/// ElementAt, Any, All, Count, LongCount, Sum, Min, Max, Average on empty,
/// one-row, two-row, and multi-row sequences, including nullable columns.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class TerminalOperatorParityTests
{
    [Table("TerminalOpRow")]
    private class TerminalOpRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public int CategoryId { get; set; }
        public string Name { get; set; } = string.Empty;
        public int IntValue { get; set; }
        public double DoubleValue { get; set; }
        public decimal DecimalValue { get; set; }
        public int? NullableInt { get; set; }
        public double? NullableDouble { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE TerminalOpRow (
                    Id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    CategoryId    INTEGER NOT NULL DEFAULT 0,
                    Name          TEXT    NOT NULL DEFAULT '',
                    IntValue      INTEGER NOT NULL DEFAULT 0,
                    DoubleValue   REAL    NOT NULL DEFAULT 0,
                    DecimalValue  REAL    NOT NULL DEFAULT 0,
                    NullableInt   INTEGER,
                    NullableDouble REAL
                )";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void Insert(
        SqliteConnection cn,
        int categoryId,
        string name,
        int intVal,
        double dblVal = 0.0,
        decimal decVal = 0m,
        int? nullInt = null,
        double? nullDbl = null)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            INSERT INTO TerminalOpRow (CategoryId, Name, IntValue, DoubleValue, DecimalValue, NullableInt, NullableDouble)
            VALUES (@c, @n, @i, @d, @m, @ni, @nd)";
        cmd.Parameters.AddWithValue("@c", categoryId);
        cmd.Parameters.AddWithValue("@n", name);
        cmd.Parameters.AddWithValue("@i", intVal);
        cmd.Parameters.AddWithValue("@d", dblVal);
        cmd.Parameters.AddWithValue("@m", (double)decVal);
        cmd.Parameters.AddWithValue("@ni", (object?)nullInt ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@nd", (object?)nullDbl ?? DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    // ═══════════════════════════════════════════════════════════════════════
    // First / FirstOrDefault
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task First_EmptySequence_ThrowsInvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<TerminalOpRow>().FirstAsync());
    }

    [Fact]
    public async Task First_EmptySequence_WithPredicate_ThrowsInvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 10);

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<TerminalOpRow>()
                     .Where(r => r.CategoryId == 99)
                     .FirstAsync());
    }

    [Fact]
    public async Task First_OneRow_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Solo", 42);

        var row = await ctx.Query<TerminalOpRow>().FirstAsync();
        Assert.Equal("Solo", row.Name);
    }

    [Fact]
    public async Task First_MultipleRows_ReturnsFirstOrdered()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "C", 30);
        Insert(cn, 1, "A", 10);
        Insert(cn, 1, "B", 20);

        var row = await ctx.Query<TerminalOpRow>()
            .OrderBy(r => r.IntValue)
            .FirstAsync();

        Assert.Equal("A", row.Name);
    }

    [Fact]
    public async Task FirstOrDefault_EmptySequence_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await ctx.Query<TerminalOpRow>().FirstOrDefaultAsync();
        Assert.Null(result);
    }

    [Fact]
    public async Task FirstOrDefault_OneRow_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Only", 5);

        var row = await ctx.Query<TerminalOpRow>().FirstOrDefaultAsync();
        Assert.NotNull(row);
        Assert.Equal("Only", row.Name);
    }

    [Fact]
    public async Task FirstOrDefault_MultipleRows_ReturnsFirstOrdered()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Z", 99);
        Insert(cn, 1, "A",  1);
        Insert(cn, 1, "M", 50);

        var row = await ctx.Query<TerminalOpRow>()
            .OrderBy(r => r.IntValue)
            .FirstOrDefaultAsync();

        Assert.Equal("A", row!.Name);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Single / SingleOrDefault
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Single_EmptySequence_ThrowsInvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Assert.Throws<InvalidOperationException>(
            () => ctx.Query<TerminalOpRow>().Single());
    }

    [Fact]
    public void Single_OneRow_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Lone", 7);

        var row = ctx.Query<TerminalOpRow>().Single();
        Assert.Equal("Lone", row.Name);
    }

    [Fact]
    public void Single_TwoPlusRows_ThrowsInvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "First",  1);
        Insert(cn, 1, "Second", 2);

        var ex = Assert.Throws<InvalidOperationException>(
            () => ctx.Query<TerminalOpRow>().Single());

        Assert.Contains("more than one element", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void SingleOrDefault_EmptySequence_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = ctx.Query<TerminalOpRow>().SingleOrDefault();
        Assert.Null(result);
    }

    [Fact]
    public void SingleOrDefault_OneRow_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 2, "Unique", 99);

        var row = ctx.Query<TerminalOpRow>()
            .Where(r => r.CategoryId == 2)
            .SingleOrDefault();
        Assert.NotNull(row);
        Assert.Equal("Unique", row.Name);
    }

    [Fact]
    public void SingleOrDefault_TwoPlusRows_ThrowsInvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 3, "X", 1);
        Insert(cn, 3, "Y", 2);

        Assert.Throws<InvalidOperationException>(
            () => ctx.Query<TerminalOpRow>()
                     .Where(r => r.CategoryId == 3)
                     .SingleOrDefault());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Last / LastOrDefault
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void Last_EmptySequence_ThrowsInvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Assert.Throws<InvalidOperationException>(
            () => ctx.Query<TerminalOpRow>().Last());
    }

    [Fact]
    public void Last_OneRow_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "OnlyRow", 5);

        var row = ctx.Query<TerminalOpRow>().Last();
        Assert.Equal("OnlyRow", row.Name);
    }

    [Fact]
    public void Last_OrderedMultipleRows_ReturnsLastRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "First",  10);
        Insert(cn, 1, "Second", 20);
        Insert(cn, 1, "Third",  30);

        var row = ctx.Query<TerminalOpRow>()
            .OrderBy(r => r.IntValue)
            .Last();

        Assert.Equal("Third", row.Name);
    }

    [Fact]
    public void LastOrDefault_EmptySequence_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = ctx.Query<TerminalOpRow>().LastOrDefault();
        Assert.Null(result);
    }

    [Fact]
    public void LastOrDefault_OrderedMultipleRows_ReturnsLastRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 1);
        Insert(cn, 1, "B", 2);
        Insert(cn, 1, "C", 3);

        var row = ctx.Query<TerminalOpRow>()
            .OrderBy(r => r.IntValue)
            .LastOrDefault();

        Assert.Equal("C", row!.Name);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // ElementAt / ElementAtOrDefault
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void ElementAt_ValidIndex_ReturnsCorrectRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Row0", 0);
        Insert(cn, 1, "Row1", 1);
        Insert(cn, 1, "Row2", 2);
        Insert(cn, 1, "Row3", 3);

        var row = ctx.Query<TerminalOpRow>()
            .OrderBy(r => r.IntValue)
            .ElementAt(2);

        Assert.Equal("Row2", row.Name);
    }

    [Fact]
    public void ElementAt_OutOfRange_ThrowsArgumentOutOfRangeException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Row0", 10);
        Insert(cn, 1, "Row1", 20);

        Assert.Throws<ArgumentOutOfRangeException>(
            () => ctx.Query<TerminalOpRow>().ElementAt(5));
    }

    [Fact]
    public void ElementAt_EmptySequence_ThrowsArgumentOutOfRangeException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Assert.Throws<ArgumentOutOfRangeException>(
            () => ctx.Query<TerminalOpRow>().ElementAt(0));
    }

    [Fact]
    public void ElementAtOrDefault_ValidIndex_ReturnsRow()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "First",  10);
        Insert(cn, 1, "Second", 20);

        var row = ctx.Query<TerminalOpRow>()
            .OrderBy(r => r.IntValue)
            .ElementAtOrDefault(1);

        Assert.NotNull(row);
        Assert.Equal("Second", row.Name);
    }

    [Fact]
    public void ElementAtOrDefault_OutOfRange_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "Only", 1);

        var result = ctx.Query<TerminalOpRow>().ElementAtOrDefault(99);
        Assert.Null(result);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Any / All
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Any_EmptySequence_ReturnsFalse()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Assert.False(await ctx.Query<TerminalOpRow>().AnyAsync());
    }

    [Fact]
    public async Task Any_NonEmptySequence_ReturnsTrue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 10);

        Assert.True(await ctx.Query<TerminalOpRow>().AnyAsync());
    }

    [Fact]
    public void Any_WithPredicate_NoMatch_ReturnsFalse()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 10);

        Assert.False(ctx.Query<TerminalOpRow>().Any(r => r.CategoryId == 99));
    }

    [Fact]
    public void Any_WithPredicate_MatchExists_ReturnsTrue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 10);
        Insert(cn, 2, "B", 20);

        Assert.True(ctx.Query<TerminalOpRow>().Any(r => r.CategoryId == 2));
    }

    [Fact]
    public void All_EmptySequence_ReturnsTrue()
    {
        // Vacuous truth: All() on empty = true
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Assert.True(ctx.Query<TerminalOpRow>().All(r => r.IntValue > 100));
    }

    [Fact]
    public void All_AllMatch_ReturnsTrue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 5);
        Insert(cn, 1, "B", 10);

        Assert.True(ctx.Query<TerminalOpRow>().All(r => r.IntValue > 0));
    }

    [Fact]
    public void All_SomeDoNotMatch_ReturnsFalse()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A",  5);
        Insert(cn, 1, "B", -1);

        Assert.False(ctx.Query<TerminalOpRow>().All(r => r.IntValue > 0));
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Count / LongCount
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Count_EmptySequence_ReturnsZero()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Assert.Equal(0, await ctx.Query<TerminalOpRow>().CountAsync());
    }

    [Fact]
    public async Task Count_OneRow_ReturnsOne()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "X", 1);

        Assert.Equal(1, await ctx.Query<TerminalOpRow>().CountAsync());
    }

    [Fact]
    public async Task Count_MultipleRows_ReturnsCorrectTotal()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 0; i < 7; i++) Insert(cn, 1, $"R{i}", i);

        Assert.Equal(7, await ctx.Query<TerminalOpRow>().CountAsync());
    }

    [Fact]
    public async Task Count_WithPredicate_ReturnsFilteredCount()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 1);
        Insert(cn, 1, "B", 2);
        Insert(cn, 2, "C", 3);

        Assert.Equal(2, await ctx.Query<TerminalOpRow>()
            .Where(r => r.CategoryId == 1).CountAsync());
    }

    [Fact]
    public void LongCount_EmptySequence_ReturnsZero()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Assert.Equal(0L, ctx.Query<TerminalOpRow>().LongCount());
    }

    [Fact]
    public void LongCount_MultipleRows_ReturnsCorrectTotal()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        for (int i = 0; i < 5; i++) Insert(cn, 1, $"R{i}", i);

        Assert.Equal(5L, ctx.Query<TerminalOpRow>().LongCount());
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Aggregate: Sum, Min, Max, Average — empty sequence behavior
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Sum_EmptySequence_NonNullable_ReturnsZero()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await ctx.Query<TerminalOpRow>().SumAsync(r => r.IntValue);
        Assert.Equal(0, result);
    }

    [Fact]
    public async Task Sum_NonEmptySequence_ReturnsCorrectSum()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 10);
        Insert(cn, 1, "B", 20);
        Insert(cn, 1, "C", 30);

        var result = await ctx.Query<TerminalOpRow>().SumAsync(r => r.IntValue);
        Assert.Equal(60, result);
    }

    [Fact]
    public async Task Sum_NullableInt_EmptySequence_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await ctx.Query<TerminalOpRow>().SumAsync(r => r.NullableInt);
        Assert.Null(result);
    }

    [Fact]
    public async Task Sum_NullableInt_AllNulls_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 10, nullInt: null);
        Insert(cn, 1, "B", 20, nullInt: null);

        var result = await ctx.Query<TerminalOpRow>().SumAsync(r => r.NullableInt);
        Assert.Null(result);
    }

    [Fact]
    public async Task Sum_NullableInt_SomeNulls_ReturnsSumOfNonNulls()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 1, nullInt: 10);
        Insert(cn, 1, "B", 2, nullInt: null);
        Insert(cn, 1, "C", 3, nullInt: 20);

        var result = await ctx.Query<TerminalOpRow>().SumAsync(r => r.NullableInt);
        Assert.Equal(30, result);
    }

    [Fact]
    public async Task Min_EmptySequence_NonNullable_ThrowsInvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<TerminalOpRow>().MinAsync(r => r.IntValue));
    }

    [Fact]
    public async Task Min_NonEmptySequence_ReturnsCorrectMin()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 30);
        Insert(cn, 1, "B",  5);
        Insert(cn, 1, "C", 15);

        var result = await ctx.Query<TerminalOpRow>().MinAsync(r => r.IntValue);
        Assert.Equal(5, result);
    }

    [Fact]
    public async Task Min_NullableInt_EmptySequence_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await ctx.Query<TerminalOpRow>().MinAsync(r => r.NullableInt);
        Assert.Null(result);
    }

    [Fact]
    public async Task Min_NullableInt_AllNulls_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 1, nullInt: null);
        Insert(cn, 1, "B", 2, nullInt: null);

        var result = await ctx.Query<TerminalOpRow>().MinAsync(r => r.NullableInt);
        Assert.Null(result);
    }

    [Fact]
    public async Task Min_NullableInt_SomeNulls_ReturnsMinOfNonNulls()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 1, nullInt: 50);
        Insert(cn, 1, "B", 2, nullInt: null);
        Insert(cn, 1, "C", 3, nullInt: 10);

        var result = await ctx.Query<TerminalOpRow>().MinAsync(r => r.NullableInt);
        Assert.Equal(10, result);
    }

    [Fact]
    public async Task Max_EmptySequence_NonNullable_ThrowsInvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<TerminalOpRow>().MaxAsync(r => r.IntValue));
    }

    [Fact]
    public async Task Max_NonEmptySequence_ReturnsCorrectMax()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A",  5);
        Insert(cn, 1, "B", 99);
        Insert(cn, 1, "C", 42);

        var result = await ctx.Query<TerminalOpRow>().MaxAsync(r => r.IntValue);
        Assert.Equal(99, result);
    }

    [Fact]
    public async Task Max_NullableDouble_EmptySequence_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await ctx.Query<TerminalOpRow>().MaxAsync(r => r.NullableDouble);
        Assert.Null(result);
    }

    [Fact]
    public async Task Max_NullableDouble_SomeNulls_ReturnsMaxOfNonNulls()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 1, nullDbl: 3.5);
        Insert(cn, 1, "B", 2, nullDbl: null);
        Insert(cn, 1, "C", 3, nullDbl: 7.2);

        var result = await ctx.Query<TerminalOpRow>().MaxAsync(r => r.NullableDouble);
        Assert.NotNull(result);
        Assert.Equal(7.2, result!.Value, precision: 10);
    }

    [Fact]
    public async Task Average_EmptySequence_NonNullable_ThrowsInvalidOperationException()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.Query<TerminalOpRow>().AverageAsync(r => r.DoubleValue));
    }

    [Fact]
    public async Task Average_NonEmptySequence_ReturnsCorrectAverage()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 1, dblVal: 10.0);
        Insert(cn, 1, "B", 2, dblVal: 20.0);
        Insert(cn, 1, "C", 3, dblVal: 30.0);

        var result = await ctx.Query<TerminalOpRow>().AverageAsync(r => r.DoubleValue);
        Assert.Equal(20.0, result, precision: 10);
    }

    [Fact]
    public async Task Average_NullableDouble_EmptySequence_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        var result = await ctx.Query<TerminalOpRow>().AverageAsync(r => r.NullableDouble);
        Assert.Null(result);
    }

    [Fact]
    public async Task Average_NullableDouble_AllNulls_ReturnsNull()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 1, nullDbl: null);
        Insert(cn, 1, "B", 2, nullDbl: null);

        var result = await ctx.Query<TerminalOpRow>().AverageAsync(r => r.NullableDouble);
        Assert.Null(result);
    }

    [Fact]
    public async Task Average_NullableDouble_SomeNulls_AveragesNonNullsOnly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 1, nullDbl: 10.0);
        Insert(cn, 1, "B", 2, nullDbl: null);
        Insert(cn, 1, "C", 3, nullDbl: 20.0);

        var result = await ctx.Query<TerminalOpRow>().AverageAsync(r => r.NullableDouble);
        Assert.NotNull(result);
        Assert.Equal(15.0, result!.Value, precision: 10);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Aggregate with decimal columns
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public async Task Sum_Decimal_NonEmptySequence_ReturnsCorrectSum()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 0, decVal: 1.5m);
        Insert(cn, 1, "B", 0, decVal: 2.5m);
        Insert(cn, 1, "C", 0, decVal: 3.0m);

        var result = await ctx.Query<TerminalOpRow>().SumAsync(r => r.DecimalValue);
        Assert.Equal(7.0m, result);
    }

    [Fact]
    public async Task Average_Decimal_NonEmptySequence_ReturnsCorrectAverage()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, "A", 0, decVal: 2.0m);
        Insert(cn, 1, "B", 0, decVal: 4.0m);

        var result = await ctx.Query<TerminalOpRow>().AverageAsync(r => r.DecimalValue);
        Assert.Equal(3.0m, result);
    }

    // ═══════════════════════════════════════════════════════════════════════
    // Sync path parity — all terminal operators on same populated table
    // ═══════════════════════════════════════════════════════════════════════

    [Fact]
    public void SyncParity_AllTerminalOperators_CorrectResults()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        // Empty sequence assertions
        Assert.Null(ctx.Query<TerminalOpRow>().FirstOrDefault());
        Assert.Null(ctx.Query<TerminalOpRow>().SingleOrDefault());
        Assert.Null(ctx.Query<TerminalOpRow>().LastOrDefault());
        Assert.Null(ctx.Query<TerminalOpRow>().ElementAtOrDefault(0));
        Assert.Throws<InvalidOperationException>(() => ctx.Query<TerminalOpRow>().First());
        Assert.Throws<InvalidOperationException>(() => ctx.Query<TerminalOpRow>().Single());
        Assert.Throws<InvalidOperationException>(() => ctx.Query<TerminalOpRow>().Last());
        Assert.Throws<ArgumentOutOfRangeException>(() => ctx.Query<TerminalOpRow>().ElementAt(0));

        // Populate
        Insert(cn, 10, "Alpha",   5);
        Insert(cn, 10, "Beta",   15);
        Insert(cn, 10, "Gamma",  25);

        var q = ctx.Query<TerminalOpRow>().Where(r => r.CategoryId == 10).OrderBy(r => r.IntValue);

        Assert.Equal("Alpha", q.First().Name);
        Assert.Equal("Alpha", q.FirstOrDefault()!.Name);
        Assert.Equal("Gamma", q.Last().Name);
        Assert.Equal("Gamma", q.LastOrDefault()!.Name);
        Assert.Equal("Beta",  q.ElementAt(1).Name);
        Assert.Null(q.ElementAtOrDefault(99));
        Assert.Throws<InvalidOperationException>(() => q.Single());
        Assert.Throws<InvalidOperationException>(() => q.SingleOrDefault());
        Assert.Throws<ArgumentOutOfRangeException>(() => q.ElementAt(10));

        // Single row case
        var single = ctx.Query<TerminalOpRow>()
            .Where(r => r.CategoryId == 10 && r.IntValue == 15);
        Assert.Equal("Beta", single.Single().Name);
        Assert.Equal("Beta", single.SingleOrDefault()!.Name);
    }
}
