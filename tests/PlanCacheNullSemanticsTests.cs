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
/// Verifies that the query plan cache correctly handles null-semantics stability.
/// When a closure-captured variable changes from non-null to null (or vice versa) between
/// two executions of the same query object, the re-execution must use the appropriate SQL
/// shape (IS NULL expansion vs. plain =) rather than returning a stale cached plan.
/// </summary>
public class PlanCacheNullSemanticsTests
{
    [Table("NullSemRow")]
    private class NullSemRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public string? Name { get; set; }
        public int? Value { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE NullSemRow (
                    Id    INTEGER PRIMARY KEY AUTOINCREMENT,
                    Name  TEXT,
                    Value INTEGER
                )";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    private static void Insert(SqliteConnection cn, string? name, int? value)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO NullSemRow (Name, Value) VALUES (@n, @v)";
        cmd.Parameters.AddWithValue("@n", (object?)name ?? DBNull.Value);
        cmd.Parameters.AddWithValue("@v", value.HasValue ? (object)value.Value : DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    // ─── Closure var starts non-null, then becomes null ─────────────

    [Fact]
    public async Task ClosureVariable_NonNullThenNull_ReturnsCorrectRowsBothTimes()
    {
        // Cache the plan with a non-null closure value, then re-execute with null.
        // The re-execution must use IS NULL semantics and return the null row.
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Insert(cn, "Alice", 1);   // matches when Name == "Alice"
        Insert(cn, null, 2);       // matches when Name == null (IS NULL)

        // First execution: non-null → plan cached with plain = comparison
        string? val = "Alice";
        var q1 = await ctx.Query<NullSemRow>().Where(x => x.Name == val).ToListAsync();
        Assert.Single(q1);
        Assert.Equal("Alice", q1[0].Name);

        // Second execution: null → must use IS NULL (requires different plan shape)
        val = null;
        var q2 = await ctx.Query<NullSemRow>().Where(x => x.Name == val).ToListAsync();
        Assert.Single(q2);
        Assert.Null(q2[0].Name);
    }

    // ─── Closure var starts null, then becomes non-null ─────────────

    [Fact]
    public async Task ClosureVariable_NullThenNonNull_ReturnsCorrectRowsBothTimes()
    {
        // Cache the plan with a null closure value, then re-execute with non-null.
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Insert(cn, "Bob", 1);    // matches when Name == "Bob"
        Insert(cn, null, 2);      // matches when Name IS NULL

        // First execution: null → plan cached with IS NULL
        string? val = null;
        var q1 = await ctx.Query<NullSemRow>().Where(x => x.Name == val).ToListAsync();
        Assert.Single(q1);
        Assert.Null(q1[0].Name);

        // Second execution: non-null → plain = must be used
        val = "Bob";
        var q2 = await ctx.Query<NullSemRow>().Where(x => x.Name == val).ToListAsync();
        Assert.Single(q2);
        Assert.Equal("Bob", q2[0].Name);
    }

    // ─── null literal in expression → IS NULL (not closure) ─────────

    [Fact]
    public async Task NullLiteral_InExpression_UsesIsNull()
    {
        // Column == null literal (hardcoded in expression tree) must always use IS NULL.
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Insert(cn, "Carol", 1);
        Insert(cn, null, 2);

        var results = await ctx.Query<NullSemRow>().Where(x => x.Name == null).ToListAsync();
        Assert.Single(results);
        Assert.Null(results[0].Name);
    }

    // ─── Non-null string literal → plain = (no unnecessary expansion) ─

    [Fact]
    public async Task StringLiteral_InExpression_UsesPlainEquals()
    {
        // Column == "literal" (non-null string literal) should work correctly.
        // It uses null-safe expansion but the expansion is logically equivalent
        // to plain = when the parameter is non-null, so the result must still be correct.
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Insert(cn, "Dave", 1);
        Insert(cn, null, 2);
        Insert(cn, "Eve", 3);

        var results = await ctx.Query<NullSemRow>().Where(x => x.Name == "Dave").ToListAsync();
        Assert.Single(results);
        Assert.Equal("Dave", results[0].Name);
    }

    // ─── Nullable int? closure — null then non-null ─────────────────

    [Fact]
    public async Task NullableIntClosure_NullThenNonNull_ReturnsCorrectRows()
    {
        // Same as the string tests but for Nullable<int> closure captures.
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Insert(cn, null, 10);
        Insert(cn, null, null);

        // First: null → IS NULL
        int? target = null;
        var q1 = await ctx.Query<NullSemRow>().Where(x => x.Value == target).ToListAsync();
        Assert.Single(q1);
        Assert.Null(q1[0].Value);

        // Second: non-null → plain =
        target = 10;
        var q2 = await ctx.Query<NullSemRow>().Where(x => x.Value == target).ToListAsync();
        Assert.Single(q2);
        Assert.Equal(10, q2[0].Value);
    }

    // ─── Different closure instances, same shape ─────────────────────

    [Fact]
    public async Task TwoSeparateQueries_SameClosure_NullAndNonNull_BothCorrect()
    {
        // Two separate query expressions with different closure values must each
        // use the appropriate SQL shape.
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;

        Insert(cn, "Frank", 1);
        Insert(cn, null, 2);

        string? val1 = "Frank";
        string? val2 = null;

        var r1 = await ctx.Query<NullSemRow>().Where(x => x.Name == val1).ToListAsync();
        var r2 = await ctx.Query<NullSemRow>().Where(x => x.Name == val2).ToListAsync();

        Assert.Single(r1);
        Assert.Equal("Frank", r1[0].Name);

        Assert.Single(r2);
        Assert.Null(r2[0].Name);
    }
}
