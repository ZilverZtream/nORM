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
/// Tests that null semantics produce the correct rows when executed against a real SQLite database.
/// Covers IS NULL, IS NOT NULL, column-vs-constant, and column-vs-column comparisons.
/// </summary>
public class NullSemanticsTests
{
    [Table("NullRow")]
    private class NullRow
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public int? NullableInt { get; set; }
        public string? NullableStr { get; set; }
        public int NonNullableInt { get; set; }
        public int? NullableB { get; set; }
        public string? NullableStrB { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = @"
                CREATE TABLE NullRow (
                    Id            INTEGER PRIMARY KEY AUTOINCREMENT,
                    NullableInt   INTEGER,
                    NullableStr   TEXT,
                    NonNullableInt INTEGER NOT NULL DEFAULT 0,
                    NullableB     INTEGER,
                    NullableStrB  TEXT
                )";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        return (cn, ctx);
    }

    private static int Insert(SqliteConnection cn, int? nullableInt, string? nullableStr,
        int nonNullableInt, int? nullableB = null, string? nullableStrB = null)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"INSERT INTO NullRow (NullableInt, NullableStr, NonNullableInt, NullableB, NullableStrB)
                            VALUES (@ni, @ns, @nn, @nb, @nsb);
                            SELECT last_insert_rowid();";
        cmd.Parameters.AddWithValue("@ni", nullableInt.HasValue ? (object)nullableInt.Value : System.DBNull.Value);
        cmd.Parameters.AddWithValue("@ns", nullableStr is null ? System.DBNull.Value : (object)nullableStr);
        cmd.Parameters.AddWithValue("@nn", nonNullableInt);
        cmd.Parameters.AddWithValue("@nb", nullableB.HasValue ? (object)nullableB.Value : System.DBNull.Value);
        cmd.Parameters.AddWithValue("@nsb", nullableStrB is null ? System.DBNull.Value : (object)nullableStrB);
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    // ─── nullable int? == null (IS NULL) ─────────────────────────────────

    [Fact]
    public async Task NullableInt_EqualNull_ReturnsRowsWithNullValue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);      // matches
        Insert(cn, 42, null, 2);         // does not match

        var results = await ctx.Query<NullRow>().Where(x => x.NullableInt == null).ToListAsync();
        Assert.Single(results);
        Assert.Null(results[0].NullableInt);
    }

    // ─── nullable int? != null (IS NOT NULL) ─────────────────────────────

    [Fact]
    public async Task NullableInt_NotEqualNull_ReturnsRowsWithNonNullValue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);      // does not match
        Insert(cn, 42, null, 2);         // matches
        Insert(cn, 99, null, 3);         // matches

        var results = await ctx.Query<NullRow>().Where(x => x.NullableInt != null).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.NotNull(r.NullableInt));
    }

    // ─── nullable int? == constant ────────────────────────────────────────

    [Fact]
    public async Task NullableInt_EqualConstant_ReturnsMatchingRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 42, null, 1);     // matches
        Insert(cn, null, null, 2);   // null — does not match
        Insert(cn, 99, null, 3);     // different value — does not match

        var results = await ctx.Query<NullRow>().Where(x => x.NullableInt == 42).ToListAsync();
        Assert.Single(results);
        Assert.Equal(42, results[0].NullableInt);
    }

    // ─── nullable string == null ──────────────────────────────────────────

    [Fact]
    public async Task NullableString_EqualNull_ReturnsRowsWithNullValue()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);          // matches
        Insert(cn, null, "hello", 2);       // does not match

        var results = await ctx.Query<NullRow>().Where(x => x.NullableStr == null).ToListAsync();
        Assert.Single(results);
        Assert.Null(results[0].NullableStr);
    }

    // ─── nullable string != null ──────────────────────────────────────────

    [Fact]
    public async Task NullableString_NotEqualNull_ReturnsNonNullRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);           // does not match
        Insert(cn, null, "hello", 2);        // matches
        Insert(cn, null, "world", 3);        // matches

        var results = await ctx.Query<NullRow>().Where(x => x.NullableStr != null).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.NotNull(r.NullableStr));
    }

    // ─── nullable string == "literal" ─────────────────────────────────────

    [Fact]
    public async Task NullableString_EqualLiteral_ReturnsMatchingRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, "Alice", 1);      // matches
        Insert(cn, null, null, 2);          // null — does not match
        Insert(cn, null, "Bob", 3);         // different value — does not match

        var results = await ctx.Query<NullRow>().Where(x => x.NullableStr == "Alice").ToListAsync();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].NullableStr);
    }

    // ─── column-vs-column: nullable int? == nullable int? ─────────────────

    [Fact]
    public async Task NullableInt_ColumnVsColumn_BothNull_Matches()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        // Row where both NullableInt and NullableB are NULL → should match NullableInt == NullableB
        Insert(cn, null, null, 1, null);          // both null → matches
        Insert(cn, 5, null, 2, 5);               // both same non-null value → matches
        Insert(cn, 5, null, 3, 10);              // different non-null values → does not match
        Insert(cn, null, null, 4, 7);            // one null, one non-null → does not match

        var results = await ctx.Query<NullRow>().Where(x => x.NullableInt == x.NullableB).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── column-vs-column: nullable int? != nullable int? ────────────────

    [Fact]
    public async Task NullableInt_ColumnVsColumn_NotEqual_OneNull_Matches()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1, null);         // both null → does not match !=
        Insert(cn, 5, null, 2, 5);              // both same → does not match !=
        Insert(cn, 5, null, 3, 10);             // different non-null → matches
        Insert(cn, null, null, 4, 7);           // one null, one non-null → matches

        var results = await ctx.Query<NullRow>().Where(x => x.NullableInt != x.NullableB).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── column-vs-column: nullable string == nullable string ─────────────

    [Fact]
    public async Task NullableString_ColumnVsColumn_BothNull_Matches()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        // Row where both NullableStr and NullableStrB are null → should match
        Insert(cn, null, null, 1, null, null);           // both null → matches
        Insert(cn, null, "hi", 2, null, "hi");           // both same → matches
        Insert(cn, null, "hi", 3, null, "bye");          // different → no match
        Insert(cn, null, null, 4, null, "val");          // one null → no match

        var results = await ctx.Query<NullRow>().Where(x => x.NullableStr == x.NullableStrB).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── column-vs-column: nullable string != nullable string ─────────────

    [Fact]
    public async Task NullableString_ColumnVsColumn_OneNull_Matches()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1, null, null);           // both null → no match for !=
        Insert(cn, null, "hi", 2, null, "hi");           // both same → no match for !=
        Insert(cn, null, "hi", 3, null, "bye");          // different non-null → matches
        Insert(cn, null, null, 4, null, "val");          // one null → matches

        var results = await ctx.Query<NullRow>().Where(x => x.NullableStr != x.NullableStrB).ToListAsync();
        Assert.Equal(2, results.Count);
    }

    // ─── non-nullable == non-nullable (no IS NULL expansion) ──────────────

    [Fact]
    public async Task NonNullableInt_EqualConstant_ReturnsMatchingRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 10);   // matches
        Insert(cn, null, null, 20);   // no match
        Insert(cn, null, null, 10);   // matches (second row with same value)

        var results = await ctx.Query<NullRow>().Where(x => x.NonNullableInt == 10).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.Equal(10, r.NonNullableInt));
    }

    // ─── Null IS NULL filter returns nothing when all rows are non-null ────

    [Fact]
    public async Task NullableInt_EqualNull_NoNullRows_ReturnsEmpty()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, 1, null, 1);
        Insert(cn, 2, null, 2);
        Insert(cn, 3, null, 3);

        var results = await ctx.Query<NullRow>().Where(x => x.NullableInt == null).ToListAsync();
        Assert.Empty(results);
    }

    // ─── IS NOT NULL returns nothing when all rows are null ────────────────

    [Fact]
    public async Task NullableInt_NotEqualNull_AllNullRows_ReturnsEmpty()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);
        Insert(cn, null, null, 2);

        var results = await ctx.Query<NullRow>().Where(x => x.NullableInt != null).ToListAsync();
        Assert.Empty(results);
    }

    // ─── ToList with null filter covers count verification ─────────────────

    [Fact]
    public async Task NullFilter_ToList_ReturnsCorrectRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);
        Insert(cn, null, null, 2);
        Insert(cn, 5, null, 3);
        Insert(cn, 5, null, 4);
        Insert(cn, 5, null, 5);

        // Verify null rows via ToList (full query path handles IS NULL correctly)
        var nullRows = await ctx.Query<NullRow>().Where(x => x.NullableInt == null).ToListAsync();
        var nonNullRows = await ctx.Query<NullRow>().Where(x => x.NullableInt != null).ToListAsync();
        Assert.Equal(2, nullRows.Count);
        Assert.Equal(3, nonNullRows.Count);
    }

    // ─── QP-1: nullable string != non-null constant includes NULL rows ─────

    /// <summary>
    /// QP-1: SQL 3VL issue — NULL &lt;&gt; 'Alice' = UNKNOWN (excluded by SQL), but
    /// C# says null != "Alice" = true (should be included).
    /// Fix: emit (col IS NULL OR col &lt;&gt; @p) for NotEqual with nullable column vs non-null constant.
    /// </summary>
    [Fact]
    public async Task NullableString_NotEqual_NonNullConstant_IncludesNullRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);      // NullableStr = NULL → null != "Alice" is true in C# → INCLUDE
        Insert(cn, null, "Bob", 2);     // NullableStr = "Bob" → "Bob" != "Alice" is true → INCLUDE
        Insert(cn, null, "Alice", 3);   // NullableStr = "Alice" → "Alice" != "Alice" is false → EXCLUDE

        var results = await ctx.Query<NullRow>().Where(x => x.NullableStr != "Alice").ToListAsync();
        // Both NULL row and "Bob" row should be returned (null != "Alice" is true in C#)
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.NullableStr is null);
        Assert.Contains(results, r => r.NullableStr == "Bob");
    }

    /// <summary>
    /// QP-1 regression: nullable string == non-null constant should NOT include NULL rows.
    /// C# says null == "Alice" = false, SQL NULL = 'Alice' = UNKNOWN (excluded) — both agree.
    /// So Equal does NOT need expansion when right side is non-null.
    /// </summary>
    [Fact]
    public async Task NullableString_Equal_NonNullConstant_ExcludesNullRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);      // NullableStr = NULL → null == "Alice" is false → EXCLUDE
        Insert(cn, null, "Bob", 2);     // NullableStr = "Bob" → "Bob" == "Alice" is false → EXCLUDE
        Insert(cn, null, "Alice", 3);   // NullableStr = "Alice" → "Alice" == "Alice" is true → INCLUDE

        var results = await ctx.Query<NullRow>().Where(x => x.NullableStr == "Alice").ToListAsync();
        Assert.Single(results);
        Assert.Equal("Alice", results[0].NullableStr);
    }

    /// <summary>
    /// QP-1: Same pattern for int? — nullable int != non-null constant must include NULL rows.
    /// </summary>
    [Fact]
    public async Task NullableInt_NotEqual_NonNullConstant_IncludesNullRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);   // NullableInt = NULL → null != 42 is true in C# → INCLUDE
        Insert(cn, 99, null, 2);     // NullableInt = 99 → 99 != 42 → INCLUDE
        Insert(cn, 42, null, 3);     // NullableInt = 42 → 42 != 42 is false → EXCLUDE

        var results = await ctx.Query<NullRow>().Where(x => x.NullableInt != 42).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.Contains(results, r => r.NullableInt is null);
        Assert.Contains(results, r => r.NullableInt == 99);
    }

    /// <summary>
    /// QP-1 regression: non-nullable column != constant should NOT expand to include IS NULL.
    /// The column cannot be null so no expansion is needed.
    /// </summary>
    [Fact]
    public async Task NonNullable_NotEqual_NoExpansion_ReturnsCorrectRows()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 10);    // NonNullableInt = 10 → 10 != 20 → INCLUDE
        Insert(cn, null, null, 20);    // NonNullableInt = 20 → 20 != 20 → EXCLUDE
        Insert(cn, null, null, 30);    // NonNullableInt = 30 → 30 != 20 → INCLUDE

        var results = await ctx.Query<NullRow>().Where(x => x.NonNullableInt != 20).ToListAsync();
        Assert.Equal(2, results.Count);
        Assert.All(results, r => Assert.NotEqual(20, r.NonNullableInt));
    }

    /// <summary>
    /// QP-1 regression guard: column-vs-column nullable != nullable still uses full 3-way expansion.
    /// When BOTH sides could be null, the full (IS NOT NULL AND ... OR IS NULL AND ... OR IS NOT NULL AND IS NULL)
    /// expansion is still needed to handle all null/non-null combinations correctly.
    /// </summary>
    [Fact]
    public async Task NullableColumn_NotEqual_NullableColumn_StillExpandsCorrectly()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        // Reuse NullableInt and NullableB columns for column-vs-column test
        Insert(cn, null, null, 1, null);          // both null → null != null is false → EXCLUDE
        Insert(cn, 5, null, 2, 5);               // both same non-null → 5 != 5 is false → EXCLUDE
        Insert(cn, 5, null, 3, 10);              // different non-null → 5 != 10 is true → INCLUDE
        Insert(cn, null, null, 4, 7);            // left null, right non-null → null != 7 is true → INCLUDE
        Insert(cn, 7, null, 5, null);            // left non-null, right null → 7 != null is true → INCLUDE

        var results = await ctx.Query<NullRow>().Where(x => x.NullableInt != x.NullableB).ToListAsync();
        Assert.Equal(3, results.Count);
    }

    // ─── Null semantics regression sweep (additional coverage) ─────────────

    /// <summary>
    /// nullable_col == null → IS NULL (already covered, regression guard)
    /// </summary>
    [Fact]
    public async Task NullableString_EqualNull_RegressionGuard()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);
        Insert(cn, null, "hello", 2);

        var results = await ctx.Query<NullRow>().Where(x => x.NullableStr == null).ToListAsync();
        Assert.Single(results);
        Assert.Null(results[0].NullableStr);
    }

    /// <summary>
    /// nullable_col != null → IS NOT NULL (already covered, regression guard)
    /// </summary>
    [Fact]
    public async Task NullableString_NotEqualNull_RegressionGuard()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 1);
        Insert(cn, null, "hello", 2);

        var results = await ctx.Query<NullRow>().Where(x => x.NullableStr != null).ToListAsync();
        Assert.Single(results);
        Assert.Equal("hello", results[0].NullableStr);
    }

    /// <summary>
    /// non_nullable_col == "literal" → plain = (no IS NULL branch)
    /// </summary>
    [Fact]
    public async Task NonNullable_Equal_Literal_NoIsNullBranch()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn; using var _ctx = ctx;
        Insert(cn, null, null, 10);
        Insert(cn, null, null, 20);

        var results = await ctx.Query<NullRow>().Where(x => x.NonNullableInt == 10).ToListAsync();
        Assert.Single(results);
        Assert.Equal(10, results[0].NonNullableInt);
    }
}
