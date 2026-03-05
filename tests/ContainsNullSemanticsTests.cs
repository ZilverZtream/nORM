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
/// QP-1: Verifies that subquery Contains on nullable columns uses null-safe SQL semantics.
/// SQL: NULL IN (...) evaluates to UNKNOWN (filtered out), but LINQ semantics require
/// null-in-set to return true when null exists in the source set.
/// Fix: emit (val IN (subq) OR (val IS NULL AND EXISTS(SELECT 1 FROM src WHERE col IS NULL))).
/// </summary>
public class ContainsNullSemanticsTests
{
    [Table("NullSetItem")]
    private class NullSetItem
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }

        public int? NullableVal { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE NullSetItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, NullableVal INTEGER)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static void Insert(SqliteConnection cn, int? val)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO NullSetItem (NullableVal) VALUES (@v)";
        cmd.Parameters.AddWithValue("@v", val.HasValue ? (object)val.Value : DBNull.Value);
        cmd.ExecuteNonQuery();
    }

    // ─── Non-null Contains (non-regression) ──────────────────────────────

    [Fact]
    public async Task Contains_NonNullValue_InSubquery_ReturnsMatchingRows()
    {
        // Non-regression: non-nullable Contains still works.
        // Rows: null, 1, 2. Query: rows where NullableVal IN (SELECT NullableVal WHERE NullableVal = 1)
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, null);
        Insert(cn, 1);
        Insert(cn, 2);

        // Source subquery: just the row with NullableVal = 1
        var subq = ctx.Query<NullSetItem>()
            .Where(x => x.NullableVal == 1)
            .Select(x => x.NullableVal);

        // Target: rows whose NullableVal is in the subquery
        var results = await ctx.Query<NullSetItem>()
            .Where(x => subq.Contains(x.NullableVal))
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(1, results[0].NullableVal);
    }

    [Fact]
    public async Task Contains_ValueNotInSubquery_ReturnsNoRows()
    {
        // Non-regression: value not in subquery correctly returns nothing.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, 10);
        Insert(cn, 20);

        var subq = ctx.Query<NullSetItem>()
            .Where(x => x.NullableVal == 10)
            .Select(x => x.NullableVal);

        var results = await ctx.Query<NullSetItem>()
            .Where(x => subq.Contains(x.NullableVal))
            .ToListAsync();

        Assert.Single(results);
        Assert.Equal(10, results[0].NullableVal);
    }

    // ─── QP-1: null value in source and target ────────────────────────────

    [Fact]
    public async Task Contains_NullInSourceSubquery_NullTargetRow_ReturnsNullRow()
    {
        // QP-1: When source subquery contains null AND target row has null,
        // LINQ semantics require that null row to be returned.
        // SQL NULL IN (...) = UNKNOWN, so without the fix the null row is absent.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        // Source set: null, 1, 2
        Insert(cn, null);
        Insert(cn, 1);
        Insert(cn, 2);

        // Target set: null, 3 (two separate rows in same table for simplicity)
        Insert(cn, null); // id=4 — target null row
        Insert(cn, 3);    // id=5 — target non-matching row

        // Source subquery: contains null, 1, 2 (all rows with Id <= 3)
        var subq = ctx.Query<NullSetItem>()
            .Where(x => x.Id <= 3)
            .Select(x => x.NullableVal);

        // Target: rows with Id > 3 whose NullableVal is in the source subquery
        var results = await ctx.Query<NullSetItem>()
            .Where(x => x.Id > 3 && subq.Contains(x.NullableVal))
            .ToListAsync();

        // The null target row (id=4) should be returned because null is in the source.
        Assert.Single(results);
        Assert.Null(results[0].NullableVal);
    }

    [Fact]
    public async Task Contains_NullInSourceSubquery_NonNullTargetRows_ExcludesNonMatching()
    {
        // QP-1 non-regression: non-null rows not in the source are still excluded.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, null); // id=1 — source
        Insert(cn, 5);    // id=2 — target, in source → included
        Insert(cn, 99);   // id=3 — target, not in source → excluded

        var subq = ctx.Query<NullSetItem>()
            .Where(x => x.Id == 1 || x.NullableVal == 5)
            .Select(x => x.NullableVal);

        var results = await ctx.Query<NullSetItem>()
            .Where(x => x.Id > 1 && subq.Contains(x.NullableVal))
            .ToListAsync();

        // Only the row with NullableVal=5 matches; 99 does not
        Assert.Single(results);
        Assert.Equal(5, results[0].NullableVal);
    }

    // ─── QP-1: compile-time null Contains ────────────────────────────────

    [Fact]
    public async Task Contains_CompileTimeNull_SourceHasNull_ReturnsAllTargetRows()
    {
        // QP-1: Contains((int?)null) with a source that has null → the WHERE clause
        // is always true for all target rows (EXISTS(SELECT 1 FROM src WHERE col IS NULL)).
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, null); // source contains null
        Insert(cn, 10);   // source non-null

        var subq = ctx.Query<NullSetItem>().Select(x => x.NullableVal);

        // Query: all rows where null is in the subquery.
        // Since the subquery has null, this is always true.
        var results = await ctx.Query<NullSetItem>()
            .Where(x => subq.Contains((int?)null))
            .ToListAsync();

        // All rows should be returned (condition is universally true)
        Assert.Equal(2, results.Count);
    }

    [Fact]
    public async Task Contains_CompileTimeNull_SourceHasNoNull_ReturnsNoRows()
    {
        // QP-1: Contains((int?)null) when source has no nulls → WHERE clause is always false.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, 1);
        Insert(cn, 2);
        // No nulls in source

        var subq = ctx.Query<NullSetItem>().Select(x => x.NullableVal);

        var results = await ctx.Query<NullSetItem>()
            .Where(x => subq.Contains((int?)null))
            .ToListAsync();

        // No rows — null is not in the source subquery
        Assert.Empty(results);
    }

    // ─── QP-1: null in source but target has no nulls ─────────────────────

    [Fact]
    public async Task Contains_NullInSource_TargetHasNoNulls_NullRowsNotInTarget()
    {
        // QP-1 non-regression: if the source has null but the target has no null rows,
        // no extra rows are returned.
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        using var _ctx = ctx;

        Insert(cn, null); // id=1 — source only
        Insert(cn, 7);    // id=2 — target, in source? no → source has null, 7 is not in source select

        // Source: just the first row (NullableVal = null)
        var subq = ctx.Query<NullSetItem>()
            .Where(x => x.Id == 1)
            .Select(x => x.NullableVal);

        // Target: only row id=2 (NullableVal=7, no null)
        var results = await ctx.Query<NullSetItem>()
            .Where(x => x.Id > 1 && subq.Contains(x.NullableVal))
            .ToListAsync();

        // 7 is not null, so the null-safe path doesn't help; 7 is not in source → no results
        Assert.Empty(results);
    }
}
