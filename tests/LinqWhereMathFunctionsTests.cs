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
/// Pins <see cref="Math"/> function calls in a WHERE predicate — should lower
/// to provider SQL functions (ABS, ROUND, MIN, MAX). Silent-wrongness risk if
/// the wrapper is dropped and the comparison runs against the bare column —
/// e.g. <c>Where(p =&gt; Math.Abs(p.X) &gt; 5)</c> silently becoming
/// <c>WHERE X &gt; 5</c> would miss the negative-large-magnitude rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereMathFunctionsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WmRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Amount REAL NOT NULL);
            -- A values mix sign so |A| > 5 is a different set than A > 5.
            -- Rows: Id, A, B, Amount
            INSERT INTO WmRow VALUES
              (1,  -8,  3,  1.4),
              (2,   2,  9,  2.6),
              (3,  -3,  1,  3.5),
              (4,  10,  5,  4.4),
              (5,  -6,  7,  5.5),
              (6,   1,  4,  6.6);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_math_abs_int_filters_by_absolute_value_not_signed_value()
    {
        // |A| > 5 → rows where A is -8, 10, -6 → Ids [1, 4, 5].
        // Silent-wrongness check: if Math.Abs is dropped, the signed compare A > 5
        // returns only Id 4 (single row, missing -8 and -6).
        var ids = (await _ctx.Query<WmRow>()
            .Where(p => Math.Abs(p.A) > 5)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 1, 4, 5 }, ids);
    }

    [Fact]
    public async Task Where_math_round_double_rounds_to_nearest_int_before_compare()
    {
        // Round(Amount) == 5 → Amount in [4.5, 5.5) for banker's, but SQLite ROUND
        // is half-away-from-zero. Amount values: 1.4→1, 2.6→3, 3.5→4 (banker's) or 4 (half-up?),
        // 4.4→4, 5.5→6 (half-up) or 6, 6.6→7. So Round==5 has NO rows in current data.
        // Use Round(Amount) == 6 instead → matches 5.5 (rounded to 6) → Id 5.
        var ids = (await _ctx.Query<WmRow>()
            .Where(p => Math.Round(p.Amount) == 6.0)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 5 }, ids);
    }

    [Fact]
    public async Task Where_math_min_two_columns_compares_smaller_of_pair()
    {
        // Min(A, B): row 1 → min(-8,3)=-8, 2 → min(2,9)=2, 3 → min(-3,1)=-3,
        // 4 → min(10,5)=5, 5 → min(-6,7)=-6, 6 → min(1,4)=1.
        // Min(A,B) >= 2 → rows where the SMALLER is at least 2 → Ids [2, 4].
        var ids = (await _ctx.Query<WmRow>()
            .Where(p => Math.Min(p.A, p.B) >= 2)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 4 }, ids);
    }

    [Fact]
    public async Task Where_math_max_two_columns_compares_larger_of_pair()
    {
        // Max(A, B): row 1 → max(-8,3)=3, 2 → max(2,9)=9, 3 → max(-3,1)=1,
        // 4 → max(10,5)=10, 5 → max(-6,7)=7, 6 → max(1,4)=4.
        // Max(A,B) > 5 → rows where the LARGER exceeds 5 → Ids [2, 4, 5].
        var ids = (await _ctx.Query<WmRow>()
            .Where(p => Math.Max(p.A, p.B) > 5)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 2, 4, 5 }, ids);
    }

    [Table("WmRow")]
    public sealed class WmRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public double Amount { get; set; }
    }
}
