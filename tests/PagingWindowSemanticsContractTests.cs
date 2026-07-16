using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Contract for paging window semantics (Query/LINQ matrix cell: Skip/Take/TakeLast/SkipLast x
/// ordering source).
///
/// Every probed window matches LINQ-to-Objects: <c>Take(n&lt;=0)</c> is empty and <c>Skip(n&lt;=0)</c>
/// is identity; <c>Take(n).Skip(m)</c> yields the algebraic [m, n) window with literal and runtime
/// counts; Skip/Take and TakeLast/SkipLast windows over a DECIMAL ordering are cut at full 28-digit
/// precision (values agreeing to 16 significant digits order exactly, so the window contains the true
/// rows - an approximate sort key would cut a different window); and TakeLast over a nullable ordering
/// flips nulls-first to nulls-last correctly. See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class PagingWindowSemanticsContractTests
{
    [Table("PageWinContract")]
    private sealed class P
    {
        [Key] public int Id { get; set; }
        public decimal DVal { get; set; }
        public int? NVal { get; set; }
    }

    // Decimal values interleaved against Id order so an approximate (REAL-merged) sort would
    // tiebreak by Id and cut a DIFFERENT window than the true decimal order 2,4,3,1,5.
    private static readonly (int Id, decimal D, int? N)[] Rows =
    {
        (1, 1.00000000000000008m, 5),
        (2, 1.00000000000000005m, null),
        (3, 1.00000000000000007m, 2),
        (4, 1.00000000000000006m, null),
        (5, 2m,                   1),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE PageWinContract (Id INTEGER PRIMARY KEY, DVal TEXT NOT NULL, NVal INTEGER);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in Rows) await ctx.InsertAsync(new P { Id = r.Id, DVal = r.D, NVal = r.N });
        return ctx;
    }

    [Fact]
    public async Task Zero_and_negative_counts_match_linq()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<P>)ctx.Query<P>()).AsNoTracking();

        Assert.Empty(q.OrderBy(p => p.Id).Take(0).Select(p => p.Id).ToList());
        Assert.Empty(q.OrderBy(p => p.Id).Take(-1).Select(p => p.Id).ToList());
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, q.OrderBy(p => p.Id).Skip(-2).Select(p => p.Id).ToList());
        Assert.Equal(new[] { 1, 2, 3, 4, 5 }, q.OrderBy(p => p.Id).Skip(0).Select(p => p.Id).ToList());
    }

    [Fact]
    public async Task Take_then_skip_yields_the_algebraic_window_with_literal_and_runtime_counts()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<P>)ctx.Query<P>()).AsNoTracking();

        Assert.Equal(
            Rows.OrderBy(r => r.Id).Take(4).Skip(2).Select(r => r.Id).ToList(),
            q.OrderBy(p => p.Id).Take(4).Skip(2).Select(p => p.Id).ToList());

        int take = 4, skip = 2;
        Assert.Equal(
            Rows.OrderBy(r => r.Id).Take(take).Skip(skip).Select(r => r.Id).ToList(),
            q.OrderBy(p => p.Id).Take(take).Skip(skip).Select(p => p.Id).ToList());
    }

    [Fact]
    public async Task Windows_over_decimal_ordering_cut_at_full_precision()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<P>)ctx.Query<P>()).AsNoTracking();

        // True decimal order is 2, 4, 3, 1, 5. An approximate key would merge the four 1.000...x
        // values and tiebreak by Id, cutting a different window.
        var window = q.OrderBy(p => p.DVal).ThenBy(p => p.Id).Skip(1).Take(2).Select(p => p.Id).ToList();
        Assert.Equal(Rows.OrderBy(r => r.D).ThenBy(r => r.Id).Skip(1).Take(2).Select(r => r.Id).ToList(), window);
        Assert.Equal(new[] { 4, 3 }, window);

        var tail = q.OrderBy(p => p.DVal).ThenBy(p => p.Id).TakeLast(2).Select(p => p.Id).ToList();
        Assert.Equal(Rows.OrderBy(r => r.D).ThenBy(r => r.Id).TakeLast(2).Select(r => r.Id).ToList(), tail);
        Assert.Equal(new[] { 1, 5 }, tail);

        var head = q.OrderBy(p => p.DVal).ThenBy(p => p.Id).SkipLast(2).Select(p => p.Id).ToList();
        Assert.Equal(Rows.OrderBy(r => r.D).ThenBy(r => r.Id).SkipLast(2).Select(r => r.Id).ToList(), head);
        Assert.Equal(new[] { 2, 4, 3 }, head);
    }

    [Fact]
    public async Task Windows_over_nullable_ordering_flip_null_placement_correctly()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<P>)ctx.Query<P>()).AsNoTracking();

        // nulls-first ascending: 2, 4 (nulls), then 5, 3, 1 by value.
        Assert.Equal(
            Rows.OrderBy(r => r.N).ThenBy(r => r.Id).Skip(1).Take(2).Select(r => r.Id).ToList(),
            q.OrderBy(p => p.NVal).ThenBy(p => p.Id).Skip(1).Take(2).Select(p => p.Id).ToList());

        // TakeLast flips the order internally; null placement must flip with it (nulls last descending).
        Assert.Equal(
            Rows.OrderBy(r => r.N).ThenBy(r => r.Id).TakeLast(2).Select(r => r.Id).ToList(),
            q.OrderBy(p => p.NVal).ThenBy(p => p.Id).TakeLast(2).Select(p => p.Id).ToList());
    }
}
