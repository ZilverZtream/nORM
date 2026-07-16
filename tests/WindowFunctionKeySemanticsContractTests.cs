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
/// Contract for window-function ordering-key semantics (Query/LINQ matrix cell: window functions x
/// order key type).
///
/// The OVER clause reuses the query's OrderBy/ThenBy chain, so window functions inherit the
/// type-exact ordering contracts: ROW_NUMBER / RANK / DENSE_RANK over a DECIMAL ordering operate at
/// full 28-digit precision (RANK ties are detected exactly - only true duplicates tie, where an
/// approximate key would tie every ~16-digit-equal value), rank gaps and dense ranks match standard
/// competition/dense semantics, LAG/LEAD read the true neighbours with the supplied default at
/// window edges, and a nullable ordering places nulls first like .NET. See docs/linq-support.md.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class WindowFunctionKeySemanticsContractTests
{
    [Table("WinKeyContract")]
    private sealed class W
    {
        [Key] public int Id { get; set; }
        public decimal DVal { get; set; }
        public int? NVal { get; set; }
        public int XVal { get; set; }
    }

    // 17th-digit-distinct decimals with ONE true tie pair (Ids 3 and 4), interleaved vs Id order.
    // True decimal order: 2(...05), 3(...06), 4(...06), 1(...07), 5(2).
    private static readonly (int Id, decimal D, int? N, int X)[] Rows =
    {
        (1, 1.00000000000000007m, 5,    10),
        (2, 1.00000000000000005m, null, 20),
        (3, 1.00000000000000006m, 2,    30),
        (4, 1.00000000000000006m, null, 40),
        (5, 2m,                   1,    50),
    };

    private static async Task<DbContext> SeedAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var c = cn.CreateCommand())
        {
            c.CommandText = "CREATE TABLE WinKeyContract (Id INTEGER PRIMARY KEY, DVal TEXT NOT NULL, NVal INTEGER, XVal INTEGER NOT NULL);";
            c.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var (id, d, n, x) in Rows) await ctx.InsertAsync(new W { Id = id, DVal = d, NVal = n, XVal = x });
        return ctx;
    }

    [Fact]
    public async Task RowNumber_over_decimal_ordering_is_full_precision()
    {
        using var ctx = await SeedAsync();
        var norm = ((INormQueryable<W>)ctx.Query<W>()).AsNoTracking()
            .OrderBy(w => w.DVal).ThenBy(w => w.Id)
            .WithRowNumber((w, n) => new { w.Id, N = n })
            .ToList().OrderBy(x => x.Id).Select(x => (x.Id, x.N)).ToList();

        var oracle = Rows.OrderBy(r => r.D).ThenBy(r => r.Id)
            .Select((r, i) => (r.Id, N: i + 1)).OrderBy(x => x.Id).ToList();

        Assert.Equal(oracle, norm);
        Assert.Equal((2, 1), norm[1]);   // ...05 is truly first - an approximate key could not know
    }

    [Fact]
    public async Task Rank_and_DenseRank_tie_only_on_true_duplicates()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<W>)ctx.Query<W>()).AsNoTracking();

        var rank = q.OrderBy(w => w.DVal)
            .WithRank((w, n) => new { w.Id, N = n })
            .ToList().OrderBy(x => x.Id).Select(x => (x.Id, x.N)).ToList();
        // Competition ranking: 2->1; the true tie pair 3,4 -> 2,2; gap to 1 -> 4; 5 -> 5.
        // An approximate (REAL-merged) key would tie Ids 1,2,3,4 all at rank 1.
        Assert.Equal(new[] { (1, 4), (2, 1), (3, 2), (4, 2), (5, 5) }, rank);

        var dense = q.OrderBy(w => w.DVal)
            .WithDenseRank((w, n) => new { w.Id, N = n })
            .ToList().OrderBy(x => x.Id).Select(x => (x.Id, x.N)).ToList();
        Assert.Equal(new[] { (1, 3), (2, 1), (3, 2), (4, 2), (5, 4) }, dense);
    }

    [Fact]
    public async Task Lag_and_Lead_read_true_neighbours_with_defaults_at_edges()
    {
        using var ctx = await SeedAsync();
        var q = ((INormQueryable<W>)ctx.Query<W>()).AsNoTracking();

        // Window order 2,3,4,1,5 -> X sequence 20,30,40,10,50.
        var lag = q.OrderBy(w => w.DVal).ThenBy(w => w.Id)
            .WithLag(w => w.XVal, 1, (w, prev) => new { w.Id, V = prev }, w => -1)
            .ToList().OrderBy(x => x.Id).Select(x => (x.Id, x.V)).ToList();
        Assert.Equal(new[] { (1, 40), (2, -1), (3, 20), (4, 30), (5, 10) }, lag!);

        var lead = q.OrderBy(w => w.DVal).ThenBy(w => w.Id)
            .WithLead(w => w.XVal, 1, (w, next) => new { w.Id, V = next }, w => -1)
            .ToList().OrderBy(x => x.Id).Select(x => (x.Id, x.V)).ToList();
        Assert.Equal(new[] { (1, 50), (2, 30), (3, 40), (4, 10), (5, -1) }, lead!);
    }

    [Fact]
    public async Task RowNumber_over_nullable_ordering_places_nulls_first_like_dotnet()
    {
        using var ctx = await SeedAsync();
        var norm = ((INormQueryable<W>)ctx.Query<W>()).AsNoTracking()
            .OrderBy(w => w.NVal).ThenBy(w => w.Id)
            .WithRowNumber((w, n) => new { w.Id, N = n })
            .ToList().OrderBy(x => x.Id).Select(x => (x.Id, x.N)).ToList();

        var oracle = Rows.OrderBy(r => r.N).ThenBy(r => r.Id)
            .Select((r, i) => (r.Id, N: i + 1)).OrderBy(x => x.Id).ToList();

        Assert.Equal(oracle, norm);
    }
}
