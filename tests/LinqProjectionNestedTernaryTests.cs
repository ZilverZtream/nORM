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
/// Pins nested ternary (<c>a ? x : b ? y : z</c>) inside a Select projection
/// — the classic 3-way classification idiom. Should emit nested
/// <c>CASE WHEN ... THEN ... ELSE (CASE WHEN ... THEN ... ELSE ... END) END</c>.
/// Silent-wrongness risk if the inner CASE is dropped or both branches of
/// either tier collapse — values in the middle band would silently land in
/// the wrong bucket.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionNestedTernaryTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NtRow (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);
            -- One row per band so each branch must produce its own answer:
            --   Id 1: 15  -> high  (> 10)
            --   Id 2:  7  -> mid   (between 6 and 10)
            --   Id 3:  3  -> low   (<= 5)
            --   Id 4: 11  -> high  (boundary above)
            --   Id 5:  5  -> low   (boundary below)
            INSERT INTO NtRow VALUES (1, 15), (2, 7), (3, 3), (4, 11), (5, 5);
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
    public async Task Projection_nested_ternary_returns_three_way_classification()
    {
        var rows = (await _ctx.Query<NtRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Band = p.Score > 10 ? "high" : p.Score > 5 ? "mid" : "low" })
            .ToListAsync())
            .Select(r => (r.Id, r.Band))
            .ToArray();
        // Silent-wrongness check: if the inner CASE is dropped, mid-band rows
        // (Id 2 with score 7) would receive the ELSE branch ("low") instead.
        Assert.Equal(new[] { (1, "high"), (2, "mid"), (3, "low"), (4, "high"), (5, "low") }, rows);
    }

    [Fact]
    public async Task Projection_triple_nested_ternary_returns_four_way_classification()
    {
        // Four-way: > 10 = "huge", > 8 = "big", > 4 = "med", else "tiny".
        //   15 > 10 -> huge
        //    7 > 4 (not > 8 or > 10) -> med
        //    3 -> tiny (not > 4)
        //   11 > 10 -> huge
        //    5 > 4 (not > 8 or > 10) -> med
        var rows = (await _ctx.Query<NtRow>()
            .OrderBy(p => p.Id)
            .Select(p => new
            {
                p.Id,
                Band = p.Score > 10 ? "huge"
                     : p.Score > 8  ? "big"
                     : p.Score > 4  ? "med"
                                    : "tiny"
            })
            .ToListAsync())
            .Select(r => (r.Id, r.Band))
            .ToArray();
        Assert.Equal(new[] { (1, "huge"), (2, "med"), (3, "tiny"), (4, "huge"), (5, "med") }, rows);
    }

    [Table("NtRow")]
    public sealed class NtRow
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }
    }
}
