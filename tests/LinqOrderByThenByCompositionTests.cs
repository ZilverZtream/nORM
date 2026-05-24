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
/// Pins <c>OrderBy(a).ThenBy(b)</c> / <c>ThenByDescending(c)</c> composition.
/// Silent-wrongness shape: if ThenBy is silently dropped, the primary sort
/// still works so results LOOK sorted, but rows that share a primary-key
/// value land in arbitrary (insertion / page) order instead of the
/// user-specified secondary ordering. Common bug class because the failure
/// only manifests when primary keys collide.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByThenByCompositionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE OtbRow (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, City TEXT NOT NULL, Tier INTEGER NOT NULL);
            -- Insertion order deliberately scrambled so PK-order is wrong for
            -- every sort criterion below. Within each Region, multiple Cities;
            -- within each (Region, City), multiple Tiers — three-level sort
            -- exercises chained ThenBy AND ThenByDescending.
            INSERT INTO OtbRow VALUES
              (1, 'West', 'Bergen',    3),
              (2, 'East', 'Oslo',      1),
              (3, 'West', 'Bergen',    1),
              (4, 'East', 'Oslo',      3),
              (5, 'West', 'Stavanger', 2),
              (6, 'East', 'Drammen',   2),
              (7, 'East', 'Oslo',      2),
              (8, 'West', 'Bergen',    2);
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
    public async Task OrderBy_then_thenby_sorts_by_secondary_when_primary_ties()
    {
        // Region ASC, then City ASC.
        //   East: Drammen(6), Oslo(2), Oslo(4), Oslo(7)
        //   West: Bergen(1), Bergen(3), Bergen(8), Stavanger(5)
        // (Within identical (Region, City) ties, order is arbitrary unless
        //  pinned further — the assertion uses only Region/City so any
        //  insertion-order leak within Oslo / Bergen passes the test, but a
        //  dropped ThenBy would scramble Region groups too.)
        var rows = (await _ctx.Query<OtbRow>()
            .OrderBy(p => p.Region)
            .ThenBy(p => p.City)
            .ToListAsync())
            .Select(r => (r.Region, r.City))
            .ToArray();
        Assert.Equal(new[]
        {
            ("East", "Drammen"),
            ("East", "Oslo"),
            ("East", "Oslo"),
            ("East", "Oslo"),
            ("West", "Bergen"),
            ("West", "Bergen"),
            ("West", "Bergen"),
            ("West", "Stavanger"),
        }, rows);
    }

    [Fact]
    public async Task OrderBy_thenby_thenbydescending_three_level_sort_produces_full_lex_order()
    {
        // Region ASC, City ASC, Tier DESC.
        //   East   Drammen   2 -> 6
        //   East   Oslo      3 -> 4
        //   East   Oslo      2 -> 7
        //   East   Oslo      1 -> 2
        //   West   Bergen    3 -> 1
        //   West   Bergen    2 -> 8
        //   West   Bergen    1 -> 3
        //   West   Stavanger 2 -> 5
        var ids = (await _ctx.Query<OtbRow>()
            .OrderBy(p => p.Region)
            .ThenBy(p => p.City)
            .ThenByDescending(p => p.Tier)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 6, 4, 7, 2, 1, 8, 3, 5 }, ids);
    }

    [Fact]
    public async Task OrderByDescending_thenby_inverts_only_primary_and_ascends_secondary()
    {
        // Region DESC, City ASC.
        //   West rows first (Bergen x3, Stavanger x1)
        //   East rows next  (Drammen x1, Oslo x3)
        var firstThree = (await _ctx.Query<OtbRow>()
            .OrderByDescending(p => p.Region)
            .ThenBy(p => p.City)
            .ToListAsync())
            .Take(4)
            .Select(r => (r.Region, r.City))
            .ToArray();
        Assert.Equal(new[]
        {
            ("West", "Bergen"),
            ("West", "Bergen"),
            ("West", "Bergen"),
            ("West", "Stavanger"),
        }, firstThree);
    }

    [Table("OtbRow")]
    public sealed class OtbRow
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
        public int Tier { get; set; }
    }
}
