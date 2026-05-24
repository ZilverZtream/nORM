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
/// Pins <c>Select(...).Distinct()</c> where the projection deliberately
/// excludes the primary key. Silent-wrongness risk if DISTINCT is dropped
/// (every row leaks through) OR if the engine quietly attaches the PK to
/// the SELECT list so DISTINCT operates on (City, Id) tuples — both shapes
/// return more rows than the user expects.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqDistinctOnProjectionWithoutPkTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE DnpRow (Id INTEGER PRIMARY KEY, City TEXT NOT NULL, Region TEXT NOT NULL);
            -- 7 rows across 3 distinct cities and 2 distinct (City, Region) pairs:
            --   City distinct: {Oslo, Bergen, Stavanger} -> 3
            --   (City, Region) distinct: {(Oslo, East), (Oslo, West) -- nope, Oslo always East;
            --                              (Bergen, West), (Stavanger, West)} -> 3 actually
            -- Recheck: rows below give (City, Region) pairs:
            --   (Oslo, East) x3, (Bergen, West) x2, (Stavanger, West) x2
            --   -> 3 distinct cities, 3 distinct (City, Region) pairs.
            INSERT INTO DnpRow VALUES
              (1, 'Oslo',      'East'),
              (2, 'Oslo',      'East'),
              (3, 'Oslo',      'East'),
              (4, 'Bergen',    'West'),
              (5, 'Bergen',    'West'),
              (6, 'Stavanger', 'West'),
              (7, 'Stavanger', 'West');
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
    public async Task Distinct_on_single_string_column_returns_unique_values_only()
    {
        // 7 source rows, 3 distinct cities. If DISTINCT silently runs on
        // (City, Id) instead, the result would be 7 rows (every Id unique).
        // If DISTINCT is dropped entirely, also 7 rows.
        var cities = (await _ctx.Query<DnpRow>()
            .Select(p => p.City)
            .Distinct()
            .OrderBy(c => c)
            .ToListAsync()).ToArray();
        Assert.Equal(new[] { "Bergen", "Oslo", "Stavanger" }, cities);
    }

    [Fact]
    public async Task Distinct_on_anon_pair_returns_unique_value_combinations()
    {
        // (City, Region) projection -> 3 distinct pairs (the Oslo+East,
        // Bergen+West, Stavanger+West shapes from the seed). If the engine
        // quietly attached Id to the GROUP BY-equivalent of DISTINCT, the
        // result would still be 7 rows; a dropped DISTINCT also 7.
        var pairs = (await _ctx.Query<DnpRow>()
            .Select(p => new { p.City, p.Region })
            .Distinct()
            .OrderBy(p => p.City)
            .ToListAsync())
            .Select(p => (p.City, p.Region))
            .ToArray();
        Assert.Equal(new[] { ("Bergen", "West"), ("Oslo", "East"), ("Stavanger", "West") }, pairs);
    }

    [Table("DnpRow")]
    public sealed class DnpRow
    {
        [Key] public int Id { get; set; }
        public string City { get; set; } = string.Empty;
        public string Region { get; set; } = string.Empty;
    }
}
