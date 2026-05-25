using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Configuration;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Strict pins for IEEE 754 predicates in WHERE: <c>double.IsInfinity</c>
/// and <c>double.IsFinite</c>. Filtering "give me the rows where the value
/// went to infinity" is a real diagnostic shape. Sister to the projection
/// pins; the provider's TranslateFunction is shared by SCV and ETSV so a
/// single switch covers both visitor paths, but this pin confirms WHERE
/// is actually wired and not silently falling back to client-eval.
///
/// NaN-storage is blocked by Microsoft.Data.Sqlite (see projection pin),
/// so IsNaN's predicate emit isn't exercised end-to-end here either.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereDoubleIeeePredicatesTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WdipItem (Id INTEGER PRIMARY KEY, X REAL NOT NULL);
            INSERT INTO WdipItem VALUES
                (1, 1.5),
                (2, 1e999),
                (3, -1e999),
                (4, 42.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WdipItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_double_IsInfinity_returns_only_infinity_rows()
    {
        var ids = await _ctx.Query<WdipItem>()
            .Where(p => double.IsInfinity(p.X))
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 2, 3 }, ids.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Where_double_IsFinite_returns_only_finite_rows()
    {
        var ids = await _ctx.Query<WdipItem>()
            .Where(p => double.IsFinite(p.X))
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1, 4 }, ids.Select(x => x.Id).ToArray());
    }

    [Fact]
    public async Task Where_double_IsPositiveInfinity_returns_only_positive_infinity_row()
    {
        var ids = await _ctx.Query<WdipItem>()
            .Where(p => double.IsPositiveInfinity(p.X))
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 2 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WdipItem")]
    public sealed class WdipItem
    {
        [Key] public int Id { get; set; }
        public double X { get; set; }
    }
}
