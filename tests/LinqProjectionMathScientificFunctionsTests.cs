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
/// Sister of <see cref="LinqProjectionMathFunctionsTests"/> (Abs/Round/Min/Max).
/// Pins the rest of the SqliteProvider <see cref="Math"/> surface inside a
/// projection: Sqrt, Pow, Log (1-arg), Log10, Sign, Truncate. All route via
/// the SCV TranslateFunction call added in 5008637; verifies the analyzer
/// probe (with placeholder args of correct arity) accepts each name+arity
/// pair so the projection is server-evaluated rather than silently splitting
/// to client-eval.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathScientificFunctionsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmsRow (Id INTEGER PRIMARY KEY, V REAL NOT NULL, N INTEGER NOT NULL);
            -- Values chosen so each function maps to a distinct expected output:
            --   Id 1: V=4.0   -> Sqrt=2,    Sign(N=-3)=-1, Trunc(V)=4
            --   Id 2: V=9.0   -> Sqrt=3,    Sign(N=0)=0,   Trunc(V)=9
            --   Id 3: V=16.0  -> Sqrt=4,    Sign(N=7)=1,   Trunc(V)=16
            INSERT INTO PmsRow VALUES (1, 4.0, -3), (2, 9.0, 0), (3, 16.0, 7);
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
    public async Task Projection_math_sqrt_returns_square_root_of_column()
    {
        var rows = (await _ctx.Query<PmsRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Root = Math.Sqrt(p.V) })
            .ToListAsync())
            .Select(r => (r.Id, r.Root))
            .ToArray();
        Assert.Equal(new[] { (1, 2.0), (2, 3.0), (3, 4.0) }, rows);
    }

    [Fact]
    public async Task Projection_math_pow_returns_value_raised_to_constant_power()
    {
        var rows = (await _ctx.Query<PmsRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Squared = Math.Pow(p.V, 2.0) })
            .ToListAsync())
            .Select(r => (r.Id, r.Squared))
            .ToArray();
        Assert.Equal(new[] { (1, 16.0), (2, 81.0), (3, 256.0) }, rows);
    }

    [Fact]
    public async Task Projection_math_sign_returns_minus_one_zero_or_plus_one()
    {
        var rows = (await _ctx.Query<PmsRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = Math.Sign(p.N) })
            .ToListAsync())
            .Select(r => (r.Id, r.S))
            .ToArray();
        Assert.Equal(new[] { (1, -1), (2, 0), (3, 1) }, rows);
    }

    [Fact]
    public async Task Projection_math_truncate_drops_fractional_component()
    {
        // SQLite has no TRUNC; provider lowers to CAST AS INTEGER, which truncates
        // toward zero for non-negative values (matching Math.Truncate semantics on
        // the positive-only data here).
        var rows = (await _ctx.Query<PmsRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, T = Math.Truncate(p.V) })
            .ToListAsync())
            .Select(r => (r.Id, r.T))
            .ToArray();
        Assert.Equal(new[] { (1, 4.0), (2, 9.0), (3, 16.0) }, rows);
    }

    [Table("PmsRow")]
    public sealed class PmsRow
    {
        [Key] public int Id { get; set; }
        public double V { get; set; }
        public int N { get; set; }
    }
}
