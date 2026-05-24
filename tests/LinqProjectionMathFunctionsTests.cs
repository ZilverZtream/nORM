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
/// Pins <see cref="Math"/> function calls inside a <c>Select</c> projection —
/// should lower to provider SQL functions (ABS, ROUND, MIN, MAX). Sister of
/// the <see cref="LinqWhereMathFunctionsTests"/> WHERE-side coverage; the
/// projection emitter is a different visitor (SelectClauseVisitor) than the
/// predicate one (ExpressionToSqlVisitor), so a working WHERE-side doesn't
/// imply a working projection-side. Silent-wrongness risk if the wrapper is
/// dropped and the bare column flows into the row data.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionMathFunctionsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PmRow (Id INTEGER PRIMARY KEY, A INTEGER NOT NULL, B INTEGER NOT NULL, Amount REAL NOT NULL);
            -- A mixes sign so |A| and A diverge; Amount has fractions to exercise ROUND.
            INSERT INTO PmRow VALUES
              (1,  -8,  3,  1.4),
              (2,   2,  9,  2.6),
              (3,  -3,  1,  3.5),
              (4,  10,  5,  4.4),
              (5,  -6,  7,  5.5);
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
    public async Task Projection_math_abs_returns_absolute_value_not_signed_value()
    {
        // Silent-wrongness: if Math.Abs is dropped, AbsA would echo signed A and the
        // tuple comparison would fail for rows 1, 3, 5 (negative source values).
        var rows = (await _ctx.Query<PmRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, AbsA = Math.Abs(p.A) })
            .ToListAsync())
            .Select(r => (r.Id, r.AbsA))
            .ToArray();
        Assert.Equal(new[] { (1, 8), (2, 2), (3, 3), (4, 10), (5, 6) }, rows);
    }

    [Fact]
    public async Task Projection_math_round_returns_rounded_double()
    {
        var rows = (await _ctx.Query<PmRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Rounded = Math.Round(p.Amount) })
            .ToListAsync())
            .Select(r => (r.Id, r.Rounded))
            .ToArray();
        // SQLite ROUND uses banker's-rounding semantics for .5 → nearest even.
        // 1.4→1, 2.6→3, 3.5→4 (banker's even), 4.4→4, 5.5→6 (banker's even).
        Assert.Equal(new[] { (1, 1.0), (2, 3.0), (3, 4.0), (4, 4.0), (5, 6.0) }, rows);
    }

    [Fact]
    public async Task Projection_math_min_two_columns_returns_smaller_of_pair()
    {
        var rows = (await _ctx.Query<PmRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Smaller = Math.Min(p.A, p.B) })
            .ToListAsync())
            .Select(r => (r.Id, r.Smaller))
            .ToArray();
        // min(-8,3)=-8, min(2,9)=2, min(-3,1)=-3, min(10,5)=5, min(-6,7)=-6.
        Assert.Equal(new[] { (1, -8), (2, 2), (3, -3), (4, 5), (5, -6) }, rows);
    }

    [Fact]
    public async Task Projection_math_max_two_columns_returns_larger_of_pair()
    {
        var rows = (await _ctx.Query<PmRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Larger = Math.Max(p.A, p.B) })
            .ToListAsync())
            .Select(r => (r.Id, r.Larger))
            .ToArray();
        Assert.Equal(new[] { (1, 3), (2, 9), (3, 1), (4, 10), (5, 7) }, rows);
    }

    [Table("PmRow")]
    public sealed class PmRow
    {
        [Key] public int Id { get; set; }
        public int A { get; set; }
        public int B { get; set; }
        public double Amount { get; set; }
    }
}
