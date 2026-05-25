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
/// Strict pins for the static method-form decimal arithmetic primitives:
/// <c>decimal.Add / Subtract / Multiply / Divide / Negate / Remainder</c>.
/// Most code uses operators, but generated code (e.g. via Expression API
/// callers, dynamic LINQ helpers) frequently emits the static form.
/// These map directly to the SQL operators on REAL.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDecimalArithmeticStaticsTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdasItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO PdasItem VALUES
                (1, '6.0', '2.0'),
                (2, '7.5', '0.5'),
                (3, '-3.0', '2.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdasItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_decimal_Add_two_columns_returns_sum_per_row()
    {
        var r = await _ctx.Query<PdasItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = decimal.Add(p.A, p.B) }).ToListAsync();
        Assert.Equal(new[] { 8.0m, 8.0m, -1.0m }, r.Select(x => x.V).ToArray());
    }

    [Fact]
    public async Task Select_decimal_Subtract_two_columns_returns_difference_per_row()
    {
        var r = await _ctx.Query<PdasItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = decimal.Subtract(p.A, p.B) }).ToListAsync();
        Assert.Equal(new[] { 4.0m, 7.0m, -5.0m }, r.Select(x => x.V).ToArray());
    }

    [Fact]
    public async Task Select_decimal_Multiply_two_columns_returns_product_per_row()
    {
        var r = await _ctx.Query<PdasItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = decimal.Multiply(p.A, p.B) }).ToListAsync();
        Assert.Equal(new[] { 12.0m, 3.75m, -6.0m }, r.Select(x => x.V).ToArray());
    }

    [Fact]
    public async Task Select_decimal_Divide_two_columns_returns_quotient_per_row()
    {
        var r = await _ctx.Query<PdasItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = decimal.Divide(p.A, p.B) }).ToListAsync();
        Assert.Equal(new[] { 3.0m, 15.0m, -1.5m }, r.Select(x => x.V).ToArray());
    }

    [Fact]
    public async Task Select_decimal_Negate_single_column_returns_negation_per_row()
    {
        var r = await _ctx.Query<PdasItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, V = decimal.Negate(p.A) }).ToListAsync();
        Assert.Equal(new[] { -6.0m, -7.5m, 3.0m }, r.Select(x => x.V).ToArray());
    }

    [Table("PdasItem")]
    public sealed class PdasItem
    {
        [Key] public int Id { get; set; }
        public decimal A { get; set; }
        public decimal B { get; set; }
    }
}
