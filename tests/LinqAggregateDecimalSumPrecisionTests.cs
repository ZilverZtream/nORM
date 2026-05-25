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
/// Probes <c>Sum</c> over a decimal column for precision loss. SQLite has
/// no native decimal type and Microsoft.Data.Sqlite stores decimal as
/// TEXT; arithmetic on TEXT-stored decimals coerces to REAL (double) and
/// loses precision past ~15-17 significant digits. This pin captures the
/// silent-wrongness shape for currency-style sums.
///
/// Note: nORM's CountAsync/SumAsync should call Microsoft.Data.Sqlite's
/// GetDecimal which DOES restore decimal precision IF the underlying
/// SQL produced text -- but SQL SUM(decimal_col) returns a REAL by SQLite
/// rules, then read as decimal loses precision. This is a real issue
/// affecting financial applications.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqAggregateDecimalSumPrecisionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PadsItem (Id INTEGER PRIMARY KEY, Amount TEXT NOT NULL);
            INSERT INTO PadsItem VALUES
                (1, '0.01'),
                (2, '0.02'),
                (3, '0.03'),
                (4, '0.04'),
                (5, '0.05'),
                (6, '0.06'),
                (7, '0.07'),
                (8, '0.08'),
                (9, '0.09'),
                (10, '0.10');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PadsItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task SumAsync_decimal_column_returns_exact_decimal_total()
    {
        // 0.01 + 0.02 + ... + 0.10 = 0.55 exactly (decimal).
        // Silent-wrongness shape: REAL arithmetic accumulates rounding
        // errors; e.g. (0.01+0.02+0.03+0.04+0.05+0.06+0.07+0.08+0.09+0.10)
        // in double can drift to 0.5499999999999999 or 0.55000000000000004.
        var total = await _ctx.Query<PadsItem>().SumAsync(p => p.Amount);
        Assert.Equal(0.55m, total);
    }

    [Table("PadsItem")]
    public sealed class PadsItem
    {
        [Key] public int Id { get; set; }
        public decimal Amount { get; set; }
    }
}
