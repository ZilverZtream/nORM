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
/// Strict pin for <c>decimal.Compare(a, b)</c> in projection. .NET
/// returns -1/0/1 indicating less-than/equal/greater-than. SQLite has
/// SIGN(a - b) which yields exactly those three values for non-NaN
/// inputs. Sister to decimal.Add/Subtract/etc just landed (173b847).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDecimalCompareTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdcItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO PdcItem VALUES
                (1, '5.0', '3.0'),
                (2, '4.0', '4.0'),
                (3, '-1.0', '7.5');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PdcItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_decimal_Compare_two_columns_returns_sign_per_row()
    {
        var r = await _ctx.Query<PdcItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = decimal.Compare(p.A, p.B) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(1, r[0].C);    // 5 > 3 -> 1
        Assert.Equal(0, r[1].C);    // 4 == 4 -> 0
        Assert.Equal(-1, r[2].C);   // -1 < 7.5 -> -1
    }

    [Table("PdcItem")]
    public sealed class PdcItem
    {
        [Key] public int Id { get; set; }
        public decimal A { get; set; }
        public decimal B { get; set; }
    }
}
