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
/// Strict pins for instance-form <c>CompareTo</c> across int, decimal,
/// string, and DateTime. Sisters of the static Compare cluster (9ae9dab,
/// dc5f846, acc04c8, e975313). The instance form passes the receiver
/// as the first arg via TranslateFunction, so the existing static-form
/// switch entries should pick them up when registered by name.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionInstanceCompareToTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PictItem (
                Id INTEGER PRIMARY KEY,
                Iv INTEGER NOT NULL,
                Dv TEXT NOT NULL,
                Sv TEXT NOT NULL,
                Tv TEXT NOT NULL DEFAULT ''
            );
            INSERT INTO PictItem VALUES
                (1, 5, '3.0', 'banana', ''),
                (2, 4, '4.0', 'cherry', ''),
                (3, 1, '7.5', 'apple',  '');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PictItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_int_CompareTo_other_int_returns_sign_per_row()
    {
        var r = await _ctx.Query<PictItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = p.Iv.CompareTo(3) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.True(r[0].C > 0);    // 5 > 3
        Assert.True(r[1].C > 0);    // 4 > 3
        Assert.True(r[2].C < 0);    // 1 < 3
    }

    [Fact]
    public async Task Select_decimal_CompareTo_other_decimal_returns_sign_per_row()
    {
        var r = await _ctx.Query<PictItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = p.Dv.CompareTo(4.0m) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.True(r[0].C < 0);    // 3 < 4
        Assert.Equal(0, r[1].C);    // 4 == 4
        Assert.True(r[2].C > 0);    // 7.5 > 4
    }

    [Fact]
    public async Task Select_string_CompareTo_other_string_returns_sign_per_row()
    {
        var r = await _ctx.Query<PictItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, C = p.Sv.CompareTo("banana") }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.Equal(0, r[0].C);    // banana == banana
        Assert.True(r[1].C > 0);    // cherry > banana
        Assert.True(r[2].C < 0);    // apple < banana
    }

    // NOTE: DateTime CompareTo with a closure-captured DateTime constant is
    // not pinned here -- SelectClauseVisitor.FormatLiteral doesn't yet emit
    // a literal for DateTime in projection (separate limitation from
    // CompareTo translation). The provider has the DateTime CompareTo emit
    // wired (julianday-based, sister to DateTime.Compare) and would work
    // once the literal-formatter gap closes.

    [Table("PictItem")]
    public sealed class PictItem
    {
        [Key] public int Id { get; set; }
        public int Iv { get; set; }
        public decimal Dv { get; set; }
        public string Sv { get; set; } = string.Empty;
        public string Tv { get; set; } = string.Empty;
    }
}
