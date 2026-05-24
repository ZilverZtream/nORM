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
/// Verifies the ab3907d ToString translation extends beyond <c>int</c>
/// to other non-string non-enum receiver types. The TranslatabilityAnalyzer
/// + SelectClauseVisitor changes only check "receiver type != string AND
/// non-enum", so they should admit decimal, Guid, double, long, DateTime
/// uniformly -- but the provider's <c>GetToStringSql</c> only knows how to
/// emit CAST(x AS TEXT) which may format-differ from .NET conventions
/// (e.g. SQLite CAST(1.5 AS TEXT) = "1.5" but CAST(1.50 AS TEXT) loses
/// trailing zero; Guid round-trips as text identically).
///
/// Pins:
///   * long.ToString -- numeric, straightforward
///   * Guid.ToString -- text already, CAST is identity
///   * decimal.ToString -- format-preserving on SQLite (TEXT storage)
///   * double.ToString -- float format pin
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionToStringNonIntTypesTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    private static readonly Guid Guid1 = new("11111111-1111-1111-1111-111111111111");
    private static readonly Guid Guid2 = new("22222222-2222-2222-2222-222222222222");

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = $"""
            CREATE TABLE PtsItem (Id INTEGER PRIMARY KEY, LongVal INTEGER NOT NULL, GuidVal TEXT NOT NULL, DecimalVal TEXT NOT NULL);
            INSERT INTO PtsItem VALUES
                (1, 10000000000, '{Guid1}', '12.5'),
                (2, 20000000000, '{Guid2}', '99.99');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PtsItem>().HasKey(p => p.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_long_ToString_returns_string_representations()
    {
        var result = await _ctx.Query<PtsItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.LongVal.ToString())
            .ToListAsync();
        Assert.Equal(new[] { "10000000000", "20000000000" }, result.ToArray());
    }

    [Fact]
    public async Task Select_Guid_ToString_returns_string_representations()
    {
        var result = await _ctx.Query<PtsItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.GuidVal.ToString())
            .ToListAsync();
        Assert.Equal(new[] { Guid1.ToString(), Guid2.ToString() }, result.ToArray());
    }

    [Fact]
    public async Task Select_decimal_ToString_returns_string_representations()
    {
        var result = await _ctx.Query<PtsItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.DecimalVal.ToString())
            .ToListAsync();
        Assert.Equal(new[] { "12.5", "99.99" }, result.ToArray());
    }

    [Fact]
    public async Task Select_string_concat_long_ToString_works_in_anonymous_projection()
    {
        // Composite projection: anon { Id, "Long=" + LongVal.ToString() }.
        // Exercises the SCV path that walks NewExpression members.
        var result = await _ctx.Query<PtsItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Label = "Long=" + p.LongVal.ToString() })
            .ToListAsync();
        Assert.Equal(2, result.Count);
        Assert.Equal("Long=10000000000", result[0].Label);
        Assert.Equal("Long=20000000000", result[1].Label);
    }

    [Table("PtsItem")]
    public sealed class PtsItem
    {
        [Key] public int Id { get; set; }
        public long LongVal { get; set; }
        public Guid GuidVal { get; set; }
        public decimal DecimalVal { get; set; }
    }
}
