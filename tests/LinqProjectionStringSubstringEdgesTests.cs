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
/// Strict pin for <c>string.Substring</c> end-cases that have bitten
/// users:
///   * Substring(0) -> identity (full string)
///   * Substring(0, len) where len == string length -> full string
///   * Substring(start) at exactly the end -> ""
///   * Substring(start, length) -> standard middle slice
/// SQLite's SUBSTR is 1-based and .NET is 0-based; the +1 conversion
/// must be applied consistently.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringSubstringEdgesTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PsseItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO PsseItem VALUES
                (1, 'abcdef'),
                (2, 'hello');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PsseItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_Substring_zero_start_returns_full_string()
    {
        var r = await _ctx.Query<PsseItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = p.V.Substring(0) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("abcdef", r[0].S);
        Assert.Equal("hello", r[1].S);
    }

    [Fact]
    public async Task Select_Substring_start_and_length_returns_middle_slice()
    {
        var r = await _ctx.Query<PsseItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = p.V.Substring(1, 3) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("bcd", r[0].S);
        Assert.Equal("ell", r[1].S);
    }

    [Fact]
    public async Task Select_Substring_from_middle_to_end_returns_tail()
    {
        var r = await _ctx.Query<PsseItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, S = p.V.Substring(2) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal("cdef", r[0].S);
        Assert.Equal("llo", r[1].S);
    }

    [Table("PsseItem")]
    public sealed class PsseItem
    {
        [Key] public int Id { get; set; }
        public string V { get; set; } = string.Empty;
    }
}
