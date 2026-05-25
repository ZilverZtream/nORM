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
/// Strict pin for <c>string.Contains/StartsWith/EndsWith(s, StringComparison)</c>
/// in projection. ETSV already handles these in WHERE per the
/// comparisonArg block at the head of ExpressionToSqlVisitor.cs; the
/// projection mirror is needed for callers projecting a boolean flag
/// column.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringContainsModeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PscoItem (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO PscoItem VALUES
                (1, 'Hello World'),
                (2, 'goodbye world'),
                (3, 'WORLD wide');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PscoItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_Contains_OrdinalIgnoreCase_flags_case_insensitive_matches()
    {
        var r = await _ctx.Query<PscoItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, F = p.V.Contains("world", StringComparison.OrdinalIgnoreCase) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.True(r[0].F);    // "World"
        Assert.True(r[1].F);    // "world"
        Assert.True(r[2].F);    // "WORLD"
    }

    [Fact]
    public async Task Select_string_StartsWith_OrdinalIgnoreCase_flags_case_insensitive_prefix()
    {
        var r = await _ctx.Query<PscoItem>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, F = p.V.StartsWith("hello", StringComparison.OrdinalIgnoreCase) }).ToListAsync();
        Assert.Equal(3, r.Count);
        Assert.True(r[0].F);    // "Hello"
        Assert.False(r[1].F);
        Assert.False(r[2].F);
    }

    [Table("PscoItem")]
    public sealed class PscoItem
    {
        [Key] public int Id { get; set; }
        public string V { get; set; } = string.Empty;
    }
}
