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
/// Strict pin + implement-first for <c>string.PadLeft</c> / <c>PadRight</c>
/// in projection. Common formatting need (CSV columns, fixed-width report
/// rows, currency alignment). SQLite has no built-in REPLICATE, but the
/// classic <c>hex(zeroblob(n))</c> + <c>replace</c> idiom produces N copies
/// of a single char which can be prefixed/suffixed.
///
/// Silent-wrongness shapes:
///   * PadLeft collapsing to PadRight -> alignment inverted; right-aligned
///     numeric columns become left-aligned and look broken.
///   * Padding char default ' ' silently changing -> '00012' vs '   12'.
///   * Width less-than-input case truncating instead of returning input
///     unchanged -- .NET semantics return the input untouched.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringPadTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PspItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PspItem VALUES
                (1, 'a'),
                (2, 'foo'),
                (3, 'longername'),
                (4, '');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PspItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_string_PadLeft_default_space_right_aligns_within_width()
    {
        // PadLeft(5): 'a' -> '    a', 'foo' -> '  foo', 'longername' -> 'longername' (>=5 unchanged),
        // '' -> '     '.
        var result = await _ctx.Query<PspItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.PadLeft(5))
            .ToListAsync();
        Assert.Equal(new[] { "    a", "  foo", "longername", "     " }, result.ToArray());
    }

    [Fact]
    public async Task Select_string_PadRight_default_space_left_aligns_within_width()
    {
        // PadRight(5): 'a' -> 'a    ', 'foo' -> 'foo  ', 'longername' -> 'longername' (unchanged),
        // '' -> '     '.
        var result = await _ctx.Query<PspItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.PadRight(5))
            .ToListAsync();
        Assert.Equal(new[] { "a    ", "foo  ", "longername", "     " }, result.ToArray());
    }

    [Fact]
    public async Task Select_string_PadLeft_with_zero_fill_char_right_aligns_with_zeros()
    {
        // PadLeft(5, '0'): 'a' -> '0000a', 'foo' -> '00foo', 'longername' -> unchanged,
        // '' -> '00000'.
        var result = await _ctx.Query<PspItem>()
            .OrderBy(p => p.Id)
            .Select(p => p.Name.PadLeft(5, '0'))
            .ToListAsync();
        Assert.Equal(new[] { "0000a", "00foo", "longername", "00000" }, result.ToArray());
    }

    [Table("PspItem")]
    public sealed class PspItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
