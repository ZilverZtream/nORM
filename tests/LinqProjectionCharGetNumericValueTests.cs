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
/// Strict pin + implement-first for <c>char.GetNumericValue</c> in
/// projection AND Where. Returns the numeric value of a digit char as
/// double; -1.0 for non-digits. Sister to the char.IsX set.
///
/// ASCII subset: codepoint - 48 for '0'-'9'; -1 otherwise. Full Unicode
/// numeric chars are not portable across providers (matches char.IsLetter
/// scope).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionCharGetNumericValueTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PcgnvItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PcgnvItem VALUES
                (1, '7seven'),
                (2, '0zero'),
                (3, '9nine'),
                (4, 'alpha');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<PcgnvItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_char_GetNumericValue_first_char_returns_double_per_row()
    {
        var result = await _ctx.Query<PcgnvItem>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, N = char.GetNumericValue(p.Name[0]) })
            .ToListAsync();
        Assert.Equal(4, result.Count);
        Assert.Equal(7.0, result[0].N);
        Assert.Equal(0.0, result[1].N);
        Assert.Equal(9.0, result[2].N);
        Assert.Equal(-1.0, result[3].N);
    }

    [Fact]
    public async Task Where_char_GetNumericValue_compared_to_threshold_filters_rows()
    {
        // GetNumericValue >= 5 -> digit chars '5'..'9' -> {1 ('7'), 3 ('9')}.
        var result = await _ctx.Query<PcgnvItem>()
            .Where(p => char.GetNumericValue(p.Name[0]) >= 5)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("PcgnvItem")]
    public sealed class PcgnvItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
