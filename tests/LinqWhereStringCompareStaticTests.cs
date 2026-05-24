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
/// Strict pin + implement-first for <c>string.Compare(a, b)</c> static method
/// in Where. .NET's string.Compare returns -1 / 0 / +1; the canonical
/// translatable shape is <c>string.Compare(col, "M") &lt; 0</c> meaning
/// "names alphabetically before M". The Where translator must lower the
/// Compare-relop pair to a SQL relational op on the strings directly:
///
///   string.Compare(a, b) < 0   ->  a &lt; b
///   string.Compare(a, b) > 0   ->  a &gt; b
///   string.Compare(a, b) == 0  ->  a = b
///
/// Silent-wrongness shapes:
///   * Untranslated -> client-eval error or worse, materializes table.
///   * Wrong direction -> Compare ... &lt; 0 becomes "names after M".
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringCompareStaticTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WscItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO WscItem VALUES
                (1, 'Alpha'),
                (2, 'Charlie'),
                (3, 'Mike'),
                (4, 'Tango'),
                (5, 'Zulu');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WscItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_string_Compare_less_than_zero_filters_alphabetically_before()
    {
        // string.Compare(Name, "Mike") < 0 -> names alphabetically before "Mike" ->
        // {1 Alpha, 2 Charlie}.
        var result = await _ctx.Query<WscItem>()
            .Where(p => string.Compare(p.Name, "Mike") < 0)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 2 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_string_Compare_greater_than_zero_filters_alphabetically_after()
    {
        // string.Compare(Name, "Mike") > 0 -> {4 Tango, 5 Zulu}.
        var result = await _ctx.Query<WscItem>()
            .Where(p => string.Compare(p.Name, "Mike") > 0)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 4, 5 }, result.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Where_string_Compare_equal_zero_filters_exact_match()
    {
        var result = await _ctx.Query<WscItem>()
            .Where(p => string.Compare(p.Name, "Mike") == 0)
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 3 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WscItem")]
    public sealed class WscItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
