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
/// Strict pin mirroring be14020 to the Where path: <c>name.Trim('!', '?')
/// == "hi"</c> -- common pattern for matching a value after stripping
/// punctuation noise.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringTrimCharArrayTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WstcaItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO WstcaItem VALUES
                (1, '!hi!'),
                (2, '?ok?'),
                (3, '?!!hi!?'),
                (4, 'hi');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WstcaItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_string_Trim_with_explicit_char_set_compares_to_literal()
    {
        // Trim('!','?') strips both chars; only rows whose stripped form
        // equals "hi" match -> Id 1, 3, 4 (Id 2 strips to "ok").
        var result = await _ctx.Query<WstcaItem>()
            .Where(p => p.Name.Trim('!', '?') == "hi")
            .OrderBy(p => p.Id)
            .ToListAsync();
        Assert.Equal(new[] { 1, 3, 4 }, result.Select(r => r.Id).ToArray());
    }

    [Table("WstcaItem")]
    public sealed class WstcaItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
