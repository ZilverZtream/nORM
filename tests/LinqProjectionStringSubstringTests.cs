using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins <c>p.Name.Substring(start, length)</c> and <c>p.Name.Substring(start)</c>
/// inside a Select projection. SqliteProvider lowers these to
/// <c>SUBSTR(col, start + 1, length)</c> (1-indexed). Silent-wrongness risks:
/// (1) start arg off-by-one (SQLite SUBSTR is 1-based, .NET Substring is
/// 0-based) — would shift every result by one character; (2) the length arg
/// silently dropped — returns the full tail from start instead of a fixed
/// window.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionStringSubstringTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PssRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO PssRow VALUES
              (1, 'Alpha'),
              (2, 'Bravo'),
              (3, 'Charlie'),
              (4, 'Delta');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider());
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Projection_substring_with_start_and_length_returns_fixed_window()
    {
        // Substring(1, 2) -> .NET semantics: skip 1 char, take 2 next.
        //   Alpha  -> lp
        //   Bravo  -> ra
        //   Charlie-> ha
        //   Delta  -> el
        // Silent-wrongness checks: off-by-one start would shift each one
        // character left or right; dropped length would return the full
        // tail ("lpha", "ravo", "harlie", "elta").
        var rows = (await _ctx.Query<PssRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Chunk = p.Name.Substring(1, 2) })
            .ToListAsync())
            .Select(r => (r.Id, r.Chunk))
            .ToArray();
        Assert.Equal(new[] { (1, "lp"), (2, "ra"), (3, "ha"), (4, "el") }, rows);
    }

    [Fact]
    public async Task Projection_substring_with_start_only_returns_full_tail()
    {
        // Substring(2) -> from index 2 to end.
        //   Alpha  -> pha
        //   Bravo  -> avo
        //   Charlie-> arlie
        //   Delta  -> lta
        var rows = (await _ctx.Query<PssRow>()
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id, Tail = p.Name.Substring(2) })
            .ToListAsync())
            .Select(r => (r.Id, r.Tail))
            .ToArray();
        Assert.Equal(new[] { (1, "pha"), (2, "avo"), (3, "arlie"), (4, "lta") }, rows);
    }

    [Table("PssRow")]
    public sealed class PssRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
