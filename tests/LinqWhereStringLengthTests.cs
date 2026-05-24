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
/// Pins <c>Where(p =&gt; p.Name.Length op N)</c>. WHERE-side sister of
/// <see cref="LinqProjectionStringLengthTests"/>. SqliteProvider lowers
/// <c>string.Length</c> to <c>LENGTH(col)</c>; silent-wrongness if the
/// member access is dropped and the comparison runs against the bare
/// column string (SQLite then collates / compares the string against an
/// integer literal — never matches anything except by accident).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringLengthTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WsLRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO WsLRow VALUES
              (1, 'a'),
              (2, 'abc'),
              (3, 'abcdefghij'),
              (4, ''),
              (5, 'hello'),
              (6, 'world!');
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
    public async Task Where_string_length_greater_than_returns_only_longer_rows()
    {
        // Length > 4 -> 'abcdefghij' (10), 'hello' (5), 'world!' (6) -> Ids {3, 5, 6}.
        var ids = (await _ctx.Query<WsLRow>()
            .Where(p => p.Name.Length > 4)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 5, 6 }, ids);
    }

    [Fact]
    public async Task Where_string_length_equals_zero_matches_empty_string_row()
    {
        // Length == 0 should match only the empty-string row.
        // Silent-wrongness check: if LENGTH() is dropped and the comparison
        // becomes 'a' == 0 / 'abc' == 0 etc., zero rows match (SQLite affinity
        // coerces but never produces a true equality between a non-empty
        // string and the integer 0).
        var ids = (await _ctx.Query<WsLRow>()
            .Where(p => p.Name.Length == 0)
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4 }, ids);
    }

    [Fact]
    public async Task Where_string_length_with_and_filter_intersects_correctly()
    {
        // Length >= 3 AND name does NOT start with 'a':
        //   Length >= 3 -> Ids {2 'abc', 3 'abcdefghij', 5 'hello', 6 'world!'}
        //   minus starts-with 'a' -> {5, 6}
        var ids = (await _ctx.Query<WsLRow>()
            .Where(p => p.Name.Length >= 3 && !p.Name.StartsWith("a"))
            .OrderBy(p => p.Id)
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 5, 6 }, ids);
    }

    [Table("WsLRow")]
    public sealed class WsLRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
