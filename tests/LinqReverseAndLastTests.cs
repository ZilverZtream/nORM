using System;
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
/// Exercises Reverse() + Last/LastOrDefault on an ordered queryable. Reverse must flip the
/// ORDER BY direction at the SQL level; Last/LastOrDefault must read the final row of the
/// ordered set, throwing the expected exceptions on empty results / no-match respectively.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqReverseAndLastTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE RvRow (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            INSERT INTO RvRow VALUES (1,'alpha'),(2,'bravo'),(3,'charlie'),(4,'delta');
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
    public async Task Reverse_flips_ordering_direction()
    {
        var ids = (await _ctx.Query<RvRow>().OrderBy(r => r.Id).Reverse().ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 4, 3, 2, 1 }, ids);
    }

    [Fact]
    public async Task LastAsync_returns_final_row_of_ordered_set()
    {
        var last = await _ctx.Query<RvRow>().OrderBy(r => r.Id).LastAsync();
        Assert.Equal(4, last.Id);
        Assert.Equal("delta", last.Name);
    }

    [Fact]
    public async Task LastOrDefaultAsync_returns_null_when_predicate_matches_nothing()
    {
        var miss = await _ctx.Query<RvRow>().OrderBy(r => r.Id)
            .Where(r => r.Name == "no-match").LastOrDefaultAsync();
        Assert.Null(miss);
    }

    [Table("RvRow")]
    public sealed class RvRow
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }
}
