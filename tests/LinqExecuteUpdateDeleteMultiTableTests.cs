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
/// Pins the deterministic error message for <c>ExecuteUpdateAsync</c> /
/// <c>ExecuteDeleteAsync</c> over a query that touches more than one table
/// (typically via a Join). The pre-existing message was a single line —
/// upgrade to surface the architectural reason and a concrete workaround
/// (correlated subquery in WHERE / Contains over the join result) the same
/// way the AsAsyncEnumerable + Include error did.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqExecuteUpdateDeleteMultiTableTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE EumLeft  (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE EumRight (Id INTEGER PRIMARY KEY, LeftId INTEGER NOT NULL, Flag INTEGER NOT NULL);
            INSERT INTO EumLeft  VALUES (1,'a'),(2,'b');
            INSERT INTO EumRight VALUES (10, 1, 1),(20, 2, 0);
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
    public async Task ExecuteDeleteAsync_over_joined_query_throws_with_actionable_message()
    {
        var ex = await Assert.ThrowsAnyAsync<Exception>(async () =>
        {
            await ((INormQueryable<EumLeft>)_ctx.Query<EumLeft>())
                .Join(_ctx.Query<EumRight>(), l => l.Id, r => r.LeftId, (l, r) => l)
                .ExecuteDeleteAsync();
        });
        // Must identify the constraint and point at the correlated-subquery workaround.
        Assert.Contains("ExecuteDelete", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Contains",      ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Table("EumLeft")]
    public sealed class EumLeft
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("EumRight")]
    public sealed class EumRight
    {
        [Key] public int Id { get; set; }
        public int LeftId { get; set; }
        public int Flag { get; set; }
    }
}
