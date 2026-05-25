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
/// Pins <c>q.OrderBy(Id).Take(N).Reverse()</c>: the Reverse must reverse
/// only the windowed N rows, not the whole table. Sister of the
/// post-Take/Skip family of fixes — Reverse is a terminal-style operator
/// that flips ORDER BY direction in the translator.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqReverseAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE RatRow (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL);
            INSERT INTO RatRow VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 999);
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
    public async Task Reverse_after_take_reverses_only_the_windowed_rows()
    {
        // OrderBy(Id).Take(3) → rows 1,2,3 (Id ascending). Reverse → Ids [3,2,1].
        // Naive translation would flip the inner OrderBy to DESC, picking rows 5,4,3
        // (top 3 by Id desc) — wrong.
        var ids = (await _ctx.Query<RatRow>()
            .OrderBy(r => r.Id)
            .Take(3)
            .Reverse()
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 3, 2, 1 }, ids);
    }

    [Fact]
    public async Task Reverse_after_skip_reverses_only_the_skipped_subset()
    {
        // OrderBy(Id).Skip(2) → rows 3,4,5. Reverse → Ids [5,4,3].
        var ids = (await _ctx.Query<RatRow>()
            .OrderBy(r => r.Id)
            .Skip(2)
            .Reverse()
            .ToListAsync())
            .Select(r => r.Id).ToArray();
        Assert.Equal(new[] { 5, 4, 3 }, ids);
    }

    [Table("RatRow")]
    public sealed class RatRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
    }
}
