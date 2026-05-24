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
/// Verifies Union / Intersect / Except over projection columns return the correct
/// combined result. Each operator runs against two side-by-side anonymous projections so
/// the row shapes match.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSetOperationProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SoLeft  (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL);
            CREATE TABLE SoRight (Id INTEGER PRIMARY KEY, Tag TEXT NOT NULL);
            INSERT INTO SoLeft  VALUES (1,'a'),(2,'b'),(3,'c');
            INSERT INTO SoRight VALUES (1,'b'),(2,'c'),(3,'d');
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
    public async Task Union_combines_distinct_tags_from_both_sides()
    {
        var tags = (await _ctx.Query<SoLeft>().Select(l => l.Tag)
            .Union(_ctx.Query<SoRight>().Select(r => r.Tag))
            .ToListAsync())
            .OrderBy(t => t).ToArray();
        Assert.Equal(new[] { "a", "b", "c", "d" }, tags);
    }

    [Fact]
    public async Task Intersect_returns_only_tags_present_in_both_sides()
    {
        var tags = (await _ctx.Query<SoLeft>().Select(l => l.Tag)
            .Intersect(_ctx.Query<SoRight>().Select(r => r.Tag))
            .ToListAsync())
            .OrderBy(t => t).ToArray();
        Assert.Equal(new[] { "b", "c" }, tags);
    }

    [Fact]
    public async Task Except_returns_tags_only_in_left_side()
    {
        var tags = (await _ctx.Query<SoLeft>().Select(l => l.Tag)
            .Except(_ctx.Query<SoRight>().Select(r => r.Tag))
            .ToListAsync())
            .OrderBy(t => t).ToArray();
        Assert.Equal(new[] { "a" }, tags);
    }

    [Table("SoLeft")]
    public sealed class SoLeft
    {
        [Key] public int Id { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    [Table("SoRight")]
    public sealed class SoRight
    {
        [Key] public int Id { get; set; }
        public string Tag { get; set; } = string.Empty;
    }
}
