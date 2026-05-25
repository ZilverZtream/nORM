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
/// Strict pin for <c>string.Join(sep, params string[])</c> inside WHERE.
/// Sister to the SCV projection fix (5f48dc3). Real-world filter:
///   <c>Where(p => string.Join("-", p.A, p.B) == "foo-bar")</c>
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereStringJoinTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WsjItem (Id INTEGER PRIMARY KEY, A TEXT NOT NULL, B TEXT NOT NULL);
            INSERT INTO WsjItem VALUES
                (1, 'foo', 'bar'),
                (2, 'x', 'y'),
                (3, 'foo', 'baz');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<WsjItem>().HasKey(i => i.Id)
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_string_Join_two_columns_equals_constant_returns_match()
    {
        var ids = await _ctx.Query<WsjItem>()
            .Where(p => string.Join("-", p.A, p.B) == "foo-bar")
            .OrderBy(p => p.Id)
            .Select(p => new { p.Id })
            .ToListAsync();
        Assert.Equal(new[] { 1 }, ids.Select(x => x.Id).ToArray());
    }

    [Table("WsjItem")]
    public sealed class WsjItem
    {
        [Key] public int Id { get; set; }
        public string A { get; set; } = string.Empty;
        public string B { get; set; } = string.Empty;
    }
}
