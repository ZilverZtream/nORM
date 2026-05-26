using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// AsAsyncEnumerable with GroupJoin: groups are streamed one at a time
/// rather than buffered into a List first.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupJoinAsyncEnumerableTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    [Table("GjaeParent")]
    public sealed class GjaeParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("GjaeChild")]
    public sealed class GjaeChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE GjaeParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE GjaeChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO GjaeParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            INSERT INTO GjaeChild  VALUES (1,1,'a1'),(2,1,'a2'),(3,2,'b1');
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
    public async Task GroupJoin_AsAsyncEnumerable_yields_one_group_per_outer_row()
    {
        var names = new List<string>();
        var counts = new Dictionary<string, int>();

        var query = _ctx.Query<GjaeParent>()
            .GroupJoin(
                _ctx.Query<GjaeChild>(),
                p => p.Id,
                c => c.ParentId,
                (p, cs) => new { Parent = p.Name, ChildCount = cs.Count() });

        await foreach (var row in query.AsAsyncEnumerable())
        {
            names.Add(row.Parent);
            counts[row.Parent] = row.ChildCount;
        }

        Assert.Equal(3, names.Count);
        Assert.Equal(2, counts["Alice"]);
        Assert.Equal(1, counts["Bob"]);
        Assert.Equal(0, counts["Carol"]);
    }

    [Fact]
    public async Task GroupJoin_AsAsyncEnumerable_child_collection_is_populated()
    {
        var results = new List<(string Parent, List<string> Tags)>();

        var query = _ctx.Query<GjaeParent>()
            .GroupJoin(
                _ctx.Query<GjaeChild>(),
                p => p.Id,
                c => c.ParentId,
                (p, cs) => new { Parent = p, Children = cs.ToList() });

        await foreach (var row in query.AsAsyncEnumerable())
        {
            results.Add((row.Parent.Name, row.Children.Select(c => c.Tag).ToList()));
        }

        Assert.Equal(3, results.Count);

        var alice = results.First(r => r.Parent == "Alice");
        Assert.Equal(2, alice.Tags.Count);
        Assert.Contains("a1", alice.Tags);
        Assert.Contains("a2", alice.Tags);

        var bob = results.First(r => r.Parent == "Bob");
        Assert.Single(bob.Tags);
        Assert.Equal("b1", bob.Tags[0]);

        Assert.Empty(results.First(r => r.Parent == "Carol").Tags);
    }

    [Fact]
    public async Task GroupJoin_AsAsyncEnumerable_matches_ToListAsync_result()
    {
        var streamedNames = new List<string>();
        await foreach (var row in _ctx.Query<GjaeParent>()
            .GroupJoin(
                _ctx.Query<GjaeChild>(),
                p => p.Id, c => c.ParentId,
                (p, cs) => new { p.Name, Count = cs.Count() })
            .AsAsyncEnumerable())
        {
            streamedNames.Add(row.Name);
        }

        var listedNames = (await _ctx.Query<GjaeParent>()
            .GroupJoin(
                _ctx.Query<GjaeChild>(),
                p => p.Id, c => c.ParentId,
                (p, cs) => new { p.Name, Count = cs.Count() })
            .ToListAsync())
            .Select(r => r.Name)
            .ToList();

        Assert.Equal(listedNames.OrderBy(n => n), streamedNames.OrderBy(n => n));
    }
}
