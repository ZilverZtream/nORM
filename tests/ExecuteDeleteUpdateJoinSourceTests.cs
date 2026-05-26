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
/// ExecuteDeleteAsync and ExecuteUpdateAsync against join-sourced queries.
///
/// A join source is translated to a portably-safe IN subquery:
///   DELETE FROM Outer WHERE pk IN (SELECT T0.pk FROM Outer T0 JOIN Inner T1 ON ...)
///   UPDATE Outer SET ... WHERE pk IN (SELECT T0.pk FROM Outer T0 JOIN Inner T1 ON ...)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ExecuteDeleteUpdateJoinSourceTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    [Table("EduParent")]
    public sealed class EduParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool Archived { get; set; }
    }

    [Table("EduChild")]
    public sealed class EduChild
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
            CREATE TABLE EduParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, Archived INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE EduChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO EduParent VALUES (1,'Alice',0),(2,'Bob',0),(3,'Carol',0);
            INSERT INTO EduChild  VALUES (1,1,'a1'),(2,1,'a2'),(3,2,'b1');
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
    public async Task ExecuteDeleteAsync_with_inner_join_deletes_outer_rows_matched_by_join()
    {
        // Delete parents that have at least one child (Alice=1 and Bob=2); Carol (3) has no child.
        var deleted = await _ctx.Query<EduParent>()
            .Join(_ctx.Query<EduChild>(),
                  p => p.Id, c => c.ParentId,
                  (p, c) => p)
            .ExecuteDeleteAsync();

        // SQLite does NOT deduplicate the IN list — Alice matches 2 child rows but is deleted once.
        // The DISTINCT-free IN subquery still deletes each parent at most once (IN semantics).
        var remaining = await _ctx.Query<EduParent>().ToListAsync();
        Assert.Single(remaining);
        Assert.Equal("Carol", remaining[0].Name);
    }

    [Fact]
    public async Task ExecuteDeleteAsync_with_join_and_where_filters_correctly()
    {
        // Only delete parents joined to a child with Tag starting with 'b' → only Bob.
        await _ctx.Query<EduParent>()
            .Join(_ctx.Query<EduChild>(),
                  p => p.Id, c => c.ParentId,
                  (p, c) => p)
            .Where(p => p.Name == "Bob")
            .ExecuteDeleteAsync();

        var names = (await _ctx.Query<EduParent>().ToListAsync())
                        .Select(p => p.Name).OrderBy(n => n).ToArray();
        Assert.Equal(new[] { "Alice", "Carol" }, names);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_with_inner_join_updates_outer_rows_matched_by_join()
    {
        // Archive parents that have children (Alice and Bob); Carol should remain unarchived.
        await _ctx.Query<EduParent>()
            .Join(_ctx.Query<EduChild>(),
                  p => p.Id, c => c.ParentId,
                  (p, c) => p)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Archived, true));

        var parents = await _ctx.Query<EduParent>().ToListAsync();
        Assert.True(parents.First(p => p.Name == "Alice").Archived);
        Assert.True(parents.First(p => p.Name == "Bob").Archived);
        Assert.False(parents.First(p => p.Name == "Carol").Archived);
    }

    [Fact]
    public async Task ExecuteUpdateAsync_with_join_and_filter_updates_subset()
    {
        await _ctx.Query<EduParent>()
            .Join(_ctx.Query<EduChild>(),
                  p => p.Id, c => c.ParentId,
                  (p, c) => p)
            .Where(p => p.Id == 2)
            .ExecuteUpdateAsync(s => s.SetProperty(p => p.Archived, true));

        var parents = await _ctx.Query<EduParent>().ToListAsync();
        Assert.False(parents.First(p => p.Name == "Alice").Archived);
        Assert.True(parents.First(p => p.Name == "Bob").Archived);
        Assert.False(parents.First(p => p.Name == "Carol").Archived);
    }
}
