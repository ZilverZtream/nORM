using System.Collections.Generic;
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
/// Pins <c>OrderBy(p =&gt; p.Children.Count())</c> — sort the parent rows by
/// the cardinality of a navigation collection. Should emit a correlated
/// COUNT subquery in the ORDER BY clause. Silent-wrongness shape: if the
/// nav-aggregate isn't recognised at OrderBy translation, the ORDER BY
/// either errors out OR (worse) silently degrades to the default key sort,
/// leaving the user with a deterministic-looking but semantically wrong
/// order.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqOrderByNavAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE ObnParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE ObnChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);
            -- Asymmetric child counts so the order MUST come from the aggregate,
            -- not from the parent Id ordering:
            --   Parent 1 -> 3 children
            --   Parent 2 -> 1 child
            --   Parent 3 -> 5 children
            --   Parent 4 -> 0 children
            -- Expected ASC by Count():  [4, 2, 1, 3]  (NOT [1, 2, 3, 4] which is PK order)
            INSERT INTO ObnParent VALUES (1, 'A'), (2, 'B'), (3, 'C'), (4, 'D');
            INSERT INTO ObnChild VALUES
              (1, 1), (2, 1), (3, 1),
              (4, 2),
              (5, 3), (6, 3), (7, 3), (8, 3), (9, 3);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<ObnParent>().HasKey(p => p.Id);
                mb.Entity<ObnChild>().HasKey(c => c.Id);
                mb.Entity<ObnParent>()
                    .HasMany(p => p.Children!)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId, p => p.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task OrderBy_nav_count_sorts_parents_ascending_by_child_count()
    {
        // Silent-wrongness check: if the nav-aggregate isn't recognised, ORDER BY
        // either errors OR drops back to PK order [1, 2, 3, 4] (not [4, 2, 1, 3]).
        var ids = (await _ctx.Query<ObnParent>()
            .OrderBy(p => p.Children!.Count())
            .ToListAsync())
            .Select(p => p.Id).ToArray();
        Assert.Equal(new[] { 4, 2, 1, 3 }, ids);
    }

    [Fact]
    public async Task OrderByDescending_nav_count_sorts_parents_with_most_children_first()
    {
        var ids = (await _ctx.Query<ObnParent>()
            .OrderByDescending(p => p.Children!.Count())
            .ToListAsync())
            .Select(p => p.Id).ToArray();
        Assert.Equal(new[] { 3, 1, 2, 4 }, ids);
    }

    [Table("ObnParent")]
    public sealed class ObnParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<ObnChild>? Children { get; set; }
    }

    [Table("ObnChild")]
    public sealed class ObnChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
    }
}
