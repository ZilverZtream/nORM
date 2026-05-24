using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// Pins <c>from p in Parents from c in p.Children select new {p.Name, c.Tag}</c>
/// — the canonical multi-from query-syntax shape that the compiler lowers to
/// <c>SelectMany</c> with a result selector. nORM emits this as an INNER JOIN
/// on the navigation FK. Silent-wrongness risk if the join key resolves
/// against the wrong table — rows would multiply or get dropped.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSelectManyMultiFromCountTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private SmmContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SmmParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SmmChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            INSERT INTO SmmParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            -- Alice: 2 children; Bob: 1 child; Carol: 0 children.
            INSERT INTO SmmChild  VALUES (1,1,'a1'),(2,1,'a2'),(3,2,'b1');
            """;
        await cmd.ExecuteNonQueryAsync();
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SmmParent>().HasKey(p => p.Id);
                mb.Entity<SmmParent>().HasMany<SmmChild>(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new SmmContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task SelectMany_over_navigation_collection_returns_inner_join_flattened_rows()
    {
        // INNER JOIN flatten: 3 rows (Alice/a1, Alice/a2, Bob/b1). Carol is dropped
        // because she has no matching children — that's INNER JOIN semantics, which
        // matches LINQ-to-Objects `from c in p.Children` (empty source yields nothing).
        var rows = (await _ctx.Query<SmmParent>()
            .SelectMany(p => p.Children, (p, c) => new { p.Name, c.Tag })
            .ToListAsync())
            .OrderBy(r => r.Name).ThenBy(r => r.Tag)
            .ToArray();
        Assert.Equal(3, rows.Length);
        Assert.Equal("Alice", rows[0].Name); Assert.Equal("a1", rows[0].Tag);
        Assert.Equal("Alice", rows[1].Name); Assert.Equal("a2", rows[1].Tag);
        Assert.Equal("Bob",   rows[2].Name); Assert.Equal("b1", rows[2].Tag);
    }

    [Table("SmmParent")]
    public sealed class SmmParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<SmmChild> Children { get; set; } = new();
    }

    [Table("SmmChild")]
    public sealed class SmmChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    private sealed class SmmContext : DbContext
    {
        public SmmContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
