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
/// Eighth in the post-Take/Skip silent-wrongness family.
/// <c>q.OrderBy(Id).Take(2).SelectMany(p =&gt; p.Children, …)</c> must flatten
/// only the windowed 2 outer rows. A naive translation runs the JOIN over the
/// full outer table and then LIMITs the joined result.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSelectManyAfterTakeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private SmaContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SmaParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SmaChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Tag TEXT NOT NULL);
            -- Differentiating data: rows 1,2 (by Id ASC) are Parents with NO children.
            -- Rows 3,4 have children. OrderBy(Id).Take(2) → rows 1,2 (no children).
            -- Windowed SelectMany: 0 rows. Full-table SelectMany then LIMIT 2: 2 rows
            -- (the first 2 children of Parents 3 and 4).
            INSERT INTO SmaParent VALUES (1,'EmptyA'),(2,'EmptyB'),(3,'WithKids1'),(4,'WithKids2');
            INSERT INTO SmaChild  VALUES (1,3,'k1'),(2,3,'k2'),(3,4,'k3'),(4,4,'k4');
            """;
        await cmd.ExecuteNonQueryAsync();
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SmaParent>().HasKey(p => p.Id);
                mb.Entity<SmaParent>().HasMany<SmaChild>(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new SmaContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task SelectMany_after_take_flattens_only_windowed_rows()
    {
        // OrderBy(Id).Take(2) → parents 1,2 — both have NO children.
        // Windowed flatten yields 0 rows. Full-table flatten then LIMIT(2) would
        // yield 2 children of parents 3 and 4 — silent-wrongness.
        var tags = (await _ctx.Query<SmaParent>()
            .OrderBy(p => p.Id)
            .Take(2)
            .SelectMany(p => p.Children, (p, c) => c.Tag)
            .ToListAsync())
            .ToArray();
        Assert.Empty(tags);
    }

    [Fact]
    public async Task SelectMany_skip_then_take_flattens_only_windowed_parents_children()
    {
        // Skip(2).Take(2) → parents 3 and 4 — both have 2 children each.
        // Windowed flatten yields the 4 children. NewExpression projection
        // hits JoinBuilder's column-extraction path.
        var rows = (await _ctx.Query<SmaParent>()
            .OrderBy(p => p.Id)
            .Skip(2)
            .Take(2)
            .SelectMany(p => p.Children, (p, c) => new { c.Tag })
            .ToListAsync())
            .Select(r => r.Tag).OrderBy(t => t)
            .ToArray();
        Assert.Equal(new[] { "k1", "k2", "k3", "k4" }, rows);
    }

    [Table("SmaParent")]
    public sealed class SmaParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<SmaChild> Children { get; set; } = new();
    }

    [Table("SmaChild")]
    public sealed class SmaChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public string Tag { get; set; } = string.Empty;
    }

    private sealed class SmaContext : DbContext
    {
        public SmaContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
