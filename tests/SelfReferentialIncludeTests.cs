using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Eager-loading a self-referential relationship (Category → Children) — the menu
/// tree / org chart pattern. The dependent query targets the same table as the
/// principal, so it exercises whether Include correlates a table against itself
/// without wrongly folding the whole table into every node's Children.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SelfReferentialIncludeTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SriCategory (Id INTEGER PRIMARY KEY, ParentId INTEGER NULL, Name TEXT NOT NULL);
            INSERT INTO SriCategory VALUES
              (1, NULL, 'Root'),
              (2, 1,    'ChildA'),
              (3, 1,    'ChildB'),
              (4, 2,    'Grandchild');
            """;
        await cmd.ExecuteNonQueryAsync();

        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<SriCategory>()
                .HasKey(c => c.Id)
                .HasMany(c => c.Children).WithOne(c => c.Parent).HasForeignKey(c => c.ParentId!, c => c.Id)
        };
        _ctx = new DbContext(_cn, new SqliteProvider(), opts);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Include_children_loads_only_direct_children_per_node()
    {
        var all = (await ((INormQueryable<SriCategory>)_ctx.Query<SriCategory>())
            .Include(c => c.Children)
            .AsSplitQuery()
            .OrderBy(c => c.Id)
            .ToListAsync())
            .ToDictionary(c => c.Id);

        // Each node must hold ONLY its direct children, not the whole self-referential table.
        Assert.Equal(new[] { "ChildA", "ChildB" }, all[1].Children.Select(c => c.Name).OrderBy(n => n));
        Assert.Equal(new[] { "Grandchild" }, all[2].Children.Select(c => c.Name));
        Assert.Empty(all[3].Children);
        Assert.Empty(all[4].Children);
    }

    [Fact]
    public async Task Include_children_on_filtered_root_loads_its_children()
    {
        var roots = await ((INormQueryable<SriCategory>)_ctx.Query<SriCategory>())
            .Include(c => c.Children)
            .AsSplitQuery()
            .Where(c => c.ParentId == null)
            .ToListAsync();

        var root = Assert.Single(roots);
        Assert.Equal("Root", root.Name);
        Assert.Equal(new[] { "ChildA", "ChildB" }, root.Children.Select(c => c.Name).OrderBy(n => n));
    }

    [Table("SriCategory")]
    public sealed class SriCategory
    {
        [Key] public int Id { get; set; }
        public int? ParentId { get; set; }
        public SriCategory Parent { get; set; } = null!;
        public List<SriCategory> Children { get; set; } = new();
        public string Name { get; set; } = "";
    }
}
