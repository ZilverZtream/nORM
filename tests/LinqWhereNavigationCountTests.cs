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
/// Pins <c>Query&lt;Parent&gt;().Where(p =&gt; p.Children.Count() &gt; N)</c> —
/// nav-Count subquery in a WHERE predicate. Sister of
/// <see cref="LinqOrderByNavigationCountTests"/> for the WHERE path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqWhereNavigationCountTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private WncContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE WncParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE WncChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL);
            INSERT INTO WncParent VALUES (1,'Alice'),(2,'Bob'),(3,'Carol');
            -- Counts: Alice=3, Bob=1, Carol=0. WHERE count > 1 → just Alice.
            INSERT INTO WncChild VALUES (1,1),(2,1),(3,1),(4,2);
            """;
        await cmd.ExecuteNonQueryAsync();
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<WncParent>().HasKey(p => p.Id);
                mb.Entity<WncParent>().HasMany<WncChild>(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new WncContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Where_on_navigation_count_filters_parents_by_correlated_subquery_value()
    {
        var rows = (await _ctx.Query<WncParent>()
            .Where(p => p.Children.Count() > 1)
            .Select(p => p.Name)
            .ToListAsync())
            .ToArray();
        Assert.Single(rows);
        Assert.Equal("Alice", rows[0]);
    }

    [Table("WncParent")]
    public sealed class WncParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<WncChild> Children { get; set; } = new();
    }

    [Table("WncChild")]
    public sealed class WncChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
    }

    private sealed class WncContext : DbContext
    {
        public WncContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
