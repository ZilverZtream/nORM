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
/// Sister of <see cref="LinqNavigationFilteredCountInProjectionTests"/> for the
/// predicate-overload form <c>parent.Children.Count(c => c.IsActive == 1)</c>.
/// Semantically equivalent to <c>Where(...).Count()</c> but syntactically a
/// 2-arg Count call, so SCV's match needs to also handle the predicate-at-arg[1]
/// shape (mirror of how aggregates in HAVING handle both forms).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNavigationCountPredicateOverloadInProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private NcpoContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NcpoParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE NcpoChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, IsActive INTEGER NOT NULL);
            INSERT INTO NcpoParent VALUES (1,'Alice'),(2,'Bob');
            INSERT INTO NcpoChild VALUES
                (1,1,1),(2,1,1),(3,1,0),
                (4,2,0),(5,2,0);
            """;
        await cmd.ExecuteNonQueryAsync();

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NcpoParent>().HasKey(p => p.Id);
                mb.Entity<NcpoParent>()
                    .HasMany<NcpoChild>(p => p.Children)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new NcpoContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Navigation_count_predicate_overload_in_projection_applies_filter_inside_subquery()
    {
        var rows = (await _ctx.Query<NcpoParent>()
            .Select(p => new
            {
                p.Name,
                ActiveCount = p.Children.Count(c => c.IsActive == 1)
            })
            .ToListAsync())
            .OrderBy(r => r.Name)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("Alice", rows[0].Name); Assert.Equal(2, rows[0].ActiveCount);
        Assert.Equal("Bob",   rows[1].Name); Assert.Equal(0, rows[1].ActiveCount);
    }

    [Table("NcpoParent")]
    public sealed class NcpoParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<NcpoChild> Children { get; set; } = new();
    }

    [Table("NcpoChild")]
    public sealed class NcpoChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int IsActive { get; set; }
    }

    private sealed class NcpoContext : DbContext
    {
        public NcpoContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
