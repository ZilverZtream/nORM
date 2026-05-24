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
/// Sister probe of <see cref="LinqNavigationFilteredCountInProjectionTests"/>
/// (665e16d) for SCALAR aggregates: <c>parent.Children.Where(pred).Sum(c => c.X)</c>
/// and friends. The 49ac214 SCV emitter for <c>parent.Children.Select(c => c.X).Sum()</c>
/// matches only the bare-nav-then-Select shape — a Where wrapper between them
/// silently falls through to the generic emitter / client-eval throw.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqNavigationFilteredScalarAggregateInProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private NfsContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE NfsParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE NfsChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, IsActive INTEGER NOT NULL, Amount REAL NOT NULL);
            INSERT INTO NfsParent VALUES (1,'Alice'),(2,'Bob');
            -- Alice: 3 children — active 10+20=30, inactive 100.
            -- Bob: 2 children — both inactive (1+2). ActiveSum = 0 (no rows).
            INSERT INTO NfsChild VALUES
                (1,1,1,10.0),(2,1,1,20.0),(3,1,0,100.0),
                (4,2,0, 1.0),(5,2,0,  2.0);
            """;
        await cmd.ExecuteNonQueryAsync();

        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<NfsParent>().HasKey(p => p.Id);
                mb.Entity<NfsParent>()
                    .HasMany<NfsChild>(p => p.Children)
                    .WithOne()
                    .HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new NfsContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Navigation_children_where_select_sum_in_projection_applies_filter_inside_subquery()
    {
        var rows = (await _ctx.Query<NfsParent>()
            .Select(p => new
            {
                p.Name,
                ActiveSum = p.Children.Where(c => c.IsActive == 1).Select(c => c.Amount).Sum()
            })
            .ToListAsync())
            .OrderBy(r => r.Name)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("Alice", rows[0].Name); Assert.Equal(30.0, rows[0].ActiveSum);
        // Bob has no active children — SUM over no rows is NULL in SQL, but the
        // double-typed materialiser coerces NULL to 0.0. (Or it might throw if the
        // materialiser is strict — either way the assertion catches silent wrongness.)
        Assert.Equal("Bob",   rows[1].Name); Assert.Equal(0.0,  rows[1].ActiveSum);
    }

    [Table("NfsParent")]
    public sealed class NfsParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<NfsChild> Children { get; set; } = new();
    }

    [Table("NfsChild")]
    public sealed class NfsChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int IsActive { get; set; }
        public double Amount { get; set; }
    }

    private sealed class NfsContext : DbContext
    {
        public NfsContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
