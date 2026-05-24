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
/// Pins side-by-side navigation aggregates in the same projection — two
/// correlated subqueries (Sum + Count) emitted into the same SELECT list. The
/// SCV emit (0977c64 / 49ac214 / 665e16d / c3c044b / 0d65531) hardcodes
/// <c>__nav</c> as the alias for every subquery; in two side-by-side
/// subqueries each <c>__nav</c> is local to its own FROM scope, so SQL allows
/// it — but if a future refactor moves the subquery emit into a shared scope
/// (e.g. a single CTE), the duplicate alias would crash. Pin the current
/// behaviour so the regression is obvious.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqMultipleNavAggregatesInProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private MnaContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE MnaParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE MnaChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount REAL NOT NULL);
            INSERT INTO MnaParent VALUES (1,'Alice'),(2,'Bob');
            INSERT INTO MnaChild  VALUES (1,1,10.0),(2,1,20.0),(3,1,30.0),(4,2, 5.0),(5,2,15.0);
            """;
        await cmd.ExecuteNonQueryAsync();
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<MnaParent>().HasKey(p => p.Id);
                mb.Entity<MnaParent>().HasMany<MnaChild>(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        };
        _ctx = new MnaContext(_cn, options);
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Projection_with_two_navigation_aggregates_emits_independent_subqueries_per_column()
    {
        var rows = (await _ctx.Query<MnaParent>()
            .Select(p => new
            {
                p.Name,
                ChildCount = p.Children.Count(),
                Total      = p.Children.Select(c => c.Amount).Sum()
            })
            .ToListAsync())
            .OrderBy(r => r.Name)
            .ToArray();
        Assert.Equal(2, rows.Length);
        Assert.Equal("Alice", rows[0].Name); Assert.Equal(3, rows[0].ChildCount); Assert.Equal(60.0, rows[0].Total);
        Assert.Equal("Bob",   rows[1].Name); Assert.Equal(2, rows[1].ChildCount); Assert.Equal(20.0, rows[1].Total);
    }

    [Table("MnaParent")]
    public sealed class MnaParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<MnaChild> Children { get; set; } = new();
    }

    [Table("MnaChild")]
    public sealed class MnaChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public double Amount { get; set; }
    }

    private sealed class MnaContext : DbContext
    {
        public MnaContext(SqliteConnection cn, DbContextOptions options) : base(cn, new SqliteProvider(), options) { }
    }
}
