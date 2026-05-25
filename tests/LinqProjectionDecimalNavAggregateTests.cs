using System;
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
/// Probe pin for decimal MIN/MAX inside a correlated nav-collection
/// aggregate subquery: `Select(p =&gt; new { Max = p.Children.Max(c =&gt; c.V) })`.
/// The HandleNavScalarAggregate path emits a `(SELECT MAX(col) FROM
/// dependent WHERE FK=parent.PK)` subquery; without CAST AS REAL on the
/// inner aggregate the same lex bug from 2002200 would apply.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqProjectionDecimalNavAggregateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE PdnaParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE PdnaChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, V TEXT NOT NULL);
            INSERT INTO PdnaParent VALUES (1, 'A'), (2, 'B');
            INSERT INTO PdnaChild VALUES
                (1, 1, '10.5'),
                (2, 1, '2.0'),
                (3, 1, '100.0'),
                (4, 2, '1.5'),
                (5, 2, '20.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<PdnaParent>().HasKey(p => p.Id);
                mb.Entity<PdnaChild>().HasKey(c => c.Id);
                mb.Entity<PdnaParent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Select_nav_collection_decimal_Max_returns_numerically_max_per_parent()
    {
        var r = await _ctx.Query<PdnaParent>().OrderBy(p => p.Id)
            .Select(p => new { p.Id, MaxV = p.Children.Max(c => c.V) }).ToListAsync();
        Assert.Equal(2, r.Count);
        Assert.Equal(100.0m, r[0].MaxV);  // parent A: 10.5, 2.0, 100.0 -> max 100
        Assert.Equal(20.0m, r[1].MaxV);   // parent B: 1.5, 20.0 -> max 20
    }

    [Table("PdnaParent")]
    public sealed class PdnaParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<PdnaChild> Children { get; set; } = new();
    }

    [Table("PdnaChild")]
    public sealed class PdnaChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public decimal V { get; set; }
    }
}
