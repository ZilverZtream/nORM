using System;
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
/// Probe pin for Union/Intersect/Except over decimal-column projections.
/// SQLite stores decimal as TEXT; UNION (set semantics, distinct) and
/// INTERSECT/EXCEPT match by string equality, so '10.5' and '10.50'
/// register as distinct values even though they're the same decimal.
/// Mirror of the DISTINCT fix (e59e44f).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSetOperationDecimalProjectionTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SodpLeft  (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            CREATE TABLE SodpRight (Id INTEGER PRIMARY KEY, V TEXT NOT NULL);
            INSERT INTO SodpLeft  VALUES (1, '10.5'), (2, '2.0'),  (3, '100.0');
            INSERT INTO SodpRight VALUES (1, '10.50'), (2, '2.00'), (3, '50.0');
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SodpLeft>().HasKey(i => i.Id);
                mb.Entity<SodpRight>().HasKey(i => i.Id);
            }
        });
    }

    public async Task DisposeAsync()
    {
        _ctx.Dispose();
        await _cn.DisposeAsync();
    }

    [Fact]
    public async Task Union_decimal_projections_dedups_numerically_not_lexically()
    {
        var left  = _ctx.Query<SodpLeft >().Select(p => new { V = p.V });
        var right = _ctx.Query<SodpRight>().Select(p => new { V = p.V });
        var union = await left.Union(right).ToListAsync();
        // Numeric union: {10.5, 2.0, 100.0, 50.0} = 4 distinct values
        // Lex union (bug): {10.5, 10.50, 2.0, 2.00, 50.0, 100.0} = 6
        Assert.Equal(4, union.Count);
        Assert.Contains(10.5m, union.Select(r => r.V));
        Assert.Contains(2.0m,  union.Select(r => r.V));
        Assert.Contains(100.0m, union.Select(r => r.V));
        Assert.Contains(50.0m, union.Select(r => r.V));
    }

    [Table("SodpLeft")]
    public sealed class SodpLeft
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }

    [Table("SodpRight")]
    public sealed class SodpRight
    {
        [Key] public int Id { get; set; }
        public decimal V { get; set; }
    }
}
