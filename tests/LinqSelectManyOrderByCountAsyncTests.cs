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
/// Second-order regression for e438a53. The fix rewrites SELECT-cols to
/// COUNT(*) when <c>_methodName is "Count" or "LongCount"</c> AND
/// <c>_sql.Length &gt; 0</c>. Composing OrderBy between SelectMany and
/// CountAsync exercises both:
///   * b2df7e1 alias plumbing (OrderBy adds a T-alias to _correlatedParams
///     which shifts the outer FROM render)
///   * e438a53's SELECT-cols -> COUNT(*) rewrite (ORDER BY is meaningless
///     on a scalar COUNT and should be dropped; the fix clears _orderBy)
/// Silent-wrongness risk if either path conflicts under composition.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSelectManyOrderByCountAsyncTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SmocParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SmocChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO SmocParent VALUES (1, 'A'), (2, 'B'), (3, 'C');
            INSERT INTO SmocChild  VALUES (10, 1, 10), (11, 1, 20), (12, 1, 30), (13, 2, 5), (14, 2, 100);
            -- Items > 10: {20, 30, 100} -> 3 qualifying rows.
            -- OrderBy(Amount) added before Count; ORDER BY is meaningless on
            -- a scalar COUNT and must not interfere with the COUNT rewrite.
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SmocParent>().HasKey(p => p.Id);
                mb.Entity<SmocChild>().HasKey(c => c.Id);
                mb.Entity<SmocParent>()
                    .HasMany(p => p.Items!)
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
    public async Task SelectMany_then_where_then_orderby_then_count_returns_correct_count_dropping_order_by()
    {
        // SelectMany -> Where -> OrderBy(Amount) -> Count. Count should
        // return 3 regardless of the ORDER BY (which the e438a53 fix
        // clears since it's meaningless on a scalar aggregate).
        var count = await _ctx.Query<SmocParent>()
            .SelectMany(p => p.Items!)
            .Where(i => i.Amount > 10)
            .OrderBy(i => i.Amount)
            .CountAsync();
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task SelectMany_then_orderby_descending_then_where_then_count_returns_correct_count()
    {
        // OrderBy interleaved BEFORE the Where -- different visit order
        // but should produce the same COUNT(*) result.
        var count = await _ctx.Query<SmocParent>()
            .SelectMany(p => p.Items!)
            .OrderByDescending(i => i.Amount)
            .Where(i => i.Amount > 10)
            .CountAsync();
        Assert.Equal(3, count);
    }

    [Table("SmocParent")]
    public sealed class SmocParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<SmocChild>? Items { get; set; }
    }

    [Table("SmocChild")]
    public sealed class SmocChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
