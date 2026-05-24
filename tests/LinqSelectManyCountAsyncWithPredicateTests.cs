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
/// Sister of e438a53 for the Count-with-predicate overload:
/// <c>SelectMany(p =&gt; p.Items).CountAsync(i =&gt; i.Amount &gt; 10)</c>.
/// CountTranslator handles the predicate via <c>node.Arguments[1]</c>
/// (a different translator path than <c>.Where(p).CountAsync()</c>).
/// Silent-wrongness risks: (1) predicate dropped silently -> counts all
/// rows; (2) e438a53's COUNT(*) rewrite skipped -> ExecuteScalar reads
/// the first matching row's first column (like the pre-fix bug).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSelectManyCountAsyncWithPredicateTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SmcpParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SmcpChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO SmcpParent VALUES (1, 'A'), (2, 'B'), (3, 'C');
            -- Children: P1=[10,20,30], P2=[5,100], P3=[].
            -- Total children = 5; Items.Amount > 10 = {20,30,100} = 3 rows.
            INSERT INTO SmcpChild VALUES (10, 1, 10), (11, 1, 20), (12, 1, 30), (13, 2, 5), (14, 2, 100);
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SmcpParent>().HasKey(p => p.Id);
                mb.Entity<SmcpChild>().HasKey(c => c.Id);
                mb.Entity<SmcpParent>()
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
    public async Task SelectMany_then_countasync_predicate_overload_returns_filtered_count()
    {
        // Count(predicate) overload after SelectMany: predicate becomes
        // CountTranslator.Args[1], appended to _where. Total children = 5;
        // predicate Amount > 10 narrows to 3. Silent-wrongness:
        //   * dropped predicate -> 5
        //   * dropped COUNT rewrite -> first matching row Id (11)
        var count = await _ctx.Query<SmcpParent>()
            .SelectMany(p => p.Items!)
            .CountAsync(i => i.Amount > 10);
        Assert.Equal(3, count);
    }

    [Fact]
    public async Task SelectMany_then_countasync_predicate_overload_with_alwaystrue_returns_total()
    {
        // Sanity check that the predicate-overload path matches the count
        // we'd get from the no-predicate form (5 total children).
        var count = await _ctx.Query<SmcpParent>()
            .SelectMany(p => p.Items!)
            .CountAsync(i => i.Amount > 0);
        Assert.Equal(5, count);
    }

    [Table("SmcpParent")]
    public sealed class SmcpParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<SmcpChild>? Items { get; set; }
    }

    [Table("SmcpChild")]
    public sealed class SmcpChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
