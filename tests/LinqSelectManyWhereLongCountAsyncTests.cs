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
/// Direct sister of e438a53 (which fixed SelectMany+Where+CountAsync).
/// The fix was deliberately gated on
/// <c>_methodName is "Count" or "LongCount"</c>; this is the positive
/// coverage that verifies the LongCount arm of that gate. Same shape,
/// 64-bit result type. Silent-wrongness would surface as either the
/// pre-fix Id-as-count value or a missing fix that returns 0/wrong type.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class LinqSelectManyWhereLongCountAsyncTests : IAsyncLifetime
{
    private SqliteConnection _cn = null!;
    private DbContext _ctx = null!;

    public async Task InitializeAsync()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        await _cn.OpenAsync();
        await using var cmd = _cn.CreateCommand();
        cmd.CommandText = """
            CREATE TABLE SmlcParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
            CREATE TABLE SmlcChild  (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Amount INTEGER NOT NULL);
            INSERT INTO SmlcParent VALUES (1, 'A'), (2, 'B'), (3, 'C');
            INSERT INTO SmlcChild  VALUES (10, 1, 10), (11, 1, 20), (12, 1, 30), (13, 2, 5), (14, 2, 100);
            -- Total children = 5; Items.Amount > 10 = {20, 30, 100} = 3 rows.
            """;
        await cmd.ExecuteNonQueryAsync();
        _ctx = new DbContext(_cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<SmlcParent>().HasKey(p => p.Id);
                mb.Entity<SmlcChild>().HasKey(c => c.Id);
                mb.Entity<SmlcParent>()
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
    public async Task SelectMany_then_where_then_longcountasync_returns_correct_long_count()
    {
        long count = await _ctx.Query<SmlcParent>()
            .SelectMany(p => p.Items!)
            .Where(i => i.Amount > 10)
            .LongCountAsync();
        Assert.Equal(3L, count);
    }

    [Fact]
    public async Task SelectMany_then_longcountasync_no_filter_returns_total_long_count()
    {
        // No-predicate baseline for the LongCount path -- verifies the
        // e438a53 rewrite fires even without a WHERE in _where.
        long count = await _ctx.Query<SmlcParent>()
            .SelectMany(p => p.Items!)
            .LongCountAsync();
        Assert.Equal(5L, count);
    }

    [Table("SmlcParent")]
    public sealed class SmlcParent
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<SmlcChild>? Items { get; set; }
    }

    [Table("SmlcChild")]
    public sealed class SmlcChild
    {
        [Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Amount { get; set; }
    }
}
