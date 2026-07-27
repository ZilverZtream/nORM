using System;
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
/// A Where / Distinct / Select applied AFTER a set operation must wrap the compound as a derived table so
/// the operator applies to the unified rows. The wrap gate matched only when the set operation was the
/// DIRECT source; a query-config passthrough (AsNoTracking / AsSplitQuery / IgnoreQueryFilters / TagWith /
/// Cast) between them defeated the gate, so a trailing Where bound to only one arm (rows from the other arm
/// bypassed the predicate) and a trailing Distinct failed to dedup across arms.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class SetOperationPassthroughWrapTests
{
    [Table("SopUser")]
    public class User
    {
        [Key] public int Id { get; set; }
        public bool Active { get; set; }
    }

    private static DbContext CreateDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE SopUser (Id INTEGER PRIMARY KEY, Active INTEGER NOT NULL);
                INSERT INTO SopUser (Id, Active) VALUES (1,1),(2,0),(3,1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<User>().HasKey(u => u.Id) };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Where_after_setop_through_passthrough_applies_to_all_arms()
    {
        await using var ctx = CreateDb();

        var ids = (await ((INormQueryable<User>)ctx.Query<User>())
            .Union(ctx.Query<User>())
            .AsNoTracking()
            .Where(u => u.Active)
            .ToListAsync())
            .Select(u => u.Id).OrderBy(i => i).ToArray();

        // Union dedups to {1,2,3}; Active filters to {1,3}. BUG: {1,2,3} — the inactive row from the
        // un-filtered arm leaked because the predicate bound to only one arm.
        Assert.Equal(new[] { 1, 3 }, ids);
    }

    [Fact]
    public async Task Distinct_after_concat_through_passthrough_dedups_across_arms()
    {
        await using var ctx = CreateDb();

        var count = (await ((INormQueryable<User>)ctx.Query<User>())
            .Concat(ctx.Query<User>())   // UNION ALL -> duplicates
            .AsNoTracking()
            .Distinct()
            .ToListAsync())
            .Count;

        Assert.Equal(3, count);   // BUG: 6 — DISTINCT never reached the compound, duplicates survived
    }
}
