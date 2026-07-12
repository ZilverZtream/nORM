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
/// A global filter (e.g. soft-delete !IsDeleted) must apply to BOTH arms of a set operation.
/// ApplyGlobalFilters deliberately does not recurse the left/source arm of a same-type binary
/// operator, relying on the base-case wrap to filter the combined result — but an early return
/// when the right arm is rewritten skipped that wrap, leaking the left arm's filtered rows.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GlobalFilterSetOperationLeakTests
{
    [Table("GfOrder")]
    private class GfOrder
    {
        [Key] public int Id { get; set; }
        public string Region { get; set; } = "";
        public bool IsDeleted { get; set; }
    }

    private static DbContext CreateContext(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE GfOrder (Id INTEGER PRIMARY KEY, Region TEXT NOT NULL, IsDeleted INTEGER NOT NULL);" +
                "INSERT INTO GfOrder VALUES (1,'US',0),(2,'US',1),(3,'EU',0),(4,'EU',1);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions().AddGlobalFilter<GfOrder>(o => !o.IsDeleted);
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Union_applies_global_filter_to_both_arms()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateContext(cn);

        var rows = (await ctx.Query<GfOrder>().Where(o => o.Region == "US")
                .Union(ctx.Query<GfOrder>().Where(o => o.Region == "EU"))
                .ToListAsync())
            .OrderBy(o => o.Id)
            .ToList();

        // Only the non-deleted rows (Id 1 and 3). Id 2 (US, deleted) must NOT leak from the left arm.
        Assert.Equal(new[] { 1, 3 }, rows.Select(o => o.Id));
        Assert.DoesNotContain(rows, o => o.IsDeleted);
    }

    [Fact]
    public async Task Except_applies_global_filter_to_left_arm()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = CreateContext(cn);

        // Left arm = all (filtered) orders; right arm = EU. Result should be the non-deleted US order only.
        var rows = (await ctx.Query<GfOrder>()
                .Except(ctx.Query<GfOrder>().Where(o => o.Region == "EU"))
                .ToListAsync())
            .OrderBy(o => o.Id)
            .ToList();

        Assert.Equal(new[] { 1 }, rows.Select(o => o.Id)); // Id 2 (deleted US) must not appear
    }
}
