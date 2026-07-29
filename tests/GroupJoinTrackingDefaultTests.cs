using System;
using System.Collections.Generic;
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
/// A GroupJoin that materializes the outer/inner entities must honour the context tracking DEFAULT, not just
/// an explicit .AsNoTracking(). Under a NoTracking default, the entities must NOT be tracked and an edit to a
/// materialized entity must NOT persist on the next SaveChanges — matching every other read path.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class GroupJoinTrackingDefaultTests
{
    [Table("GjtDept")]
    public class GjtDept
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<GjtEmp> Emps { get; set; } = new();
    }

    [Table("GjtEmp")]
    public class GjtEmp
    {
        [Key] public int Id { get; set; }
        public int DeptId { get; set; }
        public string Name { get; set; } = "";
    }

    private static DbContext NewCtx(SqliteConnection cn, QueryTrackingBehavior tracking)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE GjtDept (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE GjtEmp (Id INTEGER PRIMARY KEY, DeptId INTEGER NOT NULL, Name TEXT NOT NULL);" +
                "INSERT INTO GjtDept VALUES (1,'D1');" +
                "INSERT INTO GjtEmp VALUES (1,1,'E1'),(2,1,'E2');";
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions { DefaultTrackingBehavior = tracking });
    }

    private static long RawDeptName(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM GjtDept WHERE Name='CHANGED'";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task GroupJoin_under_notracking_default_does_not_track_or_persist()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using var ctx = NewCtx(cn, QueryTrackingBehavior.NoTracking);

        var result = ctx.Query<GjtDept>()
            .GroupJoin(ctx.Query<GjtEmp>(), d => d.Id, e => e.DeptId,
                (d, emps) => new { Dept = d, Emps = emps.ToList() })
            .ToList();

        Assert.Single(result);
        Assert.Equal(2, result[0].Emps.Count);

        // Under a NoTracking default, neither the outer nor the inner entities may be tracked.
        Assert.Equal(0, ctx.ChangeTracker.Entries.Count(e => e.Entity is GjtDept || e.Entity is GjtEmp));

        // And an edit to a materialized entity must NOT persist (NoTracking contract).
        result[0].Dept.Name = "CHANGED";
        await ctx.SaveChangesAsync();
        Assert.Equal(0L, RawDeptName(cn));
    }

    // Control: with a TRACKING default, the same GroupJoin DOES track the entities.
    [Fact]
    public void GroupJoin_under_tracking_default_tracks_materialized_entities()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using var ctx = NewCtx(cn, QueryTrackingBehavior.TrackAll);

        var result = ctx.Query<GjtDept>()
            .GroupJoin(ctx.Query<GjtEmp>(), d => d.Id, e => e.DeptId,
                (d, emps) => new { Dept = d, Emps = emps.ToList() })
            .ToList();

        Assert.True(ctx.ChangeTracker.Entries.Count(e => e.Entity is GjtDept || e.Entity is GjtEmp) > 0);
    }
}
