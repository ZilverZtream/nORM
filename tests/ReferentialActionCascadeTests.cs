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
/// nORM applies referential actions to loaded/tracked dependents client-side (that is how cascade works even
/// with no DB FK constraints). It implemented only the Cascade arm: deleting a principal with a SetNull /
/// SetDefault / Restrict relationship to TRACKED dependents did nothing — SetNull left the dependents with a
/// dangling FK pointing at the deleted principal (silent orphan), and Restrict silently succeeded where it
/// must block. These arms must be applied like Cascade is.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ReferentialActionCascadeTests
{
    [Table("RacParent")]
    public class Parent
    {
        [Key] public int Id { get; set; }
        public List<Child> Children { get; set; } = new();
    }

    [Table("RacChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int? ParentId { get; set; }
    }

    private static (SqliteConnection Keeper, Func<DbContext> Make) Setup(ReferentialAction onDelete)
    {
        var keeper = new SqliteConnection($"Data Source=file:rac_{Guid.NewGuid():N}?mode=memory&cache=shared");
        keeper.Open();
        using (var cmd = keeper.CreateCommand())
        {
            // No FK constraint — nORM applies the action client-side to tracked dependents.
            cmd.CommandText =
                "CREATE TABLE RacParent (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE RacChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NULL);" +
                "INSERT INTO RacParent VALUES (1);" +
                "INSERT INTO RacChild VALUES (10, 1), (11, 1);";
            cmd.ExecuteNonQuery();
        }
        DbContext Make()
        {
            var cn = new SqliteConnection(keeper.ConnectionString);
            cn.Open();
            var opts = new DbContextOptions
            {
                OnModelCreating = mb => mb.Entity<Parent>()
                    .HasMany(p => p.Children).WithOne()
                    .HasForeignKey(c => c.ParentId!, p => p.Id, onDelete, ReferentialAction.NoAction)
            };
            return new DbContext(cn, new SqliteProvider(), opts);
        }
        return (keeper, Make);
    }

    private static List<(long Id, object ParentId)> ChildRows(SqliteConnection k)
    {
        using var cmd = k.CreateCommand();
        cmd.CommandText = "SELECT Id, ParentId FROM RacChild ORDER BY Id";
        using var r = cmd.ExecuteReader();
        var list = new List<(long, object)>();
        while (r.Read()) list.Add((r.GetInt64(0), r.GetValue(1)));
        return list;
    }

    [Fact]
    public async Task SetNull_nulls_tracked_dependent_fk_when_principal_deleted()
    {
        var (keeper, make) = Setup(ReferentialAction.SetNull);
        using var _keeper = keeper;

        await using (var ctx = make())
        {
            var parent = ((INormQueryable<Parent>)ctx.Query<Parent>()).Include(p => p.Children).ToList().Single();
            ctx.Remove(parent);
            await ctx.SaveChangesAsync();
        }

        var rows = ChildRows(keeper);
        Assert.Equal(2, rows.Count);   // children survive
        Assert.All(rows, r => Assert.True(r.ParentId is DBNull, $"child {r.Id} FK must be NULL, was {r.ParentId}"));
    }

    [Fact]
    public async Task Restrict_blocks_deleting_a_principal_with_tracked_dependents()
    {
        var (keeper, make) = Setup(ReferentialAction.Restrict);
        using var _keeper = keeper;

        await using var ctx = make();
        var parent = ((INormQueryable<Parent>)ctx.Query<Parent>()).Include(p => p.Children).ToList().Single();
        ctx.Remove(parent);

        await Assert.ThrowsAnyAsync<InvalidOperationException>(() => ctx.SaveChangesAsync());

        // Nothing was deleted.
        using var cmd = keeper.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM RacParent";
        Assert.Equal(1L, Convert.ToInt64(cmd.ExecuteScalar()));
    }
}
