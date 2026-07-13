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

#nullable enable

namespace nORM.Tests;

/// <summary>
/// AsOf composes with navigation predicates: the temporal filter applies to the
/// ROOT entity while the navigation's correlated subquery reads the CURRENT
/// principal — temporal scope is per-table, matching SQL Server's FOR SYSTEM_TIME
/// and EF Core semantics, where joined tables need their own temporal spec.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TemporalAsOfNavigationTests
{
    [Table("AsOfNav_Dept")]
    private class Dept
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("AsOfNav_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
    }

    [Fact]
    public async Task asof_with_nav_predicate_translates_and_reads_current_principal()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE AsOfNav_Dept (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE AsOfNav_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL);
                INSERT INTO AsOfNav_Dept VALUES (1, 'Eng');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => { mb.Entity<Emp>(); mb.Entity<Dept>().HasKey(d => d.Id); } };
        opts.EnableTemporalVersioning();
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        ctx.Add(new Emp { Id = 1, Name = "ann", DeptId = 1 });
        await ctx.SaveChangesAsync();
        var afterInsert = DateTime.UtcNow.AddSeconds(1);

        // AsOf(now+1s) sees the row; the nav predicate reads the CURRENT principal
        // (nORM navigations are not time-traveled — the principal is not part of the
        // temporal root's history).
        var names = await ctx.Query<Emp>().AsOf(afterInsert).Where(e => e.Dept!.Title == "Eng")
            .Select(e => e.Name).ToListAsync();
        Assert.Equal(new[] { "ann" }, names);
    }
}
