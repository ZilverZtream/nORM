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
/// Optimistic concurrency composes with reference-navigation writes: a navigation
/// reassignment (which relationship fixup turns into an FK update) bumps the
/// [Timestamp] token, and a competing write that changed the token behind the
/// context's back makes the save throw instead of silently overwriting.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OccNavigationWriteTests
{
    [Table("OccNav_Dept")]
    private class Dept
    {
        [Key][DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Title { get; set; } = "";
    }

    [Table("OccNav_Emp")]
    private class Emp
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int? DeptId { get; set; }
        [ForeignKey(nameof(DeptId))] public Dept? Dept { get; set; }
        [Timestamp] public byte[]? Version { get; set; }
    }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OccNav_Dept (Id INTEGER PRIMARY KEY AUTOINCREMENT, Title TEXT NOT NULL);
                CREATE TABLE OccNav_Emp (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, DeptId INTEGER NULL, Version BLOB NULL);
                INSERT INTO OccNav_Dept (Id, Title) VALUES (1, 'Eng');
                INSERT INTO OccNav_Emp VALUES (1, 'ann', 1, X'0000000000000001');
                """;
            cmd.ExecuteNonQuery();
        }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<Dept>().HasKey(d => d.Id); mb.Entity<Emp>().HasKey(e => e.Id); }
        });
    }

    private static object? Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return cmd.ExecuteScalar();
    }

    [Fact]
    public async Task occ_entity_nav_reassignment_bumps_token()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emp = ctx.Query<Emp>().Single(e => e.Id == 1);
        var before = (byte[])emp.Version!.Clone();
        emp.Dept = new Dept { Title = "R&D" };
        await ctx.SaveChangesAsync();
        Assert.Equal("R&D", Scalar(cn, "SELECT d.Title FROM OccNav_Emp e JOIN OccNav_Dept d ON d.Id = e.DeptId WHERE e.Id = 1"));
        var after = (byte[])Scalar(cn, "SELECT Version FROM OccNav_Emp WHERE Id = 1")!;
        Assert.False(before.AsSpan().SequenceEqual(after)); // token bumped
    }

    [Fact]
    public async Task occ_conflict_on_nav_reassignment_throws()
    {
        using var ctx = Ctx(out var cn);
        using var _cn = cn;
        var emp = ctx.Query<Emp>().Single(e => e.Id == 1);
        emp.Dept = new Dept { Title = "R&D" };
        // Sneak update: another writer bumps the token behind our back.
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "UPDATE OccNav_Emp SET Version = X'00000000000000FF' WHERE Id = 1";
            cmd.ExecuteNonQuery();
        }
        await Assert.ThrowsAsync<DbConcurrencyException>(() => ctx.SaveChangesAsync());
    }
}
