using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// SaveChanges must be atomic: if one entity in a multi-entity batch fails (e.g. a unique-constraint
/// violation), none of the batch's changes may persist. A partial commit is silent data corruption.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class SaveChangesAtomicityTests
{
    [Table("AtomItem")]
    private class AtomItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AtomItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL UNIQUE)";
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static long Count(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM AtomItem";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Insert_batch_with_midbatch_unique_violation_rolls_back_all()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AtomItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL UNIQUE);" +
                              "INSERT INTO AtomItem (Name) VALUES ('dup');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        ctx.Add(new AtomItem { Name = "a" });
        ctx.Add(new AtomItem { Name = "dup" });   // collides with the pre-existing row
        ctx.Add(new AtomItem { Name = "c" });

        await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());

        // Atomic: only the original 'dup' row survives — 'a' and 'c' must NOT have been committed.
        Assert.Equal(1, Count(cn));
    }

    [Fact]
    public async Task Retry_after_failed_insert_batch_persists_all_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AtomItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL UNIQUE);" +
                              "INSERT INTO AtomItem (Name) VALUES ('dup');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var a = new AtomItem { Name = "a" };
        var bad = new AtomItem { Name = "dup" };   // collides
        var c = new AtomItem { Name = "c" };
        ctx.Add(a);
        ctx.Add(bad);
        ctx.Add(c);

        await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());

        // Resolve the conflict and retry. The rollback must have reset any DB-generated keys stamped
        // during the failed attempt, so the skip-already-inserted guard does not silently drop 'a'/'c'.
        bad.Name = "dup2";
        await ctx.SaveChangesAsync();

        Assert.Equal(4, Count(cn));   // dup + a + dup2 + c
        Assert.True(a.Id > 0 && bad.Id > 0 && c.Id > 0);
    }

    [Fact]
    public async Task Update_batch_with_midbatch_unique_violation_rolls_back_all()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE AtomItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL UNIQUE);" +
                              "INSERT INTO AtomItem (Name) VALUES ('r1'),('r2'),('r3');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());

        var rows = ctx.Query<AtomItem>().OrderBy(r => r.Id).ToList();
        // Two rows target the SAME new name, so exactly one collides regardless of statement order.
        rows[0].Name = "clash";
        rows[1].Name = "clash";
        ctx.Update(rows[0]);
        ctx.Update(rows[1]);

        await Assert.ThrowsAnyAsync<Exception>(() => ctx.SaveChangesAsync());

        // No partial update: the original names must all still be present.
        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM AtomItem WHERE Name IN ('r1','r2','r3')";
        Assert.Equal(3L, Convert.ToInt64(check.ExecuteScalar()));
    }
}
