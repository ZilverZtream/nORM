using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;
#nullable enable
namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class EntityEntryIsKeySetContractTests
{
    [Table("IksWidget")]
    public class Widget { [Key] [DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("IksGuidWidget")]
    public class GuidWidget { [Key] public Guid Id { get; set; } public string Name { get; set; } = ""; }

    private static DbContext Ctx(out SqliteConnection cn)
    {
        cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand()) { cmd.CommandText = "CREATE TABLE IksWidget (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL); CREATE TABLE IksGuidWidget (Id TEXT PRIMARY KEY, Name TEXT NOT NULL);"; cmd.ExecuteNonQuery(); }
        return new DbContext(cn, new SqliteProvider(), new DbContextOptions { OnModelCreating = mb => { mb.Entity<Widget>().HasKey(w => w.Id); mb.Entity<GuidWidget>().HasKey(w => w.Id); } });
    }

    [Fact]
    public void unset_int_key_is_not_set_then_set_after_save()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var w = new Widget { Name = "a" };
        var entry = ctx.Add(w);
        Assert.False(entry.IsKeySet);   // Id still 0 (DB-generated, unassigned)
    }

    [Fact]
    public async Task key_is_set_after_savechanges_assigns_it()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var w = new Widget { Name = "a" };
        var entry = ctx.Add(w);
        await ctx.SaveChangesAsync();
        Assert.NotEqual(0, w.Id);                    // did SaveChanges write the key back?
        Assert.Same(w, entry.Entity);                // does the entry still reference w?
        Assert.True(entry.IsKeySet);                 // DB assigned Id → key is set
    }

    [Fact]
    public void explicitly_set_key_is_set()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        var entry = ctx.Attach(new Widget { Id = 42, Name = "x" });
        Assert.True(entry.IsKeySet);
    }

    [Fact]
    public void empty_guid_key_is_not_set()
    {
        using var ctx = Ctx(out var cn); using var _cn = cn;
        Assert.False(ctx.Add(new GuidWidget { Id = Guid.Empty }).IsKeySet);
        Assert.True(ctx.Attach(new GuidWidget { Id = Guid.NewGuid() }).IsKeySet);
    }
}
