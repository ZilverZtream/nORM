using System;
using System.Collections.Generic;
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
/// Editing the same loaded entity across MULTIPLE SaveChanges calls inside one caller-owned transaction.
/// Under a caller-owned transaction nORM defers AcceptChanges, so the change-tracking baseline must still
/// advance to the values each save wrote — otherwise a later DetectChanges compares against the stale
/// pre-transaction baseline. An A -> B (save) -> A (save) sequence would then leave the second save seeing
/// "equal to baseline", skip the write, and SILENTLY strand the row at B. A full rollback must restore the
/// pre-transaction baseline so a still-pending edit re-applies on the next save.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class RepeatedEditUnderTransactionTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("RepParent")]
    public class Parent
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }
    [System.ComponentModel.DataAnnotations.Schema.Table("RepChild")]
    public class Child
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int ParentId { get; set; }
        public int Val { get; set; }
        public Parent? Parent { get; set; }
    }

    private static (SqliteConnection, DbContext) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE RepParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE RepChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Val INTEGER NOT NULL);
                INSERT INTO RepParent VALUES (5, 'five'), (17, 'seventeen');
                INSERT INTO RepChild VALUES (71, 17, -37);
                """;
            cmd.ExecuteNonQuery();
        }
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Parent>().HasMany(p => p.Children).WithOne().HasForeignKey(c => c.ParentId, p => p.Id);
            }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), options));
    }

    private static int FkInDb(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT ParentId FROM RepChild WHERE Id = 71";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    private static int ValInDb(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Val FROM RepChild WHERE Id = 71";
        return Convert.ToInt32(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Fk_edited_back_to_original_across_two_saves_persists_the_final_value()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var child = await ctx.Query<Child>().FirstAsync(c => c.Id == 71);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        child.ParentId = 5; await ctx.SaveChangesAsync();
        child.ParentId = 17; await ctx.SaveChangesAsync();   // back to the loaded value — must still be written
        await tx.CommitAsync();

        Assert.Equal(17, FkInDb(cn));
    }

    [Fact]
    public async Task Scalar_edited_back_to_original_across_two_saves_persists_the_final_value()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var child = await ctx.Query<Child>().FirstAsync(c => c.Id == 71);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        child.Val = 100; await ctx.SaveChangesAsync();
        child.Val = -37; await ctx.SaveChangesAsync();   // back to the loaded value
        await tx.CommitAsync();

        Assert.Equal(-37, ValInDb(cn));
    }

    [Fact]
    public async Task Navigation_moved_back_to_original_principal_across_two_saves_persists()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var parents = await ctx.Query<Parent>().ToListAsync();
        var child = await ctx.Query<Child>().FirstAsync(c => c.Id == 71);
        var p5 = parents.First(p => p.Id == 5);
        var p17 = parents.First(p => p.Id == 17);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        child.Parent = p5; await ctx.SaveChangesAsync();
        child.Parent = p17; await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        Assert.Equal(17, FkInDb(cn));
    }

    [Fact]
    public async Task Unrelated_later_save_does_not_revert_an_edit_made_earlier_in_the_transaction()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var child = await ctx.Query<Child>().FirstAsync(c => c.Id == 71);

        await using var tx = await ctx.Database.BeginTransactionAsync();
        child.ParentId = 5; await ctx.SaveChangesAsync();
        ctx.Add(new Child { Id = 99, ParentId = 5, Val = 1 }); await ctx.SaveChangesAsync();
        await tx.CommitAsync();

        Assert.Equal(5, FkInDb(cn));
    }

    [Fact]
    public async Task Edit_saved_in_transaction_then_rolled_back_re_applies_on_the_next_save()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;
        var child = await ctx.Query<Child>().FirstAsync(c => c.Id == 71);

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            child.ParentId = 5; await ctx.SaveChangesAsync();
            await tx.RollbackAsync();
        }
        Assert.Equal(17, FkInDb(cn));   // rolled back

        await ctx.SaveChangesAsync();   // the still-pending edit must re-apply
        Assert.Equal(5, FkInDb(cn));
    }
}
