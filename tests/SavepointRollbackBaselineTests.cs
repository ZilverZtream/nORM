using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Rolling a caller-owned transaction back to a savepoint must restore the change-tracking baseline of a
/// Modified entity whose save advanced that baseline after the savepoint — otherwise the next SaveChanges
/// sees current == baseline, emits no UPDATE, and silently drops the user's edit. The full-rollback path
/// already restores the baseline; the savepoint path must too.
/// </summary>
[Trait("Category", "Fast")]
public class SavepointRollbackBaselineTests
{
    [Table("SpItem")]
    private sealed class SpItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) Build()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE SpItem (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    private static string RawName(SqliteConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Name FROM SpItem WHERE Id = {id}";
        return (string)cmd.ExecuteScalar()!;
    }

    // Control: the full-rollback path already restores the baseline, so the edit re-applies.
    [Fact]
    public async Task FullRollback_reapplies_a_modified_entitys_update()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var e = new SpItem { Name = "A" };
        ctx.Add(e); await ctx.SaveChangesAsync();

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            e.Name = "B";
            await ctx.SaveChangesAsync();   // UPDATE to B (uncommitted); baseline advances to B
            await tx.RollbackAsync();       // DB reverts to A; baseline restored to A
        }
        Assert.Equal("A", RawName(cn, e.Id));

        await ctx.SaveChangesAsync();       // B re-applies (current B != restored baseline A)
        Assert.Equal("B", RawName(cn, e.Id));
    }

    [Fact]
    public async Task RollbackToSavepoint_reapplies_a_modified_entitys_update()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var e = new SpItem { Name = "A" };
        ctx.Add(e); await ctx.SaveChangesAsync();

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await tx.CreateSavepointAsync("sp");
        e.Name = "B";
        await ctx.SaveChangesAsync();       // UPDATE to B (uncommitted); baseline advances to B
        await tx.RollbackToSavepointAsync("sp"); // DB reverts to A; baseline must be restored to A
        Assert.Equal("A", RawName(cn, e.Id));

        await ctx.SaveChangesAsync();       // B must re-apply (was silently dropped before the fix)
        await tx.CommitAsync();

        Assert.Equal("B", RawName(cn, e.Id));
    }
}
