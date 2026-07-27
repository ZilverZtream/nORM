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

[Xunit.Trait("Category", "Fast")]
public class ZZZ_SavepointLostUpdateRepro
{
    [Table("SpItem")]
    private class SpItem
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

    // CONTROL: full rollback path — should already work (restores _transactionValuesSnapshot).
    [Fact]
    public async Task FullRollback_ModifiedEntity_ReappliesUpdate()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var e = new SpItem { Name = "A" };
        ctx.Add(e);
        await ctx.SaveChangesAsync();          // committed row, Id assigned, Name="A"

        await using (var tx = await ctx.Database.BeginTransactionAsync())
        {
            e.Name = "B";
            await ctx.SaveChangesAsync();      // UPDATE to B (uncommitted), baseline advances to B
            await tx.RollbackAsync();          // DB row reverts to A
        }
        Assert.Equal("A", RawName(cn, e.Id));  // rolled back

        await ctx.SaveChangesAsync();          // baseline should be restored to A, so B re-applies
        Assert.Equal("B", RawName(cn, e.Id));  // user's intent B must persist
    }

    // SUSPECT: savepoint rollback path — claim: baseline NOT restored -> silent lost update.
    [Fact]
    public async Task RollbackToSavepoint_ModifiedEntity_ReappliesUpdate()
    {
        var (cn, ctx) = Build();
        using var _ = cn; await using var __ = ctx;

        var e = new SpItem { Name = "A" };
        ctx.Add(e);
        await ctx.SaveChangesAsync();          // committed row, Id assigned, Name="A"

        await using var tx = await ctx.Database.BeginTransactionAsync();
        await tx.CreateSavepointAsync("sp");
        e.Name = "B";
        await ctx.SaveChangesAsync();          // UPDATE to B (uncommitted), baseline advances to B
        await tx.RollbackToSavepointAsync("sp"); // DB row reverts to A
        Assert.Equal("A", RawName(cn, e.Id));

        await ctx.SaveChangesAsync();          // must re-apply B, but baseline stuck at B -> no UPDATE
        await tx.CommitAsync();

        Assert.Equal("B", RawName(cn, e.Id));  // EXPECT FAIL: actual "A" (silent lost update)
    }
}
