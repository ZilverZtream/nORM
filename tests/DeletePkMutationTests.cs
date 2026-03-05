using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// CT-DELETE: Verifies that SaveChanges throws when the primary key of a tracked entity
/// is mutated before a DELETE, preventing deletion of the wrong row.
/// </summary>
public class DeletePkMutationTests
{
    [Table("DpmItem")]
    private class DpmItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Table("DpmComposite")]
    private class DpmComposite
    {
        [Key]
        [Column(Order = 0)]
        public int PartA { get; set; }

        [Key]
        [Column(Order = 1)]
        public int PartB { get; set; }

        public string Name { get; set; } = string.Empty;
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> CreateContextAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());

        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE DpmItem (
                Id   INTEGER PRIMARY KEY,
                Name TEXT    NOT NULL
            );
            INSERT INTO DpmItem (Id, Name) VALUES (1, 'Row1');
            INSERT INTO DpmItem (Id, Name) VALUES (2, 'Row2');
            CREATE TABLE DpmComposite (
                PartA INTEGER NOT NULL,
                PartB INTEGER NOT NULL,
                Name  TEXT    NOT NULL,
                PRIMARY KEY (PartA, PartB)
            );
            INSERT INTO DpmComposite (PartA, PartB, Name) VALUES (1, 10, 'Comp1');";
        await cmd.ExecuteNonQueryAsync();

        return (cn, ctx);
    }

    [Fact]
    public async Task Delete_AfterPkMutation_Throws()
    {
        var (cn, ctx) = await CreateContextAsync();
        using var _cn = cn;
        using var _ctx = ctx;

        // Attach entity with PK=1 (captures OriginalKey=1)
        var item = new DpmItem { Id = 1, Name = "Row1" };
        ctx.Attach(item);

        // Mutate the primary key then mark for deletion
        item.Id = 999;
        ctx.Remove(item);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.SaveChangesAsync());

        Assert.Contains("Primary key mutation", ex.Message);
        Assert.Contains("DpmItem", ex.Message);
    }

    [Fact]
    public async Task Delete_WithUnchangedPk_Succeeds()
    {
        var (cn, ctx) = await CreateContextAsync();
        using var _cn = cn;
        using var _ctx = ctx;

        var item = new DpmItem { Id = 1, Name = "Row1" };
        ctx.Attach(item);
        ctx.Remove(item);

        var affected = await ctx.SaveChangesAsync();
        Assert.Equal(1, affected);

        using var check = cn.CreateCommand();
        check.CommandText = "SELECT COUNT(*) FROM DpmItem WHERE Id=1";
        Assert.Equal(0L, Convert.ToInt64(check.ExecuteScalar()));
    }

    [Fact]
    public async Task Delete_CompositePkMutation_Throws()
    {
        var (cn, ctx) = await CreateContextAsync();
        using var _cn = cn;
        using var _ctx = ctx;

        var comp = new DpmComposite { PartA = 1, PartB = 10, Name = "Comp1" };
        ctx.Attach(comp);

        // Mutate one part of the composite PK
        comp.PartA = 99;
        ctx.Remove(comp);

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.SaveChangesAsync());

        Assert.Contains("Primary key mutation", ex.Message);
    }
}
