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
/// CT-1 (PK mutation): Verifies that SaveChanges throws when the primary key of a
/// tracked entity is mutated after attach, preventing silent updates to the wrong row.
/// </summary>
public class PKMutationTests
{
    [Table("Item")]
    private class Item
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    private static async Task<(SqliteConnection cn, DbContext ctx)> CreateContextAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());

        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE Item (
                Id   INTEGER PRIMARY KEY,
                Name TEXT    NOT NULL
            );
            INSERT INTO Item (Id, Name) VALUES (1, 'Original');
            INSERT INTO Item (Id, Name) VALUES (2, 'Other');";
        await cmd.ExecuteNonQueryAsync();

        return (cn, ctx);
    }

    [Fact]
    public async Task SaveChanges_AfterPkMutation_Throws()
    {
        var (cn, ctx) = await CreateContextAsync();
        using var _cn = cn;
        using var _ctx = ctx;

        var item = new Item { Id = 1, Name = "Modified" };
        ctx.Attach(item);

        // Mutate the primary key after tracking — this is the problem scenario
        item.Id = 999;
        item.Name = "MutatedKey";

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ctx.SaveChangesAsync());

        Assert.Contains("Primary key mutation", ex.Message);
        Assert.Contains("Item", ex.Message);
    }

    [Fact]
    public async Task SaveChanges_WithUnchangedPk_Succeeds()
    {
        var (cn, ctx) = await CreateContextAsync();
        using var _cn = cn;
        using var _ctx = ctx;

        var item = new Item { Id = 1, Name = "UpdatedName" };
        ctx.Attach(item);

        // Only mutate a non-key column — this must succeed
        item.Name = "UpdatedName";

        var affected = await ctx.SaveChangesAsync();
        Assert.True(affected >= 0, "SaveChanges must not throw when PK is unchanged");
    }
}
