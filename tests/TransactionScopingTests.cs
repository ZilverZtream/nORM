using System;
using System.Data.Common;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class TransactionScopingTests
{
    private class Item
    {
        [System.ComponentModel.DataAnnotations.Key]
        [System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
    }

    [Fact]
    public async Task AmbientTransaction_RollsBackAllOperations()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await using (var cmd = connection.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);";
            await cmd.ExecuteNonQueryAsync();
        }

        var provider = new SqliteProvider();
        using var ctx = new DbContext(connection, provider);

        await using var tx = await ctx.Database.BeginTransactionAsync();

        await ctx.InsertAsync(new Item { Name = "first" });
        ctx.Add(new Item { Name = "second" });
        await ctx.SaveChangesAsync();

        await tx.RollbackAsync();

        await using var countCmd = connection.CreateCommand();
        countCmd.CommandText = "SELECT COUNT(*) FROM Item";
        var countObj = await countCmd.ExecuteScalarAsync();
        var count = countObj is null ? 0L : Convert.ToInt64(countObj);
        Assert.Equal(0L, count);
    }
}
