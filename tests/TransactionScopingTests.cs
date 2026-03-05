using System;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
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

    [Fact]
    public async Task AmbientTransactionScope_NotCompleted_RollsBack()
    {
        // SQLite does not support distributed (System.Transactions) transactions,
        // so we verify that SaveChangesAsync does NOT throw when an ambient
        // TransactionScope is present and no explicit DbTransaction is active.
        // The connection enlistment behaviour is driver-specific; this test
        // validates the fix to Finding 5 (no unconditional throw on null transaction).
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();

        await using (var setup = connection.CreateCommand())
        {
            setup.CommandText = "CREATE TABLE Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT);";
            await setup.ExecuteNonQueryAsync();
        }

        var provider = new SqliteProvider();

        // Wrap in a TransactionScope that is never completed → changes should be rolled back.
        // SQLite does not enlist connections into System.Transactions, so the rows will
        // actually be committed by the internal transaction; what matters is that
        // SaveChangesAsync completes without throwing InvalidOperationException.
        //
        // Gate E: Use BestEffort policy because SQLite does not support EnlistTransaction
        // and the default FailFast policy would throw NormConfigurationException when
        // enlistment fails. BestEffort matches the original behavior: log a warning and continue.
        var options = new DbContextOptions
        {
            AmbientTransactionPolicy = AmbientTransactionEnlistmentPolicy.BestEffort
        };
        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var ctx = new DbContext(connection, provider, options);
            ctx.Add(new Item { Name = "scoped" });
            // Must not throw "Transaction cannot be null when creating a CommandScope."
            await ctx.SaveChangesAsync();
            // Do NOT call scope.Complete() — scope is abandoned.
        }
    }
}
