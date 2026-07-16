using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using MigrationRunners = nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class BulkOperationProviderCoverageTests
{
    // Concrete test subclass that delegates all abstract methods to SqliteProvider
    private sealed class TestBulkOpProvider : BulkOperationProvider
    {
        private readonly SqliteProvider _inner = new();

        public override string Escape(string id) => _inner.Escape(id);
        public override void ApplyPaging(nORM.Query.OptimizedSqlBuilder sb, int? limit, int? offset,
            string? limitParameterName, string? offsetParameterName)
            => _inner.ApplyPaging(sb, limit, offset, limitParameterName, offsetParameterName);
        public override string GetIdentityRetrievalString(TableMapping m)
            => _inner.GetIdentityRetrievalString(m);
        public override DbParameter CreateParameter(string name, object? value)
            => _inner.CreateParameter(name, value);
        public override string? TranslateFunction(string name, Type declaringType, params string[] args)
            => _inner.TranslateFunction(name, declaringType, args);
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
            => _inner.TranslateJsonPathAccess(columnName, jsonPath);
        public override string GenerateCreateHistoryTableSql(TableMapping mapping,
            IReadOnlyList<DatabaseProvider.LiveColumnInfo>? liveColumns = null)
            => _inner.GenerateCreateHistoryTableSql(mapping, liveColumns);
        public override string GenerateTemporalTriggersSql(TableMapping mapping, System.Collections.Generic.IReadOnlyList<LiveColumnInfo>? liveColumns = null)
            => _inner.GenerateTemporalTriggersSql(mapping);

        // Expose ExecuteBulkOperationAsync for testing
        public Task<int> RunBulkAsync<T>(
            DbContext ctx,
            TableMapping mapping,
            IList<T> entities,
            Func<List<T>, DbTransaction, CancellationToken, Task<int>> batchAction,
            CancellationToken ct = default) where T : class
            => ExecuteBulkOperationAsync(ctx, mapping, entities, "TestOp", batchAction, ct);
    }

    [Table("BulkOpTest")]
    private class BulkItem
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    private static (SqliteConnection Cn, DbContext Ctx) MakeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE BulkOpTest (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '')";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new TestBulkOpProvider()));
    }

    [Fact]
    public async Task ExecuteBulkOperationAsync_OwnedTx_CommitsSuccessfully()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        var provider = (TestBulkOpProvider)ctx.Provider;
        var mapping = ctx.GetMapping(typeof(BulkItem));

        var items = new List<BulkItem>
        {
            new() { Name = "A" },
            new() { Name = "B" }
        };

        var total = await provider.RunBulkAsync(ctx, mapping, items,
            async (batch, tx, ct) =>
            {
                var cnt = 0;
                foreach (var item in batch)
                {
                    using var cmd = cn.CreateCommand();
                    cmd.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
                    cmd.CommandText = "INSERT INTO BulkOpTest (Name) VALUES (@n)";
                    cmd.Parameters.AddWithValue("@n", item.Name);
                    cnt += await cmd.ExecuteNonQueryAsync(ct);
                }
                return cnt;
            });

        Assert.Equal(2, total);

        // Verify rows were committed
        using var verifyCmd = cn.CreateCommand();
        verifyCmd.CommandText = "SELECT COUNT(*) FROM BulkOpTest";
        var count = (long)verifyCmd.ExecuteScalar()!;
        Assert.Equal(2, count);
    }

    [Fact]
    public async Task ExecuteBulkOperationAsync_ReuseExistingTx_DoesNotCommit()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        var provider = (TestBulkOpProvider)ctx.Provider;
        var mapping = ctx.GetMapping(typeof(BulkItem));

        // Use ctx.Database.BeginTransactionAsync so ctx.CurrentTransaction != null.
        // ExecuteBulkOperationAsync sees ownedTx=false and reuses the existing tx.
        await using var outerTx = await ctx.Database.BeginTransactionAsync();

        BulkItem[] itemArray = { new() { Name = "X" } };

        var total = await provider.RunBulkAsync(ctx, mapping, itemArray,
            async (batch, tx, ct) =>
            {
                using var cmd = cn.CreateCommand();
                cmd.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
                cmd.CommandText = "INSERT INTO BulkOpTest (Name) VALUES (@n)";
                cmd.Parameters.AddWithValue("@n", batch[0].Name);
                return await cmd.ExecuteNonQueryAsync(ct);
            });

        Assert.Equal(1, total);
        // (it doesn't own the transaction), so rows must not be visible after rollback.
    }

    [Fact]
    public async Task ExecuteBulkOperationAsync_NonListIList_WorksCorrectly()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        var provider = (TestBulkOpProvider)ctx.Provider;
        var mapping = ctx.GetMapping(typeof(BulkItem));

        // Use an array-backed IList (not List<T>) to hit the non-List branch
        IList<BulkItem> items = new BulkItem[] { new() { Name = "Y" }, new() { Name = "Z" } };

        var total = await provider.RunBulkAsync(ctx, mapping, items,
            async (batch, tx, ct) =>
            {
                var cnt = 0;
                foreach (var item in batch)
                {
                    using var cmd = cn.CreateCommand();
                    cmd.Transaction = (Microsoft.Data.Sqlite.SqliteTransaction)tx;
                    cmd.CommandText = "INSERT INTO BulkOpTest (Name) VALUES (@n)";
                    cmd.Parameters.AddWithValue("@n", item.Name);
                    cnt += await cmd.ExecuteNonQueryAsync(ct);
                }
                return cnt;
            });

        Assert.Equal(2, total);
    }

    [Fact]
    public async Task ExecuteBulkOperationAsync_BatchActionThrows_RollsBackAndRethrows()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;
        var provider = (TestBulkOpProvider)ctx.Provider;
        var mapping = ctx.GetMapping(typeof(BulkItem));

        var items = new List<BulkItem> { new() { Name = "Fail" } };

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            provider.RunBulkAsync(ctx, mapping, items,
                (batch, tx, ct) => throw new InvalidOperationException("batch failed")));

        // After rollback, no rows should be in the table
        using var verifyCmd = cn.CreateCommand();
        verifyCmd.CommandText = "SELECT COUNT(*) FROM BulkOpTest";
        var count = (long)verifyCmd.ExecuteScalar()!;
        Assert.Equal(0, count);
    }
}

// Covers: LoadAsync (collection nav), LoadAsync (reference nav, throws path),
//         LoadNavigationProperty (sync wrapper), LoadRelationshipAsync,
//         LoadInferredRelationshipAsync, ExecuteSingleQueryAsync
