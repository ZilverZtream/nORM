using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class BatchedBulkFallbackParityTests
{
    [Table("FallbackTenantItem")]
    private class FallbackTenantItem
    {
        [Key]
        public int Id { get; set; }
        public string Name { get; set; } = "";
        public string TenantId { get; set; } = "";
    }

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _tenantId;
        public FixedTenantProvider(string tenantId) => _tenantId = tenantId;
        public object GetCurrentTenantId() => _tenantId;
    }

    private sealed class BatchedOnlySqliteProvider : SqliteProvider
    {
        public override Task<int> BulkUpdateAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct)
            => BatchedUpdateAsync(ctx, m, entities, ct);

        public override Task<int> BulkDeleteAsync<T>(DbContext ctx, TableMapping m, IEnumerable<T> entities, CancellationToken ct)
            => BatchedDeleteAsync(ctx, m, entities, ct);
    }

    [Fact]
    public async Task FallbackBatchedBulkUpdate_IsTenantScoped()
    {
        using var cn = CreateConnection();
        await SeedAsync(cn);
        await using var ctx = CreateTenantContext(cn, bulkBatchSize: 1);

        var updated = await ctx.BulkUpdateAsync(new[]
        {
            new FallbackTenantItem { Id = 1, Name = "A-updated", TenantId = "A" },
            new FallbackTenantItem { Id = 3, Name = "forged-update", TenantId = "A" }
        });

        Assert.Equal(1, updated);
        Assert.Equal("A-updated", await ReadNameAsync(cn, 1));
        Assert.Equal("A-keep", await ReadNameAsync(cn, 2));
        Assert.Equal("B-original", await ReadNameAsync(cn, 3));
    }

    [Fact]
    public async Task FallbackBatchedBulkDelete_IsTenantScopedAcrossBatches()
    {
        using var cn = CreateConnection();
        await SeedAsync(cn);
        await using var ctx = CreateTenantContext(cn, bulkBatchSize: 1);

        var deleted = await ctx.BulkDeleteAsync(new[]
        {
            new FallbackTenantItem { Id = 1, TenantId = "A" },
            new FallbackTenantItem { Id = 3, TenantId = "A" }
        });

        Assert.Equal(1, deleted);
        Assert.Equal(2, await CountAsync(cn));
        Assert.Null(await ReadNameAsync(cn, 1));
        Assert.Equal("A-keep", await ReadNameAsync(cn, 2));
        Assert.Equal("B-original", await ReadNameAsync(cn, 3));
    }

    private static SqliteConnection CreateConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText =
            "CREATE TABLE FallbackTenantItem (" +
            "Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext CreateTenantContext(SqliteConnection cn, int bulkBatchSize)
    {
        var options = new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider("A"),
            BulkBatchSize = bulkBatchSize
        };
        return new DbContext(cn, new BatchedOnlySqliteProvider(), options, ownsConnection: false);
    }

    private static async Task SeedAsync(SqliteConnection cn)
    {
        await InsertAsync(cn, 1, "A-original", "A");
        await InsertAsync(cn, 2, "A-keep", "A");
        await InsertAsync(cn, 3, "B-original", "B");
    }

    private static async Task InsertAsync(SqliteConnection cn, int id, string name, string tenantId)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "INSERT INTO FallbackTenantItem (Id, Name, TenantId) VALUES (@id, @name, @tenant)";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@name", name);
        cmd.Parameters.AddWithValue("@tenant", tenantId);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<string?> ReadNameAsync(SqliteConnection cn, int id)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM FallbackTenantItem WHERE Id = @id";
        cmd.Parameters.AddWithValue("@id", id);
        return await cmd.ExecuteScalarAsync() as string;
    }

    private static async Task<int> CountAsync(SqliteConnection cn)
    {
        await using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM FallbackTenantItem";
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }
}
