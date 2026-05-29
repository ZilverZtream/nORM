using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class TenantProjectionAndTemporalTrackingTests
{
    [Fact]
    public async Task Tenant_filter_applies_before_dto_projection()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await ExecAsync(connection, """
            CREATE TABLE TptProduct (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL, Price TEXT NOT NULL);
            INSERT INTO TptProduct VALUES (1, 10, 'tenant-a', '1.00');
            INSERT INTO TptProduct VALUES (2, 20, 'tenant-b', '2.00');
            """);

        var options = new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenantProvider(10)
        };

        await using var ctx = new DbContext(connection, new SqliteProvider(), options);
        var rows = await ctx.Query<TptProduct>()
            .Select(p => new TptProductDto { Id = p.Id, Name = p.Name })
            .ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal(1, row.Id);
        Assert.Equal("tenant-a", row.Name);
    }

    [Fact]
    public async Task Tenant_filter_applies_before_dto_projection_with_post_projection_where()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await ExecAsync(connection, """
            CREATE TABLE TptProduct (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL, Price TEXT NOT NULL);
            INSERT INTO TptProduct VALUES (1, 10, 'tenant-a', '1.00');
            INSERT INTO TptProduct VALUES (2, 20, 'tenant-b', '2.00');
            """);

        var options = new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenantProvider(10)
        };

        await using var ctx = new DbContext(connection, new SqliteProvider(), options);
        var rows = await ctx.Query<TptProduct>()
            .Select(p => new TptProductDto { Id = p.Id, Name = p.Name })
            .Where(p => p.Name.Contains("tenant"))
            .ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal(1, row.Id);
        Assert.Equal("tenant-a", row.Name);
    }

    [Fact]
    public async Task Temporal_AsOf_returns_historical_state_even_when_current_entity_is_tracked()
    {
        await using var connection = new SqliteConnection("Data Source=:memory:");
        await connection.OpenAsync();
        await ExecAsync(connection, """
            CREATE TABLE TptTemporalProduct (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL);
            """);

        var options = new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenantProvider(10),
            OnModelCreating = mb => mb.Entity<TptTemporalProduct>()
        };
        options.EnableTemporalVersioning();

        await using var ctx = new DbContext(connection, new SqliteProvider(), options);
        Assert.Equal(0, await ctx.Query<TptTemporalProduct>().CountAsync());

        ctx.Add(new TptTemporalProduct { Id = 1, TenantId = 10, Name = "old" });
        await ctx.SaveChangesAsync();

        await Task.Delay(TimeSpan.FromSeconds(2.2));
        var tagName = "tracked-current-" + Guid.NewGuid().ToString("N");
        await ctx.CreateTagAsync(tagName);
        await Task.Delay(TimeSpan.FromSeconds(2.2));

        var current = await ctx.Query<TptTemporalProduct>().Where(p => p.Id == 1).SingleAsync();
        current.Name = "new";
        await ctx.SaveChangesAsync();

        var asOf = await ctx.Query<TptTemporalProduct>()
            .AsOf(tagName)
            .Where(p => p.Id == 1)
            .SingleAsync();

        Assert.Equal("old", asOf.Name);
        Assert.NotSame(current, asOf);
    }

    private static async Task ExecAsync(SqliteConnection connection, string sql)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = sql;
        await command.ExecuteNonQueryAsync();
    }

    [Table("TptProduct")]
    private sealed class TptProduct
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Name { get; set; } = "";
        public decimal Price { get; set; }
    }

    private sealed class TptProductDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("TptTemporalProduct")]
    private sealed class TptTemporalProduct
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }
}
