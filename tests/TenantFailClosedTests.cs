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

#nullable enable

namespace nORM.Tests;

[Trait("Category", "Fast")]
public class TenantFailClosedTests
{
    [Fact]
    public void Tenant_query_without_mapped_tenant_column_throws_norm_configuration_exception()
    {
        using var cn = CreateConnection("CREATE TABLE TfcNoTenant (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);");
        using var ctx = CreateTenantContext(cn, new FixedTenantProvider(1));

        var ex = Assert.Throws<NormConfigurationException>(() =>
            ctx.Query<TfcNoTenant>().ToList());

        Assert.Contains("does not map tenant column", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("fails closed", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Tenant_insert_without_mapped_tenant_column_throws_norm_configuration_exception()
    {
        using var cn = CreateConnection("CREATE TABLE TfcNoTenant (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);");
        using var ctx = CreateTenantContext(cn, new FixedTenantProvider(1));

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
            ctx.InsertAsync(new TfcNoTenant { Id = 1, Name = "blocked" }));

        Assert.Contains("does not map tenant column", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("fails closed", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Tenant_provider_exception_is_wrapped_as_norm_configuration_exception()
    {
        using var cn = CreateConnection("CREATE TABLE TfcTenantItem (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL);");
        using var ctx = CreateTenantContext(cn, new ThrowingTenantProvider());

        var ex = Assert.Throws<NormConfigurationException>(() =>
            ctx.Query<TfcTenantItem>().ToList());

        Assert.Contains("TenantProvider.GetCurrentTenantId() failed", ex.Message, StringComparison.OrdinalIgnoreCase);
        Assert.IsType<InvalidOperationException>(ex.InnerException);
    }

    [Fact]
    public async Task Tenant_bulk_update_without_mapped_tenant_column_throws_norm_configuration_exception()
    {
        using var cn = CreateConnection("CREATE TABLE TfcNoTenant (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);");
        using var ctx = CreateTenantContext(cn, new FixedTenantProvider(1));

        var ex = await Assert.ThrowsAsync<NormConfigurationException>(() =>
            ctx.BulkUpdateAsync(new[] { new TfcNoTenant { Id = 1, Name = "blocked" } }));

        Assert.Contains("does not map tenant column", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    private static SqliteConnection CreateConnection(string schemaSql)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = schemaSql;
        cmd.ExecuteNonQuery();
        return cn;
    }

    private static DbContext CreateTenantContext(SqliteConnection cn, ITenantProvider provider)
        => new(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = provider,
            TenantColumnName = "TenantId"
        });

    [Table("TfcNoTenant")]
    private sealed class TfcNoTenant
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
    }

    [Table("TfcTenantItem")]
    private sealed class TfcTenantItem
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Name { get; set; } = "";
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    private sealed class ThrowingTenantProvider : ITenantProvider
    {
        public object GetCurrentTenantId() => throw new InvalidOperationException("tenant source unavailable");
    }
}
