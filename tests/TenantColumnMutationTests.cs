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

/// <summary>
/// Changing the tenant column on a tracked entity would move the row out of the current tenant
/// (the UPDATE matches the current tenant in its WHERE clause but rewrites the tenant value in the
/// SET), silently removing it from that tenant's data. Regression: the batched SaveChanges path
/// must reject the mutation loudly, matching the direct/bulk paths' ValidateTenantContext.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class TenantColumnMutationTests
{
    [Table("TcmItem")]
    public class TcmItem
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string TenantId { get; set; } = string.Empty;
    }

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _tenantId;
        public FixedTenantProvider(string tenantId) => _tenantId = tenantId;
        public object GetCurrentTenantId() => _tenantId;
    }

    private static (SqliteConnection cn, DbContext ctx) Create(string tenantId)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE TcmItem (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, TenantId TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { TenantProvider = new FixedTenantProvider(tenantId) };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    private static long CountInTenant(SqliteConnection cn, int id, string tenant)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM TcmItem WHERE Id=@id AND TenantId=@t";
        cmd.Parameters.AddWithValue("@id", id);
        cmd.Parameters.AddWithValue("@t", tenant);
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    [Fact]
    public async Task Changing_the_tenant_column_on_a_tracked_entity_is_rejected_and_does_not_move_the_row()
    {
        var (cn, ctx) = Create("A");
        using var _cn = cn;
        using var _ctx = ctx;

        await ctx.InsertAsync(new TcmItem { Id = 1, Name = "n", TenantId = "A" });

        // Load the tracked row (tenant A) and tamper with its tenant column.
        var item = ctx.Query<TcmItem>().First(i => i.Id == 1);
        item.TenantId = "B";

        // The save must throw rather than silently move the row into tenant B.
        await Assert.ThrowsAnyAsync<InvalidOperationException>(() => ctx.SaveChangesAsync());

        // The row stays in tenant A; tenant A did not lose it and tenant B did not gain it.
        Assert.Equal(1, CountInTenant(cn, 1, "A"));
        Assert.Equal(0, CountInTenant(cn, 1, "B"));
    }

    [Fact]
    public async Task Updating_a_non_tenant_column_on_a_tracked_entity_still_works()
    {
        var (cn, ctx) = Create("A");
        using var _cn = cn;
        using var _ctx = ctx;

        await ctx.InsertAsync(new TcmItem { Id = 1, Name = "n", TenantId = "A" });

        var item = ctx.Query<TcmItem>().First(i => i.Id == 1);
        item.Name = "updated"; // a normal column change must NOT be blocked
        await ctx.SaveChangesAsync();

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Name FROM TcmItem WHERE Id=1";
        Assert.Equal("updated", (string)cmd.ExecuteScalar()!);
        Assert.Equal(1, CountInTenant(cn, 1, "A"));
    }
}
