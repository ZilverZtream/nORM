using System.Collections.Generic;
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

/// <summary>
/// Pins IgnoreQueryFilters: it bypasses the user's global filters (soft-delete) for the root
/// query, but NEVER the tenant boundary — a query can't read another tenant's rows through it.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class IgnoreQueryFiltersContractTests
{
    [Table("IqfRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public bool IsDeleted { get; set; }
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    private static async Task ExecAsync(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    [Fact]
    public async Task IgnoreQueryFilters_bypasses_soft_delete()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await ExecAsync(cn, """
            CREATE TABLE IqfRow (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
            INSERT INTO IqfRow VALUES (1, 1, 0);  -- live
            INSERT INTO IqfRow VALUES (2, 1, 1);  -- soft-deleted
            """);
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id) };
        opts.AddGlobalFilter<Row>(r => !r.IsDeleted);
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        // Default: soft-delete filter hides row 2.
        Assert.Equal(new[] { 1 }, (await ctx.Query<Row>().ToListAsync()).Select(r => r.Id).OrderBy(i => i).ToArray());

        // IgnoreQueryFilters: both rows, including the deleted one.
        Assert.Equal(new[] { 1, 2 },
            (await ctx.Query<Row>().IgnoreQueryFilters().ToListAsync()).Select(r => r.Id).OrderBy(i => i).ToArray());

        // Still composes with Where and a terminal count.
        Assert.Equal(1, await ctx.Query<Row>().IgnoreQueryFilters().Where(r => r.IsDeleted).CountAsync());
    }

    [Fact]
    public async Task IgnoreQueryFilters_never_bypasses_tenant_isolation()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await ExecAsync(cn, """
            CREATE TABLE IqfRow (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
            INSERT INTO IqfRow VALUES (1, 10, 0);  -- tenant 10, live
            INSERT INTO IqfRow VALUES (2, 10, 1);  -- tenant 10, soft-deleted
            INSERT INTO IqfRow VALUES (3, 20, 1);  -- tenant 20, soft-deleted (must stay hidden)
            """);
        var opts = new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenantProvider(10),
            OnModelCreating = mb => mb.Entity<Row>().HasKey(r => r.Id)
        };
        opts.AddGlobalFilter<Row>(r => !r.IsDeleted);
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        // IgnoreQueryFilters reveals tenant 10's soft-deleted row (2) but NOT tenant 20's row (3):
        // the tenant predicate is a security boundary and is never bypassed.
        var ids = (await ctx.Query<Row>().IgnoreQueryFilters().ToListAsync()).Select(r => r.Id).OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1, 2 }, ids);
    }
}
