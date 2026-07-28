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
/// Entity-typed Take/Skip must apply the tenant (and user global) filter BEFORE the LIMIT/OFFSET window, not
/// after. Applying it after ran the window over ALL tenants' rows and then filtered — silently dropping the
/// caller's OWN rows (Take) or shifting the page (Skip). No foreign-tenant rows leak, but the caller loses its
/// own data in pagination, the most common query shape. The shared filter-injection pipeline means user global
/// filters (soft-delete) hit the identical bug.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TenantPagingFilterOrderTests
{
    private sealed class FixedTenant : ITenantProvider
    {
        private readonly string _id;
        public FixedTenant(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    [Table("TpfRow")]
    public class Row
    {
        [Key] public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public bool IsDeleted { get; set; }
    }

    private static SqliteConnection Db()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        // T1 owns odd Ids, T2 owns even Ids (interleaved). None deleted.
        cmd.CommandText = "CREATE TABLE TpfRow (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL, IsDeleted INTEGER NOT NULL DEFAULT 0);" +
                          "INSERT INTO TpfRow VALUES (1,'T1',0),(2,'T2',0),(3,'T1',0),(4,'T2',0),(5,'T1',0),(6,'T2',0);";
        cmd.ExecuteNonQuery();
        return cn;
    }

    [Fact]
    public async Task Tenant_OrderBy_Take_returns_top_N_of_the_tenant()
    {
        using var cn = Db();
        var opts = new DbContextOptions { TenantProvider = new FixedTenant("T1"), OnModelCreating = mb => mb.Entity<Row>() };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var top3 = await ctx.Query<Row>().OrderBy(r => r.Id).Take(3).ToListAsync();
        Assert.All(top3, r => Assert.Equal("T1", r.TenantId));   // no leak
        Assert.Equal(new[] { 1, 3, 5 }, top3.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Tenant_OrderBy_Skip_Take_pages_within_the_tenant()
    {
        using var cn = Db();
        var opts = new DbContextOptions { TenantProvider = new FixedTenant("T1"), OnModelCreating = mb => mb.Entity<Row>() };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        var page = await ctx.Query<Row>().OrderBy(r => r.Id).Skip(1).Take(2).ToListAsync();
        Assert.Equal(new[] { 3, 5 }, page.Select(r => r.Id).ToArray());
    }

    [Fact]
    public async Task Global_filter_OrderBy_Take_excludes_before_the_window()
    {
        using var cn = Db();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "UPDATE TpfRow SET IsDeleted = 1 WHERE Id IN (2,4);"; // delete some rows
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb => mb.Entity<Row>() };
        opts.AddGlobalFilter<Row>(r => !r.IsDeleted);
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Non-deleted Ids ordered = [1,3,5,6]; Take(3) = [1,3,5].
        var top3 = await ctx.Query<Row>().OrderBy(r => r.Id).Take(3).ToListAsync();
        Assert.Equal(new[] { 1, 3, 5 }, top3.Select(r => r.Id).ToArray());
    }
}
