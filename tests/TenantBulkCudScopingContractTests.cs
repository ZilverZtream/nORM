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

/// <summary>
/// Contract: bulk CUD stays tenant-scoped (tenant matrix cell). A whole-table ExecuteUpdate or
/// ExecuteDelete issued from a tenant context must touch ONLY the caller's rows - the other
/// tenant's rows are asserted untouched through raw SQL, outside any filter. Writes carrying a
/// wrong or missing tenant id keep failing closed on the bulk-insert path (the forged-PK
/// BulkUpdate direction is already covered by AdversarialBulkTenantTests and cited, not
/// duplicated).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TenantBulkCudScopingContractTests
{
    [Table("TenBulk_Row")]
    private class Row
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public string TenantId { get; set; } = "";
    }

    private sealed class FixedTenant : ITenantProvider
    {
        private readonly string _id;
        public FixedTenant(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateTenantContext(string tenant)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE TenBulk_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, TenantId TEXT NOT NULL);" +
                "INSERT INTO TenBulk_Row VALUES (1, 10, 'T1'), (2, 20, 'T1'), (3, 10, 'T2'), (4, 20, 'T2');";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>(),
            TenantProvider = new FixedTenant(tenant)
        });
        return (cn, ctx);
    }

    private static long Scalar(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        return (long)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task Whole_table_execute_update_touches_only_the_callers_tenant()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        var affected = await ctx.Query<Row>().ExecuteUpdateAsync(s => s.SetProperty(r => r.V, 99));
        Assert.Equal(2, affected);

        // Raw truth, outside any filter: T2's rows are byte-untouched.
        Assert.Equal(2, Scalar(cn, "SELECT COUNT(*) FROM TenBulk_Row WHERE TenantId = 'T1' AND V = 99;"));
        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM TenBulk_Row WHERE TenantId = 'T2' AND V = 99;"));
        Assert.Equal(2, Scalar(cn, "SELECT COUNT(*) FROM TenBulk_Row WHERE TenantId = 'T2' AND V IN (10, 20);"));
    }

    [Fact]
    public async Task Predicated_execute_update_stays_within_the_tenant()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        // The predicate matches one row per tenant; only T1's may change.
        var affected = await ctx.Query<Row>().Where(r => r.V == 10)
            .ExecuteUpdateAsync(s => s.SetProperty(r => r.V, 11));
        Assert.Equal(1, affected);
        Assert.Equal(11, Scalar(cn, "SELECT V FROM TenBulk_Row WHERE Id = 1;"));
        Assert.Equal(10, Scalar(cn, "SELECT V FROM TenBulk_Row WHERE Id = 3;"));
    }

    [Fact]
    public async Task Whole_table_execute_delete_removes_only_the_callers_tenant()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        var affected = await ctx.Query<Row>().ExecuteDeleteAsync();
        Assert.Equal(2, affected);

        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM TenBulk_Row WHERE TenantId = 'T1';"));
        Assert.Equal(2, Scalar(cn, "SELECT COUNT(*) FROM TenBulk_Row WHERE TenantId = 'T2';"));
    }

    [Fact]
    public async Task Bulk_insert_fails_closed_on_missing_or_foreign_tenant_id()
    {
        var (cn, ctx) = CreateTenantContext("T1");
        using var _cn = cn;
        await using var _ = ctx;

        // Missing tenant id: fail closed, nothing written.
        await Assert.ThrowsAnyAsync<NormException>(() => ctx.BulkInsertAsync(new[]
        {
            new Row { Id = 10, V = 1, TenantId = null! }
        }));
        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM TenBulk_Row WHERE Id = 10;"));

        // Foreign tenant id: fail closed, nothing written.
        await Assert.ThrowsAnyAsync<NormException>(() => ctx.BulkInsertAsync(new[]
        {
            new Row { Id = 11, V = 1, TenantId = "T2" }
        }));
        Assert.Equal(0, Scalar(cn, "SELECT COUNT(*) FROM TenBulk_Row WHERE Id = 11;"));

        // Correct tenant id: the same path writes.
        var inserted = await ctx.BulkInsertAsync(new[]
        {
            new Row { Id = 12, V = 1, TenantId = "T1" }
        });
        Assert.Equal(1, inserted);
        Assert.Equal(1, Scalar(cn, "SELECT COUNT(*) FROM TenBulk_Row WHERE Id = 12 AND TenantId = 'T1';"));
    }
}
