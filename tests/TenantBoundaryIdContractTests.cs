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
/// Contract: boundary-valued tenant ids are LEGITIMATE distinct tenants, never confused with
/// "no tenant" (tenant matrix cell). Numeric tenant 0 and negative ids isolate against ordinary
/// ids, and Guid.Empty isolates against a random Guid - in both directions, with no own-row
/// loss. Exotic STRING tenant ids (separators, unicode, case) are covered by
/// AdversarialTenantObjectMatrixTests and cited, not duplicated.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TenantBoundaryIdContractTests
{
    private sealed class FixedTenant : ITenantProvider
    {
        private readonly object _id;
        public FixedTenant(object id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    [Table("TenBoundInt_Row")]
    private class IntRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public int TenantId { get; set; }
    }

    [Fact]
    public async Task Tenant_zero_and_negative_are_distinct_legitimate_tenants()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE TenBoundInt_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, TenantId INTEGER NOT NULL);" +
                "INSERT INTO TenBoundInt_Row VALUES (1, 10, 0), (2, 10, -1), (3, 10, 1);";
            cmd.ExecuteNonQuery();
        }
        DbContext Ctx(object tenant) => new(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<IntRow>(),
            TenantProvider = new FixedTenant(tenant)
        });

        // Contexts share the handed connection; disposing one closes it, so dispose LAST.
        await using var zero = Ctx(0);
        await using var negative = Ctx(-1);
        await using var one = Ctx(1);

        Assert.Equal(1, Assert.Single(await zero.Query<IntRow>().ToListAsync()).Id);
        Assert.Equal(2, Assert.Single(await negative.Query<IntRow>().ToListAsync()).Id);
        Assert.Equal(3, Assert.Single(await one.Query<IntRow>().ToListAsync()).Id);
    }

    [Table("TenBoundGuid_Row")]
    private class GuidRow
    {
        [Key] public int Id { get; set; }
        public int V { get; set; }
        public Guid TenantId { get; set; }
    }

    [Fact]
    public async Task Guid_empty_is_a_distinct_legitimate_tenant()
    {
        var other = Guid.Parse("6f9619ff-8b86-d011-b42d-00c04fc964ff");
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE TenBoundGuid_Row (Id INTEGER PRIMARY KEY, V INTEGER NOT NULL, TenantId TEXT NOT NULL);" +
                $"INSERT INTO TenBoundGuid_Row VALUES (1, 10, '{Guid.Empty}'), (2, 10, '{other}');";
            cmd.ExecuteNonQuery();
        }
        DbContext Ctx(Guid tenant) => new(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<GuidRow>(),
            TenantProvider = new FixedTenant(tenant)
        });

        // Contexts share the handed connection; disposing one closes it, so dispose LAST.
        await using var empty = Ctx(Guid.Empty);
        await using var normal = Ctx(other);

        Assert.Equal(1, Assert.Single(await empty.Query<GuidRow>().ToListAsync()).Id);
        Assert.Equal(2, Assert.Single(await normal.Query<GuidRow>().ToListAsync()).Id);
    }
}
