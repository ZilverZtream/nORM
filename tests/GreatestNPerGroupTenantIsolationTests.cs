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
/// Security regression (cross-tenant isolation): the greatest-N-per-group correlated subquery
/// (<c>g.OrderByDescending(x => x.Date).First().Amount</c>) re-scans the grouped table correlated only on
/// the group key. When the group key is NOT tenant-unique (e.g. CustomerId that collides across tenants),
/// the re-scan must still be scoped to the current tenant — otherwise another tenant's newer row would win
/// the ordering and leak its scalar. Run-and-diff: two tenants share a CustomerId; the other tenant's row is
/// newer with a distinctive amount, so any leak is unambiguous.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class GreatestNPerGroupTenantIsolationTests
{
    [Table("TgnOrder")]
    private sealed class TgnOrder
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public int OrderDate { get; set; }
        public int Amount { get; set; }
        public string TenantId { get; set; } = "";
    }

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _tenantId;
        public FixedTenantProvider(string tenantId) => _tenantId = tenantId;
        public object GetCurrentTenantId() => _tenantId;
    }

    [Theory]
    [InlineData("T1", 10)]
    [InlineData("T2", 999)]
    public async Task Greatest_n_per_group_subquery_is_tenant_isolated(string tenant, int expected)
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE TgnOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, OrderDate INTEGER NOT NULL, Amount INTEGER NOT NULL, TenantId TEXT NOT NULL);" +
                // Same CustomerId=1 in both tenants. T2's row is NEWER (date 2) with a distinctive amount, so a
                // re-scan that ignored the tenant would return 999 to tenant T1.
                "INSERT INTO TgnOrder VALUES (1,1,1,10,'T1'),(2,1,2,999,'T2');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { TenantProvider = new FixedTenantProvider(tenant) };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);

        var byCustomer = (await ctx.Query<TgnOrder>()
                .GroupBy(o => o.CustomerId)
                .Select(g => new { g.Key, Latest = g.OrderByDescending(o => o.OrderDate).First().Amount })
                .ToListAsync())
            .ToDictionary(x => x.Key, x => x.Latest);

        // Each tenant must see only its OWN latest row's amount — never the other tenant's.
        Assert.Equal(expected, byCustomer[1]);
    }
}
