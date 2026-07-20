using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Security: an ordered / top-N Include must compute the top-N over the TENANT-scoped set — the tenant
/// predicate sits inside the ROW_NUMBER window, so a forged cross-tenant child (even with a higher ordering
/// key) can neither leak into the graph nor steal a slot from a legitimate child. A naive cap-then-filter
/// (rank first, tenant-filter after) would drop a real row and/or expose another tenant's data.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OrderedIncludeTenantIsolationTests
{
    [Table("OitOwner")]
    public class Owner
    {
        [Key] public int Id { get; set; }
        public string TenantId { get; set; } = "";
        public List<Child> Children { get; set; } = new();
    }

    [Table("OitChild")]
    public class Child
    {
        [Key] public int Id { get; set; }
        public int OwnerId { get; set; }
        public string TenantId { get; set; } = "";
        public int Score { get; set; }
    }

    private sealed class FixedTenantProvider : ITenantProvider
    {
        private readonly string _id;
        public FixedTenantProvider(string id) => _id = id;
        public object GetCurrentTenantId() => _id;
    }

    private static DbContext Ctx(SqliteConnection cn, string tenant) =>
        new(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantProvider = new FixedTenantProvider(tenant),
            TenantColumnName = "TenantId",
            OnModelCreating = mb =>
            {
                mb.Entity<Owner>().HasKey(o => o.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Owner>().HasMany(o => o.Children).WithOne().HasForeignKey(c => c.OwnerId, o => o.Id);
            }
        }, ownsConnection: false);

    [Fact]
    public void Forged_cross_tenant_child_cannot_leak_into_or_steal_a_topN_slot()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OitOwner (Id INTEGER PRIMARY KEY, TenantId TEXT NOT NULL);
                CREATE TABLE OitChild (Id INTEGER PRIMARY KEY, OwnerId INTEGER NOT NULL, TenantId TEXT NOT NULL, Score INTEGER NOT NULL);
                INSERT INTO OitOwner VALUES (1, 't1');
                -- Legit t1 children of owner 1: scores 10, 20.
                INSERT INTO OitChild VALUES (1, 1, 't1', 10), (2, 1, 't1', 20);
                -- Forged: a t2 child pointing at owner 1 with a HIGHER score than any legit child.
                INSERT INTO OitChild VALUES (3, 1, 't2', 100);
                """;
            cmd.ExecuteNonQuery();
        }
        using var ctx = Ctx(cn, "t1");

        var owner = ctx.Query<Owner>()
            .Include(o => o.Children.OrderByDescending(c => c.Score).Take(2))
            .ToList().Single();

        // Top-2 by score of t1's children = [20, 10]. The forged score-100 t2 child must NOT appear,
        // and must NOT have displaced the legit score-10 child.
        Assert.Equal(new[] { 20, 10 }, owner.Children.Select(c => c.Score).ToArray());
        Assert.DoesNotContain(owner.Children, c => c.TenantId == "t2");
    }
}
