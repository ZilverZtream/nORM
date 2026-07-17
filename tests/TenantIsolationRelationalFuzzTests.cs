using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
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
/// Seeded tenant-isolation machine over a relational graph: two tenants mutate
/// their own principal/dependent rows through tenant-scoped contexts while the
/// machine occasionally FORGES a cross-tenant foreign key (an emp whose DeptId
/// points at the other tenant's department). After every round, the relational
/// query shapes — navigation projection, correlated Count/Sum, SelectMany,
/// explicit Join, GroupJoin count, and predicate-side aggregates — replay under
/// BOTH tenants and must match that tenant's oracle exactly: no other tenant's
/// rows in any membership, no other tenant's values through any navigation
/// (a forged principal reads as MISSING). This is the sweepable coverage for
/// the leak class previously caught only by hand-written probes.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TenantIsolationRelationalFuzzTests
{
    [Table("TirDept")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Title { get; set; } = "";
        public List<Emp> Emps { get; set; } = new();
    }

    [Table("TirEmp")]
    public class Emp
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Name { get; set; } = "";
        public int DeptId { get; set; }
        public Dept? Dept { get; set; }
    }

    private sealed class FixedTenantProvider(int tenantId) : ITenantProvider
    {
        public object GetCurrentTenantId() => tenantId;
    }

    private static DbContextOptions OptionsFor(int tenant) => new()
    {
        TenantColumnName = "TenantId",
        TenantProvider = new FixedTenantProvider(tenant),
        OnModelCreating = mb =>
        {
            mb.Entity<Dept>().HasKey(d => d.Id);
            mb.Entity<Emp>().HasKey(e => e.Id);
            mb.Entity<Dept>().HasMany(d => d.Emps).WithOne(e => e.Dept!).HasForeignKey(e => e.DeptId, d => d.Id);
        }
    };

    [Fact]
    public async Task Environment_directed_seed_sweep()
    {
        var spec = Environment.GetEnvironmentVariable("NORM_TENANT_FUZZ_SWEEP");
        if (string.IsNullOrEmpty(spec)) return;
        var parts = spec.Split(':');
        var start = int.Parse(parts[0], CultureInfo.InvariantCulture);
        var count = int.Parse(parts[1], CultureInfo.InvariantCulture);
        for (var s = start; s < start + count; s++)
            await Relational_shapes_stay_tenant_isolated(s);
    }

    [Theory]
    [InlineData(20260718)]
    [InlineData(777)]
    [InlineData(481_516_234)]
    public async Task Relational_shapes_stay_tenant_isolated(int seed)
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE TirDept (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Title TEXT NOT NULL);
                CREATE TABLE TirEmp (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL, DeptId INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }

        var tenants = new[] { 10, 20 };
        var contexts = new Dictionary<int, DbContext>
        {
            [10] = new DbContext(cn, new SqliteProvider(), OptionsFor(10), ownsConnection: false),
            [20] = new DbContext(cn, new SqliteProvider(), OptionsFor(20), ownsConnection: false),
        };
        var trackedDepts = new Dictionary<int, Dept>();
        var trackedEmps = new Dictionary<int, Emp>();
        // Global registries: id -> (owning tenant, value). The forged-FK oracle
        // resolves a navigation by looking the dept up here and checking ownership.
        var depts = new Dictionary<int, (int Tenant, string Title)>();
        var emps = new Dictionary<int, (int Tenant, string Name, int DeptId)>();
        var rng = new Random(seed);
        var nextDeptId = 1;
        var nextEmpId = 1;

        try
        {
            for (var round = 0; round < 6; round++)
            {
                foreach (var t in tenants)
                {
                    var ctx = contexts[t];
                    var mutations = rng.Next(1, 4);
                    for (var m = 0; m < mutations; m++)
                    {
                        var myDepts = depts.Where(kv => kv.Value.Tenant == t).Select(kv => kv.Key).ToList();
                        // Forged emps live only in the oracle registry (raw SQL, untracked);
                        // mutation ops target tracked rows only.
                        var myEmps = emps.Where(kv => kv.Value.Tenant == t && trackedEmps.ContainsKey(kv.Key)).Select(kv => kv.Key).ToList();
                        var otherDepts = depts.Where(kv => kv.Value.Tenant != t).Select(kv => kv.Key).ToList();
                        var op = rng.Next(8);
                        if (op == 0 || myDepts.Count == 0)
                        {
                            var d = new Dept { Id = nextDeptId++, TenantId = t, Title = "t" + rng.Next(1000) };
                            ctx.Add(d);
                            trackedDepts[d.Id] = d;
                            depts[d.Id] = (t, d.Title);
                        }
                        else if (op == 1)
                        {
                            var id = myDepts[rng.Next(myDepts.Count)];
                            var d = trackedDepts[id];
                            d.Title = "t" + rng.Next(1000);
                            depts[id] = (t, d.Title);
                        }
                        else if (op == 2)
                        {
                            // Delete only depts without SAME-tenant emps; a forged
                            // reference from the other tenant may dangle (reads NULL).
                            var childless = myDepts.Where(id => !emps.Values.Any(e => e.Tenant == t && e.DeptId == id)).ToList();
                            if (childless.Count == 0) continue;
                            var id = childless[rng.Next(childless.Count)];
                            ctx.Remove(trackedDepts[id]);
                            trackedDepts.Remove(id);
                            depts.Remove(id);
                        }
                        else if (op == 3 || myEmps.Count == 0)
                        {
                            var deptId = myDepts[rng.Next(myDepts.Count)];
                            var e = new Emp { Id = nextEmpId++, TenantId = t, Name = "n" + rng.Next(1000), DeptId = deptId };
                            ctx.Add(e);
                            trackedEmps[e.Id] = e;
                            emps[e.Id] = (t, e.Name, deptId);
                        }
                        else if (op == 4)
                        {
                            var id = myEmps[rng.Next(myEmps.Count)];
                            var e = trackedEmps[id];
                            e.Name = "n" + rng.Next(1000);
                            emps[id] = (t, e.Name, emps[id].DeptId);
                        }
                        else if (op == 5)
                        {
                            var id = myEmps[rng.Next(myEmps.Count)];
                            var e = trackedEmps[id];
                            var deptId = myDepts[rng.Next(myDepts.Count)];
                            e.DeptId = deptId;
                            emps[id] = (t, emps[id].Name, deptId);
                        }
                        else if (op == 6)
                        {
                            var id = myEmps[rng.Next(myEmps.Count)];
                            ctx.Remove(trackedEmps[id]);
                            trackedEmps.Remove(id);
                            emps.Remove(id);
                        }
                        else if (otherDepts.Count > 0)
                        {
                            // FORGE: own-tenant emp pointing at the OTHER tenant's dept.
                            var id = nextEmpId++;
                            var deptId = otherDepts[rng.Next(otherDepts.Count)];
                            var name = "f" + rng.Next(1000);
                            using var cmd = cn.CreateCommand();
                            cmd.CommandText = $"INSERT INTO TirEmp VALUES ({id}, {t}, '{name}', {deptId})";
                            cmd.ExecuteNonQuery();
                            emps[id] = (t, name, deptId);
                        }
                    }
                    await ctx.SaveChangesAsync();
                }

                foreach (var t in tenants)
                {
                    var ctx = contexts[t];
                    var myDepts = depts.Where(kv => kv.Value.Tenant == t).ToDictionary(kv => kv.Key, kv => kv.Value.Title);
                    var myEmps = emps.Where(kv => kv.Value.Tenant == t).ToDictionary(kv => kv.Key, kv => (kv.Value.Name, kv.Value.DeptId));

                    // Navigation projection: a forged/deleted principal reads NULL.
                    var navProj = (await ctx.Query<Emp>()
                            .Select(e => new { e.Id, DeptTitle = (string?)e.Dept!.Title }).ToListAsync())
                        .OrderBy(r => r.Id).Select(r => $"{r.Id}:{r.DeptTitle ?? "~"}").ToList();
                    var navProjExpected = myEmps.OrderBy(kv => kv.Key)
                        .Select(kv => $"{kv.Key}:{(depts.TryGetValue(kv.Value.DeptId, out var d) && d.Tenant == t ? d.Title : "~")}")
                        .ToList();
                    Assert.True(navProjExpected.SequenceEqual(navProj),
                        $"seed={seed} round={round} tenant={t} navProj: expected [{string.Join(" ", navProjExpected)}] got [{string.Join(" ", navProj)}]");

                    // Correlated Count and Sum per own dept.
                    var counts = (await ctx.Query<Dept>()
                            .Select(d => new { d.Id, N = d.Emps.Count(), S = d.Emps.Sum(e => e.Id) }).ToListAsync())
                        .OrderBy(r => r.Id).Select(r => $"{r.Id}:{r.N}:{r.S}").ToList();
                    var countsExpected = myDepts.Keys.OrderBy(id => id)
                        .Select(id => $"{id}:{myEmps.Values.Count(e => e.DeptId == id)}:{myEmps.Where(kv => kv.Value.DeptId == id).Sum(kv => kv.Key)}")
                        .ToList();
                    Assert.True(countsExpected.SequenceEqual(counts),
                        $"seed={seed} round={round} tenant={t} corrAgg: expected [{string.Join(" ", countsExpected)}] got [{string.Join(" ", counts)}]");

                    // SelectMany: own graph membership only.
                    var flat = (await ctx.Query<Dept>().SelectMany(d => d.Emps).ToListAsync())
                        .Select(e => e.Id).OrderBy(id => id).ToList();
                    var flatExpected = myEmps.Where(kv => myDepts.ContainsKey(kv.Value.DeptId))
                        .Select(kv => kv.Key).OrderBy(id => id).ToList();
                    Assert.True(flatExpected.SequenceEqual(flat),
                        $"seed={seed} round={round} tenant={t} selectMany: expected [{string.Join(",", flatExpected)}] got [{string.Join(",", flat)}]");

                    // Explicit Join and GroupJoin count.
                    var joined = (await ctx.Query<Dept>()
                            .Join(ctx.Query<Emp>(), d => d.Id, e => e.DeptId, (d, e) => new { d.Id, EmpId = e.Id }).ToListAsync())
                        .OrderBy(r => r.EmpId).Select(r => $"{r.Id}:{r.EmpId}").ToList();
                    var joinedExpected = myEmps.Where(kv => myDepts.ContainsKey(kv.Value.DeptId))
                        .OrderBy(kv => kv.Key).Select(kv => $"{kv.Value.DeptId}:{kv.Key}").ToList();
                    Assert.True(joinedExpected.SequenceEqual(joined),
                        $"seed={seed} round={round} tenant={t} join: expected [{string.Join(" ", joinedExpected)}] got [{string.Join(" ", joined)}]");

                    var grouped = (await ctx.Query<Dept>()
                            .GroupJoin(ctx.Query<Emp>(), d => d.Id, e => e.DeptId, (d, es) => new { d.Id, N = es.Count() }).ToListAsync())
                        .OrderBy(r => r.Id).Select(r => $"{r.Id}:{r.N}").ToList();
                    var groupedExpected = myDepts.Keys.OrderBy(id => id)
                        .Select(id => $"{id}:{myEmps.Values.Count(e => e.DeptId == id)}").ToList();
                    Assert.True(groupedExpected.SequenceEqual(grouped),
                        $"seed={seed} round={round} tenant={t} groupJoin: expected [{string.Join(" ", groupedExpected)}] got [{string.Join(" ", grouped)}]");

                    // Predicate-side aggregate: depts with at least one own emp.
                    var withEmps = (await ctx.Query<Dept>().Where(d => d.Emps.Count() > 0).ToListAsync())
                        .Select(d => d.Id).OrderBy(id => id).ToList();
                    var withEmpsExpected = myDepts.Keys
                        .Where(id => myEmps.Values.Any(e => e.DeptId == id)).OrderBy(id => id).ToList();
                    Assert.True(withEmpsExpected.SequenceEqual(withEmps),
                        $"seed={seed} round={round} tenant={t} whereCount: expected [{string.Join(",", withEmpsExpected)}] got [{string.Join(",", withEmps)}]");
                }
            }
        }
        finally
        {
            foreach (var ctx in contexts.Values)
                await ctx.DisposeAsync();
        }
    }
}
