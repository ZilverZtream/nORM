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
/// Pins tenant isolation through the AsOf history windows of JOIN-based shapes:
/// navigation projections, navigation predicates, SelectMany, correlated
/// aggregates, Include, and set-operation arms must apply the tenant predicate
/// against the WINDOWED rows — era-consistent AND tenant-scoped. The seed is
/// deliberately adversarial: the other tenant's employee references THIS
/// tenant's department key, so a dropped tenant filter in any windowed child
/// read surfaces as a membership leak, not just a value difference.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class TenantJoinAsOfConsistencyContractTests
{
    [Table("TjaDept")]
    public class Dept
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public string Title { get; set; } = "";
        public List<Emp> Emps { get; set; } = new();
    }

    [Table("TjaEmp")]
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

    private static async Task ExecAsync(SqliteConnection cn, string sql)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<DateTime> ServerNowAsync(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
        var text = (string)(await cmd.ExecuteScalarAsync())!;
        return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
    }

    [Fact]
    public async Task Tenant_predicate_holds_through_windowed_join_shapes()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await ExecAsync(cn, """
            CREATE TABLE TjaDept (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Title TEXT NOT NULL);
            CREATE TABLE TjaEmp (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL, DeptId INTEGER NOT NULL);
            """);

        var opts = new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenantProvider(10),
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Dept>().HasMany(d => d.Emps).WithOne(e => e.Dept!).HasForeignKey(e => e.DeptId, d => d.Id);
            }
        };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        // Era 1 for tenant 10: dept "da" with one emp "ea".
        var dept = new Dept { Id = 1, TenantId = 10, Title = "da" };
        var e1 = new Emp { Id = 1, TenantId = 10, Name = "ea", DeptId = 1 };
        ctx.Add(dept);
        ctx.Add(e1);
        await ctx.SaveChangesAsync();

        // Tenant 20's rows land via raw SQL AFTER bootstrap so the temporal
        // triggers record them: its emp deliberately references TENANT 10's
        // department key — a dropped tenant filter in any windowed child read
        // makes this row appear in tenant 10's results.
        await ExecAsync(cn, """
            INSERT INTO TjaDept VALUES (100, 20, 'db');
            INSERT INTO TjaEmp VALUES (100, 20, 'eb', 1);
            """);

        await Task.Delay(60);
        var t1 = await ServerNowAsync(cn);
        await Task.Delay(60);

        // Era 2: both tenants' rows change after t1.
        dept.Title = "dax";
        e1.Name = "eax";
        await ctx.SaveChangesAsync();
        await ExecAsync(cn, """
            UPDATE TjaDept SET Title = 'dbx' WHERE Id = 100;
            UPDATE TjaEmp SET Name = 'ebx' WHERE Id = 100;
            """);

        // Navigation projection: era values, own tenant only.
        var navProj = await ctx.Query<Emp>().AsOf(t1)
            .Select(e => new { e.Name, DeptTitle = e.Dept!.Title }).ToListAsync();
        Assert.Equal(new[] { "ea|da" },
            navProj.OrderBy(r => r.Name).Select(r => $"{r.Name}|{r.DeptTitle}").ToArray());

        // Navigation predicate: the era title matches; the other tenant's title never does.
        Assert.Equal(new[] { "ea" },
            (await ctx.Query<Emp>().AsOf(t1).Where(e => e.Dept!.Title == "da").ToListAsync())
                .Select(e => e.Name).ToArray());
        Assert.Empty(await ctx.Query<Emp>().AsOf(t1).Where(e => e.Dept!.Title == "db").ToListAsync());

        // SelectMany: era membership of the tenant's own graph — the foreign
        // tenant's emp pointing at dept 1 must not flatten in.
        var flat = await ctx.Query<Dept>().AsOf(t1).SelectMany(d => d.Emps).ToListAsync();
        Assert.Equal(new[] { "ea" }, flat.Select(e => e.Name).ToArray());

        // Correlated aggregate: counts only the tenant's own era emps.
        var counts = await ctx.Query<Dept>().AsOf(t1)
            .Select(d => new { d.Id, N = d.Emps.Count() }).ToListAsync();
        Assert.Equal(1, counts.Single().N);

        // Include: loaded children are the tenant's own era rows.
        var included = await ((INormQueryable<Dept>)ctx.Query<Dept>())
            .Include(d => d.Emps).AsOf(t1).ToListAsync();
        var only = Assert.Single(included);
        Assert.Equal("da", only.Title);
        Assert.Equal(new[] { "ea" }, only.Emps.Select(e => e.Name).ToArray());

        // Set operation: both windowed arms stay tenant-scoped.
        var union = await ctx.Query<Emp>().AsOf(t1).Where(e => e.Id % 2 == 0)
            .Union(ctx.Query<Emp>().Where(e => e.Id % 2 == 1)).ToListAsync();
        Assert.Equal(new[] { "ea" }, union.Select(e => e.Name).ToArray());

        // Live sanity: the same shapes without AsOf read live values, still tenant-scoped.
        var liveProj = await ctx.Query<Emp>()
            .Select(e => new { e.Name, DeptTitle = e.Dept!.Title }).ToListAsync();
        Assert.Equal(new[] { "eax|dax" },
            liveProj.OrderBy(r => r.Name).Select(r => $"{r.Name}|{r.DeptTitle}").ToArray());
        var liveCounts = await ctx.Query<Dept>()
            .Select(d => new { d.Id, N = d.Emps.Count() }).ToListAsync();
        Assert.Equal(1, liveCounts.Single().N);
    }

    [Fact]
    public async Task Tenant_predicate_holds_through_live_nav_aggregates_and_forged_fk()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await ExecAsync(cn, """
            CREATE TABLE TjaDept (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Title TEXT NOT NULL);
            CREATE TABLE TjaEmp (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL, DeptId INTEGER NOT NULL);
            INSERT INTO TjaDept VALUES (1, 10, 'da');
            INSERT INTO TjaEmp VALUES (1, 10, 'ea', 1);
            INSERT INTO TjaDept VALUES (100, 20, 'db');
            INSERT INTO TjaEmp VALUES (100, 20, 'eb', 1);
            """);

        static DbContextOptions OptionsFor(int tenant) => new()
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

        await using (var ctxA = new DbContext(cn, new SqliteProvider(), OptionsFor(10), ownsConnection: false))
        {
            // Correlated count in a projection counts only tenant A's dependents.
            var counts = await ctxA.Query<Dept>()
                .Select(d => new { d.Id, N = d.Emps.Count() }).ToListAsync();
            Assert.Equal(1, counts.Single().N);

            // Navigation Sum in a projection: tenant A's emp Id is 1; the foreign
            // tenant's emp (Id 100) sharing the FK value must not fold in.
            var sums = await ctxA.Query<Dept>()
                .Select(d => new { d.Id, S = d.Emps.Sum(e => e.Id) }).ToListAsync();
            Assert.Equal(1, sums.Single().S);

            // WHERE-side navigation aggregates: tenant A's dept has one emp with
            // Id sum 1, so neither predicate matches unless the foreign row leaks.
            Assert.Empty(await ctxA.Query<Dept>().Where(d => d.Emps.Count() > 1).ToListAsync());
            Assert.Empty(await ctxA.Query<Dept>().Where(d => d.Emps.Sum(e => e.Id) > 1).ToListAsync());
        }

        await using (var ctxB = new DbContext(cn, new SqliteProvider(), OptionsFor(20), ownsConnection: false))
        {
            // The forged FK: tenant B's emp references tenant A's department key.
            // The principal is another tenant's row, so it reads as MISSING —
            // its values must not leak into tenant B's projections or predicates.
            var proj = await ctxB.Query<Emp>()
                .Select(e => new { e.Name, DeptTitle = (string?)e.Dept!.Title }).ToListAsync();
            var row = Assert.Single(proj);
            Assert.Equal("eb", row.Name);
            Assert.Null(row.DeptTitle);

            Assert.Empty(await ctxB.Query<Emp>().Where(e => e.Dept!.Title == "da").ToListAsync());
        }
    }

    [Fact]
    public async Task Tenant_predicate_holds_through_explicit_join_shapes()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        await ExecAsync(cn, """
            CREATE TABLE TjaDept (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Title TEXT NOT NULL);
            CREATE TABLE TjaEmp (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL, Name TEXT NOT NULL, DeptId INTEGER NOT NULL);
            INSERT INTO TjaDept VALUES (1, 10, 'da');
            INSERT INTO TjaEmp VALUES (1, 10, 'ea', 1);
            INSERT INTO TjaDept VALUES (100, 20, 'db');
            INSERT INTO TjaEmp VALUES (100, 20, 'eb', 1);
            """);

        var opts = new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenantProvider(10),
            OnModelCreating = mb =>
            {
                mb.Entity<Dept>().HasKey(d => d.Id);
                mb.Entity<Emp>().HasKey(e => e.Id);
                mb.Entity<Dept>().HasMany(d => d.Emps).WithOne(e => e.Dept!).HasForeignKey(e => e.DeptId, d => d.Id);
            }
        };
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        // Explicit Join: the INNER source is a second mapped query — the foreign
        // tenant's emp shares the join key, so a missing tenant filter on the
        // inner table surfaces as an extra joined row.
        var joined = await ctx.Query<Dept>()
            .Join(ctx.Query<Emp>(), d => d.Id, e => e.DeptId, (d, e) => new { d.Title, e.Name })
            .ToListAsync();
        Assert.Equal(new[] { "da|ea" },
            joined.OrderBy(r => r.Name, StringComparer.Ordinal).Select(r => $"{r.Title}|{r.Name}").ToArray());

        // GroupJoin: the grouped inner rows stay tenant-scoped.
        var grouped = await ctx.Query<Dept>()
            .GroupJoin(ctx.Query<Emp>(), d => d.Id, e => e.DeptId, (d, es) => new { d.Title, N = es.Count() })
            .ToListAsync();
        Assert.Equal(1, grouped.Single().N);

        // GroupJoin + DefaultIfEmpty flatten (left join): same rule.
        var leftJoined = await ctx.Query<Dept>()
            .GroupJoin(ctx.Query<Emp>(), d => d.Id, e => e.DeptId, (d, es) => new { d, es })
            .SelectMany(x => x.es.DefaultIfEmpty(), (x, e) => new { x.d.Title, Name = (string?)e!.Name })
            .ToListAsync();
        Assert.Equal(new[] { "da|ea" },
            leftJoined.OrderBy(r => r.Name, StringComparer.Ordinal).Select(r => $"{r.Title}|{r.Name}").ToArray());

        // Cross-join SelectMany over a second mapped query: only the tenant's
        // own rows may appear on either side.
        var cross = await ctx.Query<Dept>()
            .SelectMany(d => ctx.Query<Emp>(), (d, e) => new { d.Title, e.Name })
            .ToListAsync();
        Assert.Equal(new[] { "da|ea" },
            cross.OrderBy(r => r.Name, StringComparer.Ordinal).Select(r => $"{r.Title}|{r.Name}").ToArray());
    }
}
