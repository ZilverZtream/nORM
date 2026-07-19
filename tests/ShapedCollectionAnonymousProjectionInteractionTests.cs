using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Interaction contracts for anonymous-type shaped collection projections
/// (<c>Select(o =&gt; new { o.Id, Lines = o.Lines.Select(..).ToList() })</c>): the split-query child load
/// must honour the same tenant isolation, global (soft-delete) filters, and value converters as any other
/// eager/child load. Tenant × new-collection-load is a repeated critical-bug pattern, so the child fetch must
/// never leak another tenant's rows.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ShapedCollectionAnonymousProjectionInteractionTests
{
    [Table("AipOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public int TenantId { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    [Table("AipLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public int TenantId { get; set; }
        public string Sku { get; set; } = "";
        public int Score { get; set; }
        public bool IsDeleted { get; set; }
    }

    private sealed class FixedTenant(int t) : ITenantProvider { public object GetCurrentTenantId() => t; }

    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    [Fact]
    public void child_load_does_not_leak_across_tenants()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE AipOrder (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL);
                CREATE TABLE AipLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, TenantId INTEGER NOT NULL, Sku TEXT NOT NULL, Score INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO AipOrder VALUES (1,1),(2,2);
                -- Order 1 (tenant 1) has a tenant-1 line AND a forged tenant-2 line pointing at it.
                INSERT INTO AipLine VALUES (1,1,1,'mine',0,0),(2,1,2,'FORGED',0,0);
                """;
            cmd.ExecuteNonQuery();
        }
        DbContext CtxFor(int t) => new(cn, new SqliteProvider(), new DbContextOptions
        {
            TenantColumnName = "TenantId",
            TenantProvider = new FixedTenant(t),
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasKey(o => o.Id);
                mb.Entity<Line>().HasKey(l => l.Id);
                mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
            }
        }, ownsConnection: false);

        using var ctx = CtxFor(1);
        var rows = ctx.Query<Order>().Select(o => new { o.Id, Skus = o.Lines.Select(l => l.Sku).ToList() }).ToList();
        Assert.Single(rows);                          // tenant 1 sees order 1 only
        Assert.Equal(new[] { "mine" }, rows[0].Skus); // the forged tenant-2 line is excluded
    }

    [Fact]
    public void child_load_applies_soft_delete_global_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE AipOrder (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL);
                CREATE TABLE AipLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, TenantId INTEGER NOT NULL, Sku TEXT NOT NULL, Score INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO AipOrder VALUES (1,0);
                INSERT INTO AipLine VALUES (1,1,0,'live',5,0),(2,1,0,'dead',6,1);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Line>(l => !l.IsDeleted);
        opts.OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        var rows = ctx.Query<Order>().Select(o => new { o.Id, Skus = o.Lines.Select(l => l.Sku).ToList() }).ToList();
        Assert.Equal(new[] { "live" }, rows[0].Skus);   // the soft-deleted 'dead' line is excluded
    }

    [Fact]
    public void element_projection_applies_value_converter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE AipOrder (Id INTEGER PRIMARY KEY, TenantId INTEGER NOT NULL);
                CREATE TABLE AipLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, TenantId INTEGER NOT NULL, Sku TEXT NOT NULL, Score INTEGER NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO AipOrder VALUES (1,0);
                INSERT INTO AipLine VALUES (1,1,0,'a',1005,0);
                """;   // Score stored as 1005 = model 5 through the converter
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Line>().Property<int>(l => l.Score).HasConversion(new OffsetConverter());
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
        }};
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
        var rows = ctx.Query<Order>().Select(o => new { o.Id, Scores = o.Lines.Select(l => l.Score).ToList() }).ToList();
        Assert.Equal(new[] { 5 }, rows[0].Scores);   // stored 1005 → model 5 (ConvertFromProvider applied)
    }
}
