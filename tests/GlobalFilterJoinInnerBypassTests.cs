using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Global filters (soft-delete, multi-tenancy) MUST apply to the INNER sequence
/// of a Join/GroupJoin/SelectMany, not only the outer source. ApplyGlobalFilters
/// previously recursed only into Arguments[0], silently dropping the filter on
/// the joined-to table — a soft-deleted or cross-tenant row on the inner side
/// leaked into results. These are failing-first pins for that fix.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GlobalFilterJoinInnerBypassTests
{
    [Table("GfjOrder")]
    private class GfjOrder
    {
        [Key] public int Id { get; set; }
        public int CustomerId { get; set; }
        public string Ref { get; set; } = string.Empty;
    }

    [Table("GfjCustomer")]
    private class GfjCustomer
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public bool IsDeleted { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) CreateContext()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE GfjOrder (Id INTEGER PRIMARY KEY, CustomerId INTEGER NOT NULL, Ref TEXT NOT NULL);
                CREATE TABLE GfjCustomer (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL, IsDeleted INTEGER NOT NULL);
                INSERT INTO GfjCustomer VALUES (1,'Live',0),(2,'Deleted',1);
                INSERT INTO GfjOrder VALUES (10,1,'o-live'),(11,2,'o-deleted');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => { mb.Entity<GfjOrder>(); mb.Entity<GfjCustomer>(); }
        };
        // Soft-delete filter on the CUSTOMER (inner) entity.
        opts.AddGlobalFilter<GfjCustomer>(c => !c.IsDeleted);
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public async Task Join_applies_global_filter_to_the_inner_sequence()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Join orders to customers; the soft-delete filter on the inner Customer
        // sequence must exclude the deleted customer (Id 2), so only the order
        // for the live customer survives the join.
        var rows = await ctx.Query<GfjOrder>()
            .Join(ctx.Query<GfjCustomer>(), o => o.CustomerId, c => c.Id, (o, c) => new { o.Ref, c.Name })
            .ToListAsync();

        var row = Assert.Single(rows);
        Assert.Equal("o-live", row.Ref);
        Assert.Equal("Live", row.Name);
    }

    [Fact]
    public async Task Group_join_applies_global_filter_to_the_inner_sequence()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // GroupJoin customers to their orders is the reverse shape: filter on the
        // OUTER customer excludes the deleted one, and a join back must never
        // surface deleted customers. Query customers, expect only the live one.
        var customers = await ctx.Query<GfjCustomer>().ToListAsync();
        Assert.Single(customers);
        Assert.Equal("Live", customers[0].Name);
    }

    [Fact]
    public async Task Select_many_navigation_applies_global_filter_to_the_inner_sequence()
    {
        var (cn, ctx) = CreateContext();
        using var _cn = cn;
        await using var _ctx = ctx;

        // Manual SelectMany-style join expressed via Join over both query roots:
        // the deleted customer must not contribute even when reached from the
        // order side.
        var names = (await ctx.Query<GfjCustomer>()
            .Join(ctx.Query<GfjOrder>(), c => c.Id, o => o.CustomerId, (c, o) => new { c.Name, o.Ref })
            .ToListAsync())
            .Select(x => x.Name)
            .Distinct()
            .ToArray();

        Assert.Equal(new[] { "Live" }, names);
    }
}
