using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A shaped-collection element projection that dereferences the child element's OWN navigation
/// (a nested collection <c>i.Tags</c> or a reference <c>i.Category.Name</c>) is admitted by the
/// projection-safety gate because the access roots in the element parameter — but the child is
/// materialized bare (its navigations are never loaded), so the nested collection comes back empty
/// (silent data loss) and a reference deref throws deep inside execution. Such a projection must not
/// silently drop data: it must load the navigation or fail loud with a clear, actionable message.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class CollectionProjectionChildNavDerefTests
{
    [Table("CpcItem")]
    public class Item
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("CpcTag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
        public int ItemId { get; set; }
        public string Label { get; set; } = "";
    }

    [Table("CpcOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Item> Items { get; set; } = new();
    }

    private static (SqliteConnection, DbContext) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE CpcOrder (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE CpcItem (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL);" +
                "CREATE TABLE CpcTag (Id INTEGER PRIMARY KEY, ItemId INTEGER NOT NULL, Label TEXT NOT NULL);" +
                "INSERT INTO CpcOrder VALUES (1);" +
                "INSERT INTO CpcItem VALUES (10, 1, 'sku-a'), (11, 1, 'sku-b');" +
                "INSERT INTO CpcTag VALUES (100, 10, 'red'), (101, 10, 'blue'), (102, 11, 'green');";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Order>().HasMany(o => o.Items).WithOne().HasForeignKey(i => i.OrderId, o => o.Id);
                mb.Entity<Item>().HasMany(i => i.Tags).WithOne().HasForeignKey(t => t.ItemId, i => i.Id);
            }
        };
        return (cn, new DbContext(cn, new SqliteProvider(), opts));
    }

    [Fact]
    public void Collection_projection_dereferencing_child_collection_nav_fails_loud_not_silent()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // The bug silently returned empty Tags for every line. That is unacceptable data loss — the
        // projection must fail loud with a clear, actionable message instead.
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Order>()
                .OrderBy(o => o.Id)
                .Select(o => new { o.Id, Lines = o.Items.Select(i => new { i.Sku, i.Tags }).ToList() })
                .ToList());
    }

    [Fact]
    public void Collection_projection_of_child_scalar_columns_still_works()
    {
        var (cn, ctx) = Create();
        using var _cn = cn; using var _ctx = ctx;

        // Scalar-only child projections are the supported surface and must NOT be over-rejected.
        var rows = ctx.Query<Order>()
            .OrderBy(o => o.Id)
            .Select(o => new { o.Id, Lines = o.Items.Select(i => new { i.Sku }).ToList() })
            .ToList();

        Assert.Equal(new[] { "sku-a", "sku-b" },
            rows.Single().Lines.Select(l => l.Sku).OrderBy(s => s).ToArray());
    }
}
