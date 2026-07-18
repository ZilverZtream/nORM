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
/// Pins that a shaped collection projection binding may name its DTO property DIFFERENTLY from the entity
/// navigation it comes from — <c>new Dto { LineItems = o.Lines.Select(...).ToList() }</c> — for both the
/// Stage 1 (whole-entity) and Stage 2 (element-projected) forms. The split-query stitch must target the
/// DTO binding member, not the navigation name.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ShapedCollectionRenamedPropertyContractTests
{
    [Table("ScrOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    [Table("ScrLine")]
    public class Line
    {
        [Key] public int Id { get; set; }
        public int OrderId { get; set; }
        public string Sku { get; set; } = "";
    }

    public class LineDto { public string Sku { get; set; } = ""; }
    // Note: the collection property is named "LineItems", NOT matching the nav "Lines".
    public class OrderProjectedDto { public int Id { get; set; } public List<LineDto> LineItems { get; set; } = new(); }
    public class OrderEntityDto { public int Id { get; set; } public List<Line> LineItems { get; set; } = new(); }

    private static DbContext Bootstrap(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE ScrOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE ScrLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Sku TEXT NOT NULL);
                INSERT INTO ScrOrder VALUES (1);
                INSERT INTO ScrLine VALUES (1,1,'a'),(2,1,'b');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Line>().HasKey(l => l.Id);
            mb.Entity<Order>().HasMany(o => o.Lines).WithOne().HasForeignKey(l => l.OrderId, o => o.Id);
        }};
        return new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);
    }

    [Fact]
    public async Task Element_projected_collection_binds_to_a_renamed_dto_property()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        var dto = (await ctx.Query<Order>()
            .Select(o => new OrderProjectedDto { Id = o.Id, LineItems = o.Lines.Select(l => new LineDto { Sku = l.Sku }).ToList() })
            .ToListAsync()).Single();

        Assert.Equal(new[] { "a", "b" }, dto.LineItems.OrderBy(l => l.Sku).Select(l => l.Sku).ToArray());
    }

    [Fact]
    public async Task Whole_entity_collection_binds_to_a_renamed_dto_property()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open(); await using var ctx = Bootstrap(cn);

        var dto = (await ctx.Query<Order>()
            .Select(o => new OrderEntityDto { Id = o.Id, LineItems = o.Lines.ToList() })
            .ToListAsync()).Single();

        Assert.Equal(new[] { "a", "b" }, dto.LineItems.OrderBy(l => l.Sku).Select(l => l.Sku).ToArray());
    }
}
