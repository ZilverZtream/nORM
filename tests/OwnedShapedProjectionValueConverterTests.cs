using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A value converter on an owned (OwnsMany) column must be applied when the owned collection is loaded through
/// a SHAPED PROJECTION: the hand-rolled owned materializer runs <c>Converter.ConvertFromProvider</c> per column,
/// so the projected owned rows carry model values, not the raw stored provider values. This owned-converter
/// branch of the projection loader is distinct from the relation path's converter handling.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedShapedProjectionValueConverterTests
{
    [Table("OcvOrder")]
    public class Order
    {
        [Key] public int Id { get; set; }
        public List<Line> Lines { get; set; } = new();
    }

    public class Line { [Key] public int Id { get; set; } public int Score { get; set; } }

    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int value) => value + 1000;   // model N stored as N+1000
        public override object? ConvertFromProvider(int value) => value - 1000;
    }

    [Fact]
    public void Owned_shaped_projection_applies_the_column_value_converter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OcvOrder (Id INTEGER PRIMARY KEY);
                CREATE TABLE OcvLine (Id INTEGER PRIMARY KEY, OrderId INTEGER NOT NULL, Score INTEGER NOT NULL);
                INSERT INTO OcvOrder VALUES (1);
                INSERT INTO OcvLine VALUES (1,1,1005),(2,1,1050);
                """;   // Scores stored as 1005 / 1050 = model 5 / 50 through the converter
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions { OnModelCreating = mb =>
        {
            mb.Entity<Order>().HasKey(o => o.Id);
            mb.Entity<Order>().OwnsMany<Line>(o => o.Lines, tableName: "OcvLine", foreignKey: "OrderId",
                buildAction: b => b.Property<int>(l => l.Score).HasConversion(new OffsetConverter()));
        }};
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var rows = ctx.Query<Order>().Select(o => new { o.Id, Lines = o.Lines.ToList() }).ToList();
        Assert.Equal(new[] { 5, 50 }, rows.Single().Lines.Select(l => l.Score).OrderBy(s => s));   // raw 1005/1050 → 5/50
    }
}
