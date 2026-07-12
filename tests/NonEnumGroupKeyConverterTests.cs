using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Grouping by a value-converter column must materialize g.Key through the converter regardless
/// of the model type. The enum-keyed case is covered elsewhere; these pin the non-enum shapes
/// (an int stored as text), where a missed conversion yields a wrong-typed or wrong-valued key.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class NonEnumGroupKeyConverterTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("NgRow")]
    private class Row
    {
        [System.ComponentModel.DataAnnotations.Key,
         System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int Code { get; set; }
        public int Amount { get; set; }
    }

    // Stores the int as zero-padded text (a divergence-visible non-enum converter).
    private sealed class IntToPaddedTextConverter : ValueConverter<int, string>
    {
        public override object? ConvertToProvider(int v) => v.ToString("D4");
        public override object? ConvertFromProvider(string v) => int.Parse(v);
    }

    [Fact]
    public async Task GroupBy_nonenum_converter_key_materializes_model_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE NgRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Code TEXT NOT NULL, Amount INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().Property<int>(p => p.Code).HasConversion(new IntToPaddedTextConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Row { Code = 7, Amount = 1 });
        ctx.Add(new Row { Code = 7, Amount = 2 });
        ctx.Add(new Row { Code = 12, Amount = 5 });
        await ctx.SaveChangesAsync();

        var groups = ctx.Query<Row>()
            .GroupBy(r => r.Code)
            .Select(g => new { g.Key, Total = g.Sum(x => x.Amount) })
            .ToList().OrderBy(x => x.Key).ToList();

        Assert.Equal(2, groups.Count);
        Assert.Equal(7, groups[0].Key);      // stored '0007' must come back as 7
        Assert.Equal(3, groups[0].Total);
        Assert.Equal(12, groups[1].Key);
        Assert.Equal(5, groups[1].Total);
    }
}
