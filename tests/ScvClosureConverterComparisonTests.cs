using System;
using System.Collections.Generic;
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
/// A projected comparison against a value-converter column must bind the converter's PROVIDER
/// value even when the compared value is a CLOSURE capture — and must re-bind the LIVE value on
/// every run (plans are cached by fingerprint).
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ScvClosureConverterComparisonTests
{
    private enum Status { Active = 1, Archived = 2 }

    [System.ComponentModel.DataAnnotations.Schema.Table("SccRow")]
    private class Row
    {
        [System.ComponentModel.DataAnnotations.Key,
         System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public Status State { get; set; }
    }

    private sealed class EnumToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v);
    }

    [Fact]
    public async Task Projected_comparison_with_closure_value_converts_and_rebinds()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SccRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, State TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().Property<Status>(p => p.State).HasConversion(new EnumToNameConverter())
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts);
        ctx.Add(new Row { State = Status.Active });
        ctx.Add(new Row { State = Status.Archived });
        await ctx.SaveChangesAsync();

        // Batched inserts need not preserve Add order; read the actual per-Id states and
        // compute the LINQ-expected booleans from them so the assertions are order-robust.
        var actualRows = ctx.Query<Row>().OrderBy(r => r.Id).ToList();
        Assert.Equal(2, actualRows.Count);

        List<bool> Run(Status wanted)
        {
            var w = wanted; // closure capture
            return ctx.Query<Row>().OrderBy(r => r.Id)
                .Select(r => new { r.Id, Hit = r.State == w })
                .ToList().Select(x => x.Hit).ToList();
        }

        Assert.Equal(actualRows.Select(r => r.State == Status.Active).ToList(), Run(Status.Active));
        Assert.Equal(actualRows.Select(r => r.State == Status.Archived).ToList(), Run(Status.Archived)); // plan-cache rebind
    }
}
