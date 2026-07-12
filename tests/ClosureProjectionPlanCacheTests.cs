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
/// A projected comparison against a CLOSURE capture must use the caller's CURRENT value on every
/// execution. Query plans are cached by expression fingerprint, so a captured value baked into
/// the cached SQL would make a second run with a different value silently compute booleans
/// against the first run's value.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ClosureProjectionPlanCacheTests
{
    [System.ComponentModel.DataAnnotations.Schema.Table("CpcRow")]
    private class Row
    {
        [System.ComponentModel.DataAnnotations.Key,
         System.ComponentModel.DataAnnotations.Schema.DatabaseGenerated(System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int Amount { get; set; }
    }

    private enum Status { Active = 1, Archived = 2 }

    [System.ComponentModel.DataAnnotations.Schema.Table("SccRow")]
    private class ConvRow
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
    public async Task Projected_closure_comparison_uses_current_value_on_each_run()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CpcRow (Id INTEGER PRIMARY KEY AUTOINCREMENT, Amount INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider());
        ctx.Add(new Row { Amount = 5 });
        ctx.Add(new Row { Amount = 10 });
        await ctx.SaveChangesAsync();

        List<bool> Run(int threshold)
        {
            var t = threshold; // closure capture
            return ctx.Query<Row>().OrderBy(r => r.Id)
                .Select(r => new { r.Id, Big = r.Amount > t })
                .ToList().Select(x => x.Big).ToList();
        }

        Assert.Equal(new[] { false, true }, Run(7));   // 5>7 false, 10>7 true
        Assert.Equal(new[] { true, true }, Run(3));    // 5>3 true, 10>3 true — must NOT reuse 7
    }
}
