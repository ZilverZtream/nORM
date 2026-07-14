using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
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
/// Grouping by a value-converter column must expose the MODEL value as g.Key (ConvertFromProvider
/// applied), not the raw stored representation. With an enum-as-name converter, g.Key must be the
/// enum, not the string — otherwise the projection returns the wrong type or crashes.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class GroupByConverterKeyTests
{
    private enum Status { Active = 1, Inactive = 2, Archived = 3 }

    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public Status Status { get; set; }
    }

    private sealed class EnumToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v);
    }

    private static DbContext Create(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Row (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Status TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Row>().Property<Status>(p => p.Status).HasConversion(new EnumToNameConverter())
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static async Task Seed(DbContext ctx)
    {
        ctx.Add(new Row { Name = "a", Status = Status.Active });
        ctx.Add(new Row { Name = "b", Status = Status.Inactive });
        ctx.Add(new Row { Name = "c", Status = Status.Active });
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task GroupBy_converter_column_key_is_model_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var groups = await ctx.Query<Row>()
            .GroupBy(p => p.Status)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToListAsync();

        var byStatus = groups.ToDictionary(x => x.Key, x => x.Count);
        Assert.Equal(2, byStatus[Status.Active]);     // BUG if Key is the string "Active" (KeyNotFound)
        Assert.Equal(1, byStatus[Status.Inactive]);
    }

    [Fact]
    public async Task GroupBy_converter_key_with_predicate_binds_provider_value_and_aggregates()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        ctx.Add(new Row { Name = "a", Status = Status.Active });
        ctx.Add(new Row { Name = "b", Status = Status.Inactive });
        ctx.Add(new Row { Name = "c", Status = Status.Active });
        ctx.Add(new Row { Name = "d", Status = Status.Archived });
        await ctx.SaveChangesAsync();

        // WHERE on the converter column before grouping must bind the stored
        // string, and the group's aggregate must reflect only the matched rows.
        var status = Status.Active;
        var groups = await ctx.Query<Row>()
            .Where(p => p.Status == status)
            .GroupBy(p => p.Status)
            .Select(g => new { g.Key, Count = g.Count() })
            .ToListAsync();

        Assert.Single(groups);
        Assert.Equal(Status.Active, groups[0].Key);
        Assert.Equal(2, groups[0].Count);
    }

    [Fact]
    public async Task Distinct_converter_column_materializes_model_values()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        ctx.Add(new Row { Name = "a", Status = Status.Active });
        ctx.Add(new Row { Name = "b", Status = Status.Inactive });
        ctx.Add(new Row { Name = "c", Status = Status.Active });
        ctx.Add(new Row { Name = "d", Status = Status.Archived });
        await ctx.SaveChangesAsync();

        var distinct = (await ctx.Query<Row>().Select(p => new { p.Status }).Distinct().ToListAsync())
            .Select(x => x.Status).OrderBy(s => s).ToList();

        Assert.Equal(new[] { Status.Active, Status.Inactive, Status.Archived }.OrderBy(s => s).ToList(), distinct);
    }
}
