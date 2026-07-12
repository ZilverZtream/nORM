using System;
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
/// A captured (closure) value compared to a value-converter column in the FULL translator must bind
/// the converted value. Enum comparisons int-lift, so `Where(p => p.Status == captured)` fails the
/// fast-path member-equality guard and lands in the full translator; multi-predicate comparisons do
/// too. Both must convert the captured value at execution time (the plan is fingerprint-cached
/// across differing captures, so the value cannot be baked at translation).
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ClosureConverterPredicateTests
{
    private enum Status { Active = 1, Inactive = 2, Archived = 3 }

    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public int Score { get; set; }
        public Status Status { get; set; }
    }

    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
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
            cmd.CommandText = "CREATE TABLE Row (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Score INTEGER NOT NULL, Status TEXT NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Row>().Property<int>(p => p.Score).HasConversion(new NegatingConverter());
                mb.Entity<Row>().Property<Status>(p => p.Status).HasConversion(new EnumToNameConverter());
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static async Task Seed(DbContext ctx)
    {
        ctx.Add(new Row { Name = "a", Score = 10, Status = Status.Active });
        ctx.Add(new Row { Name = "b", Score = 20, Status = Status.Inactive });
        ctx.Add(new Row { Name = "c", Score = 30, Status = Status.Active });
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task Enum_captured_single_predicate()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var wanted = Status.Inactive;
        var names = (await ctx.Query<Row>().Where(p => p.Status == wanted).ToListAsync())
            .Select(r => r.Name).ToArray();
        Assert.Equal(new[] { "b" }, names);
    }

    [Fact]
    public async Task Enum_captured_multi_predicate()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var wanted = Status.Active;
        var names = (await ctx.Query<Row>().Where(p => p.Status == wanted && p.Id > 0).ToListAsync())
            .Select(r => r.Name).OrderBy(n => n).ToArray();
        Assert.Equal(new[] { "a", "c" }, names);
    }

    [Fact]
    public async Task Numeric_captured_multi_predicate()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var wanted = 20;   // multi-predicate -> full translator (single-predicate int would hit fast path)
        var names = (await ctx.Query<Row>().Where(p => p.Score == wanted && p.Id > 0).ToListAsync())
            .Select(r => r.Name).ToArray();
        Assert.Equal(new[] { "b" }, names);
    }
}
