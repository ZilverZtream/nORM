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
/// Filtering on a column with a value converter must bind the converted (provider) value in the
/// WHERE clause. The classic real-world case is an enum stored as its string name: a converter maps
/// Status.Active to "Active", so `Where(p => p.Status == Status.Active)` must emit `Status = 'Active'`
/// — binding the raw enum (its underlying int) silently matches nothing. Covers inline enum literals
/// (single and multi-predicate) and a captured non-enum converter value through the fast path.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ValueConverterPredicateMatrixTests
{
    private enum Status { Active = 1, Inactive = 2, Archived = 3 }

    private class Row
    {
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)] public int Id { get; set; }
        public string Name { get; set; } = "";
        public Status Status { get; set; }
        public int Score { get; set; }
    }

    private sealed class EnumToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v);
    }

    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    private static DbContext Create(SqliteConnection cn)
    {
        using (var cmd = cn.CreateCommand())
        {
            // Status stored as TEXT (converter's provider representation); Score as INTEGER (negated).
            cmd.CommandText = "CREATE TABLE Row (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Status TEXT NOT NULL, Score INTEGER NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Row>().Property<Status>(p => p.Status).HasConversion(new EnumToNameConverter());
                mb.Entity<Row>().Property<int>(p => p.Score).HasConversion(new NegatingConverter());
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private static async Task Seed(DbContext ctx)
    {
        ctx.Add(new Row { Name = "a", Status = Status.Active, Score = 10 });
        ctx.Add(new Row { Name = "b", Status = Status.Inactive, Score = 20 });
        ctx.Add(new Row { Name = "c", Status = Status.Active, Score = 30 });
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task Converter_stores_provider_representation()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT Status, Score FROM Row WHERE Name = 'a'";
        using var r = cmd.ExecuteReader();
        Assert.True(r.Read());
        Assert.Equal("Active", r.GetString(0));  // enum -> name
        Assert.Equal(-10L, r.GetInt64(1));        // score negated
    }

    [Fact]
    public async Task Enum_inline_literal_single_predicate()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var names = (await ctx.Query<Row>().Where(p => p.Status == Status.Active).ToListAsync())
            .Select(r => r.Name).OrderBy(n => n).ToArray();
        Assert.Equal(new[] { "a", "c" }, names);
    }

    [Fact]
    public async Task Enum_inline_literal_multi_predicate()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        // Multi-predicate (AndAlso) bypasses the fast path -> full translator.
        var names = (await ctx.Query<Row>().Where(p => p.Status == Status.Active && p.Id > 0).ToListAsync())
            .Select(r => r.Name).OrderBy(n => n).ToArray();
        Assert.Equal(new[] { "a", "c" }, names);
    }

    [Fact]
    public async Task Enum_inline_literal_not_equal()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var names = (await ctx.Query<Row>().Where(p => p.Status != Status.Active && p.Id > 0).ToListAsync())
            .Select(r => r.Name).ToArray();
        Assert.Equal(new[] { "b" }, names);
    }

    [Fact]
    public async Task NonEnum_converter_captured_variable_fast_path()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        // Score is int->int: `p.Score == var` stays a plain member equality (no enum int-lift), so
        // it hits the single-predicate fast path, which extracts and converts the captured value.
        var wanted = 20;
        var names = (await ctx.Query<Row>().Where(p => p.Score == wanted).ToListAsync())
            .Select(r => r.Name).ToArray();
        Assert.Equal(new[] { "b" }, names);
    }
}
