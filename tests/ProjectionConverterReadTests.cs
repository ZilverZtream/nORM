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
/// Projecting a value-converter column (Select(p => p.Status) or into an anonymous type) must apply
/// ConvertFromProvider, returning the model value — not the raw stored representation. A full-entity
/// read already does this via the materializer's converter path; a scalar/anonymous projection is a
/// separate path (SelectClauseVisitor) and is verified here.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ProjectionConverterReadTests
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
        ctx.Add(new Row { Name = "a", Score = 10, Status = Status.Archived });
        await ctx.SaveChangesAsync();
    }

    private class Dto
    {
        public int Score { get; set; }
        public Status Status { get; set; }
    }

    [Fact]
    public async Task MemberInit_dto_projection_of_converter_columns()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var dtos = await ctx.Query<Row>().Select(p => new Dto { Score = p.Score, Status = p.Status }).ToListAsync();
        Assert.Single(dtos);
        Assert.Equal(10, dtos[0].Score);
        Assert.Equal(Status.Archived, dtos[0].Status);
    }

    [Fact]
    public async Task Scalar_projection_of_numeric_converter_column()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var scores = ctx.Query<Row>().Select(p => p.Score).ToList();
        Assert.Equal(new[] { 10 }, scores);   // stored -10; must read back as model 10
    }

    [Fact]
    public async Task Anonymous_projection_of_converter_columns()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var rows = await ctx.Query<Row>().Select(p => new { p.Id, p.Score, p.Status }).ToListAsync();
        Assert.Single(rows);
        Assert.Equal(10, rows[0].Score);
        Assert.Equal(Status.Archived, rows[0].Status);
    }
}
