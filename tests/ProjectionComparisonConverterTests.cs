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
/// Projecting a comparison against a value-converter column (Select(p => p.Status == Status.Active))
/// must bind the converted value, or the projected boolean is wrong for every row. This goes through
/// SelectClauseVisitor, a different path from the WHERE translator.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ProjectionComparisonConverterTests
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
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task Projected_equality_against_converter_column_is_correct()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var rows = await ctx.Query<Row>()
            .OrderBy(p => p.Name)
            .Select(p => new { p.Name, IsActive = p.Status == Status.Active })
            .ToListAsync();

        Assert.Equal(2, rows.Count);
        Assert.True(rows[0].IsActive);    // 'a' is Active
        Assert.False(rows[1].IsActive);   // 'b' is Inactive
    }
}
