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
/// Projecting a single enum column (Select(p => p.Status)) must return the enum values. Enums are
/// value types stored as an integer (or, with a converter, a name); a scalar projection of one must
/// not fall through to the entity-constructor path.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class ScalarEnumProjectionTests
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

    private static DbContext Create(SqliteConnection cn, bool withConverter)
    {
        using (var cmd = cn.CreateCommand())
        {
            var statusType = withConverter ? "TEXT" : "INTEGER";
            cmd.CommandText = $"CREATE TABLE Row (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL, Status {statusType} NOT NULL)";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        if (withConverter)
            opts.OnModelCreating = mb => mb.Entity<Row>().Property<Status>(p => p.Status).HasConversion(new EnumToNameConverter());
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
    public async Task Plain_enum_scalar_projection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn, withConverter: false);
        await Seed(ctx);

        var statuses = ctx.Query<Row>().OrderBy(p => p.Name).Select(p => p.Status).ToList();
        Assert.Equal(new[] { Status.Active, Status.Inactive, Status.Active }, statuses);
    }

    [Fact]
    public async Task Converter_enum_scalar_projection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn, withConverter: true);
        await Seed(ctx);

        var statuses = ctx.Query<Row>().OrderBy(p => p.Name).Select(p => p.Status).ToList();
        Assert.Equal(new[] { Status.Active, Status.Inactive, Status.Active }, statuses);
    }
}
