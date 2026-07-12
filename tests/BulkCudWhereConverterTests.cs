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
/// ExecuteDelete / ExecuteUpdate must convert a WHERE predicate value on a value-converter column,
/// or the operation matches the wrong rows. The data is seeded so a missed conversion is not merely
/// a no-op but deletes/updates the WRONG row: with a negating converter, model Score=10 is stored as
/// -10 and model Score=-10 is stored as 10, so an unconverted `Score == 10` predicate emits
/// `Score = 10` and hits the row whose model value is -10 — silent data loss / corruption.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkCudWhereConverterTests
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

    private static string NameOfStoredScore(SqliteConnection cn, long storedScore)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Name FROM Row WHERE Score = {storedScore}";
        return (string?)cmd.ExecuteScalar() ?? "<none>";
    }

    private static long Count(SqliteConnection cn)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Row";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static async Task Seed(DbContext ctx)
    {
        ctx.Add(new Row { Name = "pos10", Score = 10, Status = Status.Active });   // stored Score=-10
        ctx.Add(new Row { Name = "neg10", Score = -10, Status = Status.Inactive }); // stored Score=10 (collision)
        ctx.Add(new Row { Name = "pos20", Score = 20, Status = Status.Archived });  // stored Score=-20
        await ctx.SaveChangesAsync();
    }

    [Fact]
    public async Task ExecuteDelete_where_numeric_converter_targets_correct_row()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var deleted = await ctx.Query<Row>().Where(r => r.Score == 10).ExecuteDeleteAsync();

        Assert.Equal(1, deleted);
        Assert.Equal(2, Count(cn));
        // The row whose MODEL score is 10 (stored -10, name "pos10") must be gone.
        Assert.Equal("<none>", NameOfStoredScore(cn, -10));
        // The collision row (model -10, stored 10, name "neg10") must remain untouched.
        Assert.Equal("neg10", NameOfStoredScore(cn, 10));
    }

    [Fact]
    public async Task ExecuteDelete_where_enum_converter_targets_correct_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var deleted = await ctx.Query<Row>().Where(r => r.Status == Status.Inactive).ExecuteDeleteAsync();

        Assert.Equal(1, deleted);
        Assert.Equal(2, Count(cn));
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM Row WHERE Status = 'Inactive'";
        Assert.Equal(0L, Convert.ToInt64(cmd.ExecuteScalar()));
    }

    [Fact]
    public async Task ExecuteUpdate_where_numeric_converter_targets_correct_row()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        // Rename only the row whose MODEL score is 10 (stored -10).
        var updated = await ctx.Query<Row>().Where(r => r.Score == 10)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Name, "renamed"));

        Assert.Equal(1, updated);
        Assert.Equal("renamed", NameOfStoredScore(cn, -10)); // the model-10 row got renamed
        Assert.Equal("neg10", NameOfStoredScore(cn, 10));    // collision row untouched
    }
}
