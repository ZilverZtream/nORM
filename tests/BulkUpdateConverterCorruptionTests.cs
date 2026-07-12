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
/// ExecuteUpdate's SET clause must write the converted (provider) value for a value-converter
/// column, exactly as the write path does for tracked SaveChanges. Binding the raw model value
/// persists the wrong data — a silent corruption. Uses a negating converter (42 stored as -42) and
/// an enum-as-name converter so a missed conversion is visible in the raw stored bytes.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class BulkUpdateConverterCorruptionTests
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

    private static long RawScore(SqliteConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Score FROM Row WHERE Id = {id}";
        return Convert.ToInt64(cmd.ExecuteScalar());
    }

    private static string RawStatus(SqliteConnection cn, int id)
    {
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"SELECT Status FROM Row WHERE Id = {id}";
        return (string)cmd.ExecuteScalar()!;
    }

    [Fact]
    public async Task ExecuteUpdate_literal_applies_numeric_converter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        var row = new Row { Name = "a", Score = 10, Status = Status.Active };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        await ctx.Query<Row>().Where(r => r.Id == row.Id)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Score, 42));

        Assert.Equal(-42, RawScore(cn, row.Id));                     // BUG if 42: raw value persisted
    }

    [Fact]
    public async Task ExecuteUpdate_enum_literal_applies_string_converter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        var row = new Row { Name = "a", Score = 10, Status = Status.Active };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        await ctx.Query<Row>().Where(r => r.Id == row.Id)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Status, Status.Archived));

        Assert.Equal("Archived", RawStatus(cn, row.Id));            // BUG if "3" or int: raw enum persisted
    }

    [Fact]
    public async Task ExecuteUpdate_captured_value_applies_converter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        var row = new Row { Name = "a", Score = 10, Status = Status.Active };
        ctx.Add(row);
        await ctx.SaveChangesAsync();

        var newScore = 7;   // captured value form
        await ctx.Query<Row>().Where(r => r.Id == row.Id)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Score, newScore));

        Assert.Equal(-7, RawScore(cn, row.Id));
    }
}
