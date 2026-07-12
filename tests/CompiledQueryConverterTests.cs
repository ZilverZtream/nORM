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
/// A compiled query whose parameter is compared to a value-converter column must bind the converted
/// value, exactly like an interpreted query. The compiled-query parameter is a free
/// ParameterExpression (not a closure), so it needs the converter recorded for its compiled slot.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class CompiledQueryConverterTests
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

    // Compile once per AppDomain (static) to avoid the compiled-query concurrency semaphore trap.
    private static readonly Func<DbContext, Status, Task<List<Row>>> _byStatus =
        Norm.CompileQuery((DbContext ctx, Status s) => ctx.Query<Row>().Where(p => p.Status == s));

    private static readonly Func<DbContext, int, Task<List<Row>>> _byScore =
        Norm.CompileQuery((DbContext ctx, int sc) => ctx.Query<Row>().Where(p => p.Score == sc));

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
    public async Task Compiled_query_enum_parameter_converts()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var names = (await _byStatus(ctx, Status.Inactive)).Select(r => r.Name).ToArray();
        Assert.Equal(new[] { "b" }, names);
    }

    [Fact]
    public async Task Compiled_query_numeric_parameter_converts()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var ctx = Create(cn);
        await Seed(ctx);

        var names = (await _byScore(ctx, 20)).Select(r => r.Name).ToArray();
        Assert.Equal(new[] { "b" }, names);
    }
}
