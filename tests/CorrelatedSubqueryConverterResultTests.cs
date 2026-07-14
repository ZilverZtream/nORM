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
/// Comparing a correlated subquery whose scalar result is a value-converter column against a
/// constant/closure must bind the constant's PROVIDER representation, not the raw model value.
/// Covers First/FirstOrDefault and Min/Max over an enum-stored-as-name column; binding the raw
/// enum int silently matched no rows.
/// </summary>
[Trait("Category", "Fast")]
public class CorrelatedSubqueryConverterResultTests
{
    public enum Status { Active = 1, Inactive = 2, Archived = 3 }

    [Table("CscrParent")]
    public class Parent { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("CscrChild")]
    public class Child { [Key] public int Id { get; set; } public int ParentId { get; set; } public Status Status { get; set; } }

    private sealed class EnumToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>(v);
    }

    private static DbContext Make()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE CscrParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CscrChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Status TEXT NOT NULL);
                INSERT INTO CscrParent VALUES (1,'a'),(2,'b'),(3,'c');
                INSERT INTO CscrChild VALUES (1,1,'Active'),(2,1,'Inactive'),(3,2,'Archived'),(4,2,'Active'),(5,3,'Inactive');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Child>().Property<Status>(p => p.Status).HasConversion(new EnumToNameConverter());
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task First_of_converter_column_equals_inline_enum()
    {
        await using var ctx = Make();
        // First child by Id per parent: p1->Active, p2->Archived, p3->Inactive. Active => {1}.
        var got = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                            .OrderBy(c => c.Id).Select(c => c.Status).First() == Status.Active)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 1 }, got);
    }

    [Fact]
    public async Task Max_of_converter_column_equals_inline_enum()
    {
        await using var ctx = Make();
        // Max provider-string per parent: p1->'Inactive', p2->'Archived', p3->'Inactive'.
        // == Inactive => {1,3}.
        var got = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).Max(c => c.Status) == Status.Inactive)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 1, 3 }, got);
    }

    [Fact]
    public async Task First_of_converter_column_equals_closure_enum_rebinds_across_executions()
    {
        await using var ctx = Make();
        async Task<int[]> Run(Status s) => (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                            .OrderBy(c => c.Id).Select(c => c.Status).First() == s)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();

        // Prime the plan cache with Active, then re-run with a different enum: the closure must
        // re-bind through the converter, not replay the first provider string.
        Assert.Equal(new[] { 1 }, await Run(Status.Active));     // p1 first child Active
        Assert.Equal(new[] { 2 }, await Run(Status.Archived));   // p2 first child Archived
        Assert.Equal(new[] { 3 }, await Run(Status.Inactive));   // p3 first child Inactive
    }

    [Fact]
    public async Task Not_equal_keeps_the_semantics()
    {
        await using var ctx = Make();
        // First child != Active: p2(Archived), p3(Inactive) => {2,3}.
        var got = (await ctx.Query<Parent>()
            .Where(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                            .OrderBy(c => c.Id).Select(c => c.Status).First() != Status.Active)
            .Select(p => new { p.Id }).ToListAsync()).Select(x => x.Id).OrderBy(x => x).ToArray();
        Assert.Equal(new[] { 2, 3 }, got);
    }
}
