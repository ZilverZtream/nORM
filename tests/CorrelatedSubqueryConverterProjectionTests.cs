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
/// Projecting a correlated First/Last/Min/Max over a value-converter column must apply the
/// column's ConvertFromProvider on materialization. Previously the shadow projection column
/// carried no converter, so the raw provider value (an enum stored as its name) was blindly
/// cast and threw a FormatException.
/// </summary>
[Trait("Category", "Fast")]
public class CorrelatedSubqueryConverterProjectionTests
{
    public enum Status { Active = 1, Inactive = 2, Archived = 3 }

    [Table("CscpParent")]
    public class Parent { [Key] public int Id { get; set; } public string Name { get; set; } = ""; }

    [Table("CscpChild")]
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
                CREATE TABLE CscpParent (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE CscpChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Status TEXT NOT NULL);
                INSERT INTO CscpParent VALUES (1,'a'),(2,'b'),(3,'c');
                INSERT INTO CscpChild VALUES (1,1,'Active'),(2,1,'Inactive'),(3,2,'Archived'),(4,2,'Active'),(5,3,'Inactive');
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
    public async Task First_of_converter_column_projects_as_clr_enum()
    {
        await using var ctx = Make();
        // First child (by Id) Status per parent: p1->Active, p2->Archived, p3->Inactive.
        var got = (await ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => new
            {
                p.Id,
                First = ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                            .OrderBy(c => c.Id).Select(c => c.Status).First()
            })
            .ToListAsync());
        Assert.Equal(Status.Active, got.Single(x => x.Id == 1).First);
        Assert.Equal(Status.Archived, got.Single(x => x.Id == 2).First);
        Assert.Equal(Status.Inactive, got.Single(x => x.Id == 3).First);
    }

    [Fact]
    public async Task Max_of_converter_column_projects_as_clr_enum()
    {
        await using var ctx = Make();
        // Max provider-string per parent: p1->'Inactive', p2->'Archived', p3->'Inactive'.
        var got = (await ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => new
            {
                p.Id,
                Mx = ctx.Query<Child>().Where(c => c.ParentId == p.Id).Max(c => c.Status)
            })
            .ToListAsync());
        Assert.Equal(Status.Inactive, got.Single(x => x.Id == 1).Mx);
        Assert.Equal(Status.Archived, got.Single(x => x.Id == 2).Mx);
        Assert.Equal(Status.Inactive, got.Single(x => x.Id == 3).Mx);
    }

    [Fact]
    public async Task Last_of_converter_column_projects_as_clr_enum()
    {
        await using var ctx = Make();
        // Last child (by Id) Status per parent: p1->Inactive(id2), p2->Active(id4), p3->Inactive(id5).
        var got = (await ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => new
            {
                p.Id,
                L = ctx.Query<Child>().Where(c => c.ParentId == p.Id)
                        .OrderBy(c => c.Id).Select(c => c.Status).Last()
            })
            .ToListAsync());
        Assert.Equal(Status.Inactive, got.Single(x => x.Id == 1).L);
        Assert.Equal(Status.Active, got.Single(x => x.Id == 2).L);
        Assert.Equal(Status.Inactive, got.Single(x => x.Id == 3).L);
    }
}
