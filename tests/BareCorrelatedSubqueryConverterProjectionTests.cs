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
/// A BARE correlated-subquery scalar projection — where the projection body IS the subquery op with no
/// anonymous-type wrapper, e.g. <c>Select(p =&gt; ctx.Query&lt;Child&gt;().Where(...).OrderBy(...).Select(c =&gt; c.Col).First())</c>
/// — over a value-converter column must apply ConvertFromProvider to the scalar result, exactly like the
/// wrapped <c>Select(p =&gt; new { X = ...First() })</c> form already did. Previously the wrapped form registered
/// the column's converter (via a DTO member name) but the bare form had no member name, so the scalar
/// materializer projected the RAW stored value: a same-CLR-type converter (int stored negated) returned the
/// wrong number silently, and an enum-stored-as-name converter threw a FormatException casting the string.
/// The bare converter is now registered under a reserved key that the scalar-projection path reads.
/// </summary>
[Trait("Category", "Fast")]
public class BareCorrelatedSubqueryConverterProjectionTests
{
    // --- Same-CLR-type converter (int stored as its negation): a silent-wrong risk, since no type
    //     mismatch forces a conversion at materialization. ---
    private sealed class NegatingConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -(Convert.ToInt32(v));
    }

    [Table("BcscParent")]
    public class Parent { [Key] public int Id { get; set; } }

    [Table("BcscChild")]
    public class Child { [Key] public int Id { get; set; } public int ParentId { get; set; } public int Score { get; set; } }

    private static DbContext MakeInt()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE BcscParent (Id INTEGER PRIMARY KEY);
                CREATE TABLE BcscChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Score INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Parent>().HasKey(p => p.Id);
                mb.Entity<Child>().HasKey(c => c.Id);
                mb.Entity<Child>().Property(c => c.Score).HasConversion(new NegatingConverter());
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Seed through the context so the converter stores the negated value.
        ctx.InsertAsync(new Parent { Id = 1 }).GetAwaiter().GetResult();
        ctx.InsertAsync(new Parent { Id = 2 }).GetAwaiter().GetResult();
        ctx.InsertAsync(new Child { Id = 1, ParentId = 1, Score = 3 }).GetAwaiter().GetResult();
        ctx.InsertAsync(new Child { Id = 2, ParentId = 1, Score = 7 }).GetAwaiter().GetResult();
        ctx.InsertAsync(new Child { Id = 3, ParentId = 2, Score = 5 }).GetAwaiter().GetResult();
        return ctx;
    }

    [Fact]
    public async Task Bare_First_selecting_converter_column_applies_ConvertFromProvider()
    {
        await using var ctx = MakeInt();
        // First child by Id per parent: p1 -> child1 (Score 3), p2 -> child3 (Score 5). Model values, not stored -3/-5.
        var got = ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).OrderBy(c => c.Id).Select(c => c.Score).FirstOrDefault())
            .ToList();
        Assert.Equal(new[] { 3, 5 }, got);
    }

    [Fact]
    public async Task Bare_Max_over_converter_column_converts_result()
    {
        await using var ctx = MakeInt();
        // MAX runs on the STORED (negated) representation then ConvertFromProvider maps it back:
        // p1 stored {-3,-7} -> MAX -3 -> model 3; p2 stored {-5} -> model 5. (EF-consistent: aggregate on
        // the stored form, convert the scalar result — equals MIN of the model values under negation.)
        var got = ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).Max(c => c.Score))
            .ToList();
        Assert.Equal(new[] { 3, 5 }, got);
    }

    [Fact]
    public async Task Bare_matches_wrapped_for_same_query()
    {
        await using var ctx = MakeInt();
        var bare = ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => ctx.Query<Child>().Where(c => c.ParentId == p.Id).OrderBy(c => c.Id).Select(c => c.Score).FirstOrDefault())
            .ToList();
        var wrapped = ctx.Query<Parent>().OrderBy(p => p.Id)
            .Select(p => new { X = ctx.Query<Child>().Where(c => c.ParentId == p.Id).OrderBy(c => c.Id).Select(c => c.Score).FirstOrDefault() })
            .ToList().Select(r => r.X).ToList();
        Assert.Equal(wrapped, bare);
    }

    // --- Enum-stored-as-name converter: a type MISMATCH (string stored, enum model). A bare projection
    //     previously threw a FormatException blindly casting the provider string to the enum. ---
    public enum Status { Active = 1, Inactive = 2, Archived = 3 }

    [Table("BcscEParent")]
    public class EParent { [Key] public int Id { get; set; } }

    [Table("BcscEChild")]
    public class EChild { [Key] public int Id { get; set; } public int ParentId { get; set; } public Status Status { get; set; } }

    private sealed class EnumToNameConverter : ValueConverter<Status, string>
    {
        public override object? ConvertToProvider(Status v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Status>((string)v);
    }

    private static DbContext MakeEnum()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE BcscEParent (Id INTEGER PRIMARY KEY);
                CREATE TABLE BcscEChild (Id INTEGER PRIMARY KEY, ParentId INTEGER NOT NULL, Status TEXT NOT NULL);
                INSERT INTO BcscEParent VALUES (1),(2);
                INSERT INTO BcscEChild VALUES (1,1,'Active'),(2,1,'Inactive'),(3,2,'Archived');
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<EParent>().HasKey(p => p.Id);
                mb.Entity<EChild>().HasKey(c => c.Id);
                mb.Entity<EChild>().Property(c => c.Status).HasConversion(new EnumToNameConverter());
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    [Fact]
    public async Task Bare_First_selecting_enum_converter_column_projects_clr_enum()
    {
        await using var ctx = MakeEnum();
        // First child by Id: p1 -> Active, p2 -> Archived. Must materialize the enum, not throw casting the name.
        var got = ctx.Query<EParent>().OrderBy(p => p.Id)
            .Select(p => ctx.Query<EChild>().Where(c => c.ParentId == p.Id).OrderBy(c => c.Id).Select(c => c.Status).FirstOrDefault())
            .ToList();
        Assert.Equal(new[] { Status.Active, Status.Archived }, got);
    }
}
