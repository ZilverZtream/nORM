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
/// Value-converter columns used across emitter surfaces (subquery Contains/IN, JOIN-ON, ExecuteDelete WHERE,
/// composite keys, nullable columns, navigation FKs, predicate aggregates) must bind/compare the PROVIDER
/// representation, not the raw MODEL value. Every case is checked against an explicit model-correct oracle.
/// Sharpest probes: an enum-stored-AS-STRING converter (model enum &lt;-&gt; "Active"/"Closed") and a NEGATING int
/// converter (model N &lt;-&gt; stored -N), so a raw model bind against provider storage matches nothing. In
/// particular a subquery Select(i =&gt; i.ConvCol).Contains(constant/closure) must convert the tested value.
/// SQLite :memory: only; seeded with the STORED (provider) representation via raw SQL or nORM writes.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ConverterEmitterSurfaceTests
{
    public enum Kind { Active, Pending, Closed }

    private sealed class KindToStringConverter : ValueConverter<Kind, string>
    {
        public override object? ConvertToProvider(Kind v) => v.ToString();
        public override object? ConvertFromProvider(string v) => Enum.Parse<Kind>(v);
    }

    // Model int N stored as -N. Any raw bind of the model value against storage is unambiguously wrong.
    private sealed class NegatingIntConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => -v;
        public override object? ConvertFromProvider(int v) => -v;
    }

    [Table("H55Inner")]
    public class InnerRow { [Key] public int Id { get; set; } public Kind Status { get; set; } public int Neg { get; set; } }

    [Table("H55Outer")]
    public class OuterRow { [Key] public int Id { get; set; } public Kind Status { get; set; } public int Neg { get; set; } }

    private static DbContext New(SqliteConnection cn, string ddl, Action<ModelBuilder>? extra = null)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand()) { cmd.CommandText = ddl; cmd.ExecuteNonQuery(); }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<InnerRow>().Property(e => e.Status).HasConversion(new KindToStringConverter());
                mb.Entity<InnerRow>().Property(e => e.Neg).HasConversion(new NegatingIntConverter());
                mb.Entity<OuterRow>().Property(e => e.Status).HasConversion(new KindToStringConverter());
                mb.Entity<OuterRow>().Property(e => e.Neg).HasConversion(new NegatingIntConverter());
                extra?.Invoke(mb);
            }
        };
        return new DbContext(cn, new SqliteProvider(), opts);
    }

    private const string TwoTableDdl =
        "CREATE TABLE H55Inner (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL, Neg INTEGER NOT NULL);" +
        "CREATE TABLE H55Outer (Id INTEGER PRIMARY KEY, Status TEXT NOT NULL, Neg INTEGER NOT NULL);";

    // =========================================================================================
    // SURFACE 2 (SHARP): subquery `.Select(x => x.ConvCol).Contains(<CONSTANT model value>)`.
    // The subquery yields PROVIDER values ('Active'); the tested constant must be converted too.
    // =========================================================================================
    [Fact]
    public async Task SubqueryContains_enumString_constant_matches_when_inner_has_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = New(cn, TwoTableDdl);
        ctx.Add(new InnerRow { Id = 1, Status = Kind.Active, Neg = 5 });
        ctx.Add(new InnerRow { Id = 2, Status = Kind.Closed, Neg = 6 });
        ctx.Add(new OuterRow { Id = 10, Status = Kind.Pending, Neg = 1 });
        ctx.Add(new OuterRow { Id = 11, Status = Kind.Pending, Neg = 2 });
        await ctx.SaveChangesAsync();

        // Inner DOES contain Kind.Active -> semi-join is TRUE for every outer row.
        var expected = new[] { 10, 11 };
        var actual = ctx.Query<OuterRow>()
            .Where(o => ctx.Query<InnerRow>().Select(i => i.Status).Contains(Kind.Active))
            .Select(o => o.Id).ToList().OrderBy(i => i).ToArray();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task SubqueryContains_negatingInt_constant_matches_when_inner_has_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = New(cn, TwoTableDdl);
        ctx.Add(new InnerRow { Id = 1, Status = Kind.Active, Neg = 5 });
        ctx.Add(new OuterRow { Id = 10, Status = Kind.Pending, Neg = 1 });
        await ctx.SaveChangesAsync();

        // Inner has model Neg == 5 (stored -5). Subquery yields stored -5; constant 5 must convert to -5.
        var expected = new[] { 10 };
        var actual = ctx.Query<OuterRow>()
            .Where(o => ctx.Query<InnerRow>().Select(i => i.Neg).Contains(5))
            .Select(o => o.Id).ToList().OrderBy(i => i).ToArray();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public async Task SubqueryContains_enumString_closure_matches_when_inner_has_value()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = New(cn, TwoTableDdl);
        ctx.Add(new InnerRow { Id = 1, Status = Kind.Active, Neg = 5 });
        ctx.Add(new OuterRow { Id = 10, Status = Kind.Pending, Neg = 1 });
        await ctx.SaveChangesAsync();

        var target = Kind.Active;
        var expected = new[] { 10 };
        var actual = ctx.Query<OuterRow>()
            .Where(o => ctx.Query<InnerRow>().Select(i => i.Status).Contains(target))
            .Select(o => o.Id).ToList().OrderBy(i => i).ToArray();
        Assert.Equal(expected, actual);
    }

    // CONTROL: tested value is a CONVERTER COLUMN of the outer row (both sides provider). Should be clean.
    [Fact]
    public async Task SubqueryContains_enumString_correlated_column_matches_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = New(cn, TwoTableDdl);
        ctx.Add(new InnerRow { Id = 1, Status = Kind.Active, Neg = 5 });
        ctx.Add(new InnerRow { Id = 2, Status = Kind.Closed, Neg = 6 });
        ctx.Add(new OuterRow { Id = 10, Status = Kind.Active, Neg = 1 });   // Active in inner -> match
        ctx.Add(new OuterRow { Id = 11, Status = Kind.Pending, Neg = 2 });  // Pending not in inner -> no match
        await ctx.SaveChangesAsync();

        var expected = new[] { 10 };
        var actual = ctx.Query<OuterRow>()
            .Where(o => ctx.Query<InnerRow>().Select(i => i.Status).Contains(o.Status))
            .Select(o => o.Id).ToList().OrderBy(i => i).ToArray();
        Assert.Equal(expected, actual);
    }

    // =========================================================================================
    // SURFACE 3: ExecuteDelete WHERE over a converter column.
    // =========================================================================================
    [Fact]
    public async Task ExecuteDelete_enumString_where_deletes_correct_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = New(cn, TwoTableDdl);
        ctx.Add(new OuterRow { Id = 1, Status = Kind.Active, Neg = 1 });
        ctx.Add(new OuterRow { Id = 2, Status = Kind.Closed, Neg = 2 });
        ctx.Add(new OuterRow { Id = 3, Status = Kind.Closed, Neg = 3 });
        await ctx.SaveChangesAsync();

        var deleted = await ctx.Query<OuterRow>().Where(o => o.Status == Kind.Closed).ExecuteDeleteAsync();
        Assert.Equal(2, deleted);
        var remaining = ctx.Query<OuterRow>().Select(o => o.Id).ToList().OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 1 }, remaining);
    }

    [Fact]
    public async Task ExecuteDelete_negatingInt_where_deletes_correct_rows()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = New(cn, TwoTableDdl);
        ctx.Add(new OuterRow { Id = 1, Status = Kind.Active, Neg = 7 });
        ctx.Add(new OuterRow { Id = 2, Status = Kind.Active, Neg = 8 });
        await ctx.SaveChangesAsync();

        var deleted = await ctx.Query<OuterRow>().Where(o => o.Neg == 7).ExecuteDeleteAsync();
        Assert.Equal(1, deleted);
        var remaining = ctx.Query<OuterRow>().Select(o => o.Id).ToList().OrderBy(i => i).ToArray();
        Assert.Equal(new[] { 2 }, remaining);
    }

    // =========================================================================================
    // SURFACE 7: Any/All/Count/First predicate over a converter column.
    // =========================================================================================
    [Fact]
    public async Task Predicate_terminals_over_converter_column_match_oracle()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = New(cn, TwoTableDdl);
        var data = new[]
        {
            new OuterRow { Id = 1, Status = Kind.Active, Neg = 5 },
            new OuterRow { Id = 2, Status = Kind.Closed, Neg = 6 },
            new OuterRow { Id = 3, Status = Kind.Closed, Neg = 7 },
        };
        foreach (var r in data) ctx.Add(r);
        await ctx.SaveChangesAsync();

        Assert.True(ctx.Query<OuterRow>().Any(o => o.Status == Kind.Active));
        Assert.False(ctx.Query<OuterRow>().Any(o => o.Status == Kind.Pending));
        Assert.False(ctx.Query<OuterRow>().All(o => o.Status == Kind.Closed));
        Assert.Equal(2, ctx.Query<OuterRow>().Count(o => o.Status == Kind.Closed));
        Assert.Equal(2, ctx.Query<OuterRow>().Count(o => o.Neg == 6 || o.Neg == 7));
        Assert.Equal(1, ctx.Query<OuterRow>().First(o => o.Status == Kind.Active).Id);
    }

    // =========================================================================================
    // SURFACE 8: projected value + projected boolean over a converter column.
    // =========================================================================================
    [Fact]
    public async Task Projected_value_and_boolean_over_converter_column()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = New(cn, TwoTableDdl);
        ctx.Add(new OuterRow { Id = 1, Status = Kind.Active, Neg = 5 });
        ctx.Add(new OuterRow { Id = 2, Status = Kind.Closed, Neg = 6 });
        await ctx.SaveChangesAsync();

        // Select(a => a.ConvCol) returns the MODEL value.
        var values = ctx.Query<OuterRow>().OrderBy(o => o.Id).Select(o => o.Status).ToList();
        Assert.Equal(new[] { Kind.Active, Kind.Closed }, values);
        var negs = ctx.Query<OuterRow>().OrderBy(o => o.Id).Select(o => o.Neg).ToList();
        Assert.Equal(new[] { 5, 6 }, negs);

        // Boolean projection over converter column.
        var flags = ctx.Query<OuterRow>().OrderBy(o => o.Id)
            .Select(o => new { o.Id, IsActive = o.Status == Kind.Active, NegIs5 = o.Neg == 5 }).ToList();
        Assert.True(flags[0].IsActive);
        Assert.True(flags[0].NegIs5);
        Assert.False(flags[1].IsActive);
        Assert.False(flags[1].NegIs5);
    }

    // =========================================================================================
    // SURFACE 4: composite key including a converter component (Find + Where identity).
    // =========================================================================================
    [Table("H55Comp")]
    public class CompRow { public int Part { get; set; } public Kind Status { get; set; } public string Data { get; set; } = ""; }

    [Fact]
    public async Task CompositeKey_with_converter_component_find_and_where()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE H55Comp (Part INTEGER NOT NULL, Status TEXT NOT NULL, Data TEXT NOT NULL, PRIMARY KEY(Part, Status));";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<CompRow>().HasKey(c => new { c.Part, c.Status });
                mb.Entity<CompRow>().Property(c => c.Status).HasConversion(new KindToStringConverter());
            }
        });
        ctx.Add(new CompRow { Part = 1, Status = Kind.Active, Data = "a" });
        ctx.Add(new CompRow { Part = 1, Status = Kind.Closed, Data = "c" });
        await ctx.SaveChangesAsync();

        var where = ctx.Query<CompRow>().Where(c => c.Part == 1 && c.Status == Kind.Closed).Select(c => c.Data).ToList();
        Assert.Equal(new[] { "c" }, where);

        var found = ctx.Find<CompRow>(1, Kind.Active);
        Assert.NotNull(found);
        Assert.Equal("a", found!.Data);
    }

    // =========================================================================================
    // SURFACE 6: navigation FK that is a converter column (Include).
    // =========================================================================================
    [Table("H55Principal")]
    public class Principal { [Key] public int Id { get; set; } public string Name { get; set; } = ""; public List<Dependent> Kids { get; set; } = new(); }

    [Table("H55Dependent")]
    public class Dependent { [Key] public int Id { get; set; } public int OwnerId { get; set; } public string Tag { get; set; } = ""; public Principal? Owner { get; set; } }

    [Fact]
    public void Navigation_fk_converter_include_matches_children()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE H55Principal (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);" +
                "CREATE TABLE H55Dependent (Id INTEGER PRIMARY KEY, OwnerId INTEGER NOT NULL, Tag TEXT NOT NULL);" +
                // Principal 3 stores FK -3 (converter negates). Seed raw provider values.
                "INSERT INTO H55Principal VALUES (3,'P3'),(4,'P4');" +
                "INSERT INTO H55Dependent VALUES (1,-3,'k3a'),(2,-3,'k3b'),(3,-4,'k4a');";
            cmd.ExecuteNonQuery();
        }
        using var ctx = new DbContext(cn, new SqliteProvider(), new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                // FK stored as negated int: model OwnerId=3 <-> stored -3, principal key Id (no converter).
                mb.Entity<Dependent>().Property(d => d.OwnerId).HasConversion(new NegatingIntConverter());
                mb.Entity<Principal>().HasMany(p => p.Kids).WithOne(d => d.Owner!).HasForeignKey(d => d.OwnerId, p => p.Id);
            }
        });

        var p3 = ctx.Query<Principal>().Include(p => p.Kids).First(p => p.Id == 3);
        var tags = p3.Kids.Select(k => k.Tag).OrderBy(s => s).ToArray();
        Assert.Equal(new[] { "k3a", "k3b" }, tags);
    }
}
