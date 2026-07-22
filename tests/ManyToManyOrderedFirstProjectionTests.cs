using System;
using System.Collections.Generic;
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
/// Projecting an ordered First/Last over a MANY-TO-MANY collection —
/// <c>p.Tags.[Where(pred).]OrderBy(k).Select(t =&gt; t.Col).First/Last()</c> — emits a LIMIT-1 correlated
/// subquery that joins the bridge table to the related table. Previously m2m links (which live in
/// ManyToManyJoins, not Relations) were unrecognized by the ordered-First emit, so the shape fell through
/// to the outer handler and was rendered as a nonexistent SQL function (FIRSTORDEFAULT) that crashed with a
/// provider error. m2m Min/Max/Count already worked; this closes the ordered-First gap. A value-converter
/// selector column is materialized through ConvertFromProvider (a +1000 offset converter makes an
/// unconverted result off-by-1000); ordering runs on the stored column; an empty association yields the
/// scalar default.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ManyToManyOrderedFirstProjectionTests
{
    // Order-preserving offset so agg/ordering on stored == on model, and an unconverted result is off-by-1000.
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => v + 1000;
        public override object? ConvertFromProvider(int v) => Convert.ToInt32(v) - 1000;
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("MofPost")]
    public sealed class Post
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("MofTag")]
    public sealed class Tag
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Weight { get; set; } // converter (+1000)
    }

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE MofPost (Id INTEGER PRIMARY KEY);" +
                "CREATE TABLE MofTag (Id INTEGER PRIMARY KEY, Weight INTEGER NOT NULL);" +
                "CREATE TABLE MofPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL, PRIMARY KEY(PostId, TagId));";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("MofPostTag", "PostId", "TagId");
                mb.Entity<Tag>().HasKey(t => t.Id);
                mb.Entity<Tag>().Property(t => t.Weight).HasConversion(new OffsetConverter());
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        await ctx.InsertAsync(new Post { Id = 1 });
        await ctx.InsertAsync(new Post { Id = 2 });
        await ctx.InsertAsync(new Post { Id = 3 }); // no tags
        await ctx.InsertAsync(new Tag { Id = 1, Weight = 10 });
        await ctx.InsertAsync(new Tag { Id = 2, Weight = 20 });
        await ctx.InsertAsync(new Tag { Id = 3, Weight = 30 });
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "INSERT INTO MofPostTag VALUES (1,1),(1,2),(2,3);"; // post1->{10,20}, post2->{30}, post3->{}
            cmd.ExecuteNonQuery();
        }
        return ctx;
    }

    [Fact]
    public async Task First_selecting_converter_column_over_m2m_applies_conversion()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Post>().OrderBy(p => p.Id)
            .Select(p => p.Tags.OrderBy(t => t.Id).Select(t => t.Weight).FirstOrDefault()).ToList();
        Assert.Equal(new[] { 10, 30, 0 }, got); // model values (not stored 1010/1030), empty -> 0
    }

    [Fact]
    public async Task First_selecting_nonconverter_column_over_m2m()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Post>().OrderBy(p => p.Id)
            .Select(p => p.Tags.OrderBy(t => t.Id).Select(t => t.Id).FirstOrDefault()).ToList();
        Assert.Equal(new[] { 1, 3, 0 }, got);
    }

    [Fact]
    public async Task Last_over_m2m_reverses_ordering()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Post>().OrderBy(p => p.Id)
            .Select(p => p.Tags.OrderBy(t => t.Id).Select(t => t.Weight).LastOrDefault()).ToList();
        Assert.Equal(new[] { 20, 30, 0 }, got);
    }

    [Fact]
    public async Task Filtered_first_over_m2m_on_converter_column()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Post>().OrderBy(p => p.Id)
            .Select(p => p.Tags.Where(t => t.Weight > 15).OrderBy(t => t.Id).Select(t => t.Weight).FirstOrDefault()).ToList();
        Assert.Equal(new[] { 20, 30, 0 }, got);
    }

    [Fact]
    public async Task OrderByDescending_first_over_m2m()
    {
        using var ctx = await CtxAsync();
        var got = ctx.Query<Post>().OrderBy(p => p.Id)
            .Select(p => p.Tags.OrderByDescending(t => t.Weight).Select(t => t.Weight).FirstOrDefault()).ToList();
        Assert.Equal(new[] { 20, 30, 0 }, got);
    }

    [Fact]
    public async Task Min_max_count_over_m2m_still_work()
    {
        using var ctx = await CtxAsync();
        Assert.Equal(new[] { 20, 30, 0 }, ctx.Query<Post>().OrderBy(p => p.Id).Select(p => p.Tags.Max(t => t.Weight)).ToList());
        Assert.Equal(new[] { 10, 30, 0 }, ctx.Query<Post>().OrderBy(p => p.Id).Select(p => p.Tags.Min(t => t.Weight)).ToList());
        Assert.Equal(new[] { 2, 1, 0 }, ctx.Query<Post>().OrderBy(p => p.Id).Select(p => p.Tags.Count()).ToList());
    }
}
