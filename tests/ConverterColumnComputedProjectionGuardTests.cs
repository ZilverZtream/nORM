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
/// A value-converter column used in VALUE position inside a computed projection — a conditional
/// (<c>cond ? c.Col : x</c>), arithmetic (<c>c.Col + n</c>), cast, or method call — emits the STORED column
/// in SQL and ConvertFromProvider can't be applied to the mixed computed result, so it would return the raw
/// provider representation (silently wrong). These now FAIL LOUD with NormUnsupportedFeatureException instead
/// of returning a wrong value. A converter column in PREDICATE position (a comparison / conditional Test such
/// as <c>c.Col == k ? a : b</c>) is bound by the predicate path and still works, as does a direct projection
/// (<c>Select(c => c.Col)</c> / <c>new { c.Col }</c>) and any non-converter computed projection. An
/// order-preserving +1000 converter makes an unconverted result observable.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class ConverterColumnComputedProjectionGuardTests
{
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => v + 1000;
        public override object? ConvertFromProvider(int v) => Convert.ToInt32(v) - 1000;
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("CccpCat")]
    public sealed class Cat
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Code { get; set; } // converter (+1000)
        public string Name { get; set; } = "";
    }

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE CccpCat (Id INTEGER PRIMARY KEY, Code INTEGER NOT NULL, Name TEXT NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Cat>().HasKey(c => c.Id);
                mb.Entity<Cat>().Property(c => c.Code).HasConversion(new OffsetConverter());
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        await ctx.InsertAsync(new Cat { Id = 1, Code = 10, Name = "a" });
        await ctx.InsertAsync(new Cat { Id = 2, Code = 20, Name = "b" });
        return ctx;
    }

    [Fact]
    public async Task Bare_conditional_over_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Cat>().Select(c => c.Id > 0 ? c.Code : -1).ToList());
    }

    [Fact]
    public async Task Anon_conditional_over_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Cat>().Select(c => new { c.Id, X = c.Id > 0 ? c.Code : -1 }).ToList());
    }

    [Fact]
    public async Task Arithmetic_over_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Cat>().Select(c => c.Code + 5).ToList());
    }

    [Fact]
    public async Task Anon_method_call_over_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Cat>().Select(c => new { c.Id, X = c.Code.ToString() }).ToList());
    }

    [Fact]
    public async Task Anon_cast_over_converter_column_fails_loud()
    {
        using var ctx = await CtxAsync();
        Assert.Throws<NormUnsupportedFeatureException>(() =>
            ctx.Query<Cat>().Select(c => new { c.Id, X = (long)c.Code }).ToList());
    }

    [Fact]
    public async Task Direct_projection_of_converter_column_still_converts()
    {
        using var ctx = await CtxAsync();
        Assert.Equal(new[] { 10, 20 }, ctx.Query<Cat>().OrderBy(c => c.Id).Select(c => c.Code).ToList());
        Assert.Equal(new[] { 10, 20 }, ctx.Query<Cat>().OrderBy(c => c.Id).Select(c => new { c.Id, c.Code }).ToList().Select(x => x.Code).ToArray());
    }

    [Fact]
    public async Task Converter_column_in_predicate_position_still_works()
    {
        using var ctx = await CtxAsync();
        // c.Code == 20 puts the converter in the conditional Test (predicate) — bound to provider form, works.
        Assert.Equal(new[] { "lo", "hi" }, ctx.Query<Cat>().OrderBy(c => c.Id).Select(c => c.Code == 20 ? "hi" : "lo").ToList());
        Assert.Equal(new[] { "a", "b" }, ctx.Query<Cat>().Where(c => c.Code >= 10).OrderBy(c => c.Id).Select(c => c.Name).ToList());
    }

    [Fact]
    public async Task Nonconverter_computed_projection_still_works()
    {
        using var ctx = await CtxAsync();
        Assert.Equal(new[] { -1, 2 }, ctx.Query<Cat>().OrderBy(c => c.Id).Select(c => c.Id > 1 ? c.Id : -1).ToList());
    }
}
