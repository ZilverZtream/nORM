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
/// Joins whose key columns carry a value converter. The join matches on the stored representation (both
/// sides store through the same converter, so equal model values imply equal stored values), a converter
/// column projected from the joined side is run through ConvertFromProvider, and a WHERE on a converter
/// column binds the constant's provider representation. A +1000 offset converter makes an unconverted
/// projected value visible as off-by-1000. (Regression guard for the join × value-converter intersection.)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public sealed class JoinOnConverterKeyColumnTests
{
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => v + 1000;
        public override object? ConvertFromProvider(int v) => Convert.ToInt32(v) - 1000;
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("JckCat")]
    public sealed class Cat
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int Code { get; set; } // converter (+1000)
    }

    [System.ComponentModel.DataAnnotations.Schema.Table("JckItem")]
    public sealed class Item
    {
        [System.ComponentModel.DataAnnotations.Key] public int Id { get; set; }
        public int CatCode { get; set; } // converter (+1000)
    }

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE JckCat (Id INTEGER PRIMARY KEY, Code INTEGER NOT NULL);" +
                "CREATE TABLE JckItem (Id INTEGER PRIMARY KEY, CatCode INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Cat>().HasKey(c => c.Id);
                mb.Entity<Cat>().Property(c => c.Code).HasConversion(new OffsetConverter());
                mb.Entity<Item>().HasKey(i => i.Id);
                mb.Entity<Item>().Property(i => i.CatCode).HasConversion(new OffsetConverter());
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        await ctx.InsertAsync(new Cat { Id = 1, Code = 10 });
        await ctx.InsertAsync(new Cat { Id = 2, Code = 20 });
        await ctx.InsertAsync(new Item { Id = 1, CatCode = 10 });
        await ctx.InsertAsync(new Item { Id = 2, CatCode = 20 });
        await ctx.InsertAsync(new Item { Id = 3, CatCode = 99 }); // no matching cat
        return ctx;
    }

    [Fact]
    public async Task Inner_join_on_converter_keys_matches_by_model_value()
    {
        using var ctx = await CtxAsync();
        var pairs = (from i in ctx.Query<Item>()
                     join c in ctx.Query<Cat>() on i.CatCode equals c.Code
                     orderby i.Id
                     select i.Id + "->" + c.Id).ToList();
        Assert.Equal(new[] { "1->1", "2->2" }, pairs); // item3 (99) has no match
    }

    [Fact]
    public async Task Project_converter_column_from_joined_side_converts()
    {
        using var ctx = await CtxAsync();
        var codes = (from i in ctx.Query<Item>()
                     join c in ctx.Query<Cat>() on i.CatCode equals c.Code
                     orderby i.Id
                     select c.Code).ToList();
        Assert.Equal(new[] { 10, 20 }, codes); // model values, not stored 1010/1020
    }

    [Fact]
    public async Task Where_on_joined_converter_column_binds_provider_value()
    {
        using var ctx = await CtxAsync();
        var ids = (from i in ctx.Query<Item>()
                   join c in ctx.Query<Cat>() on i.CatCode equals c.Code
                   where c.Code == 20
                   select i.Id).ToList();
        Assert.Equal(new[] { 2 }, ids);
    }

    [Fact]
    public async Task Left_join_clean_projection_over_converter_keys()
    {
        using var ctx = await CtxAsync();
        // Clean anon projection selecting a non-converter member from the nullable side.
        var rows = (from i in ctx.Query<Item>()
                    join c in ctx.Query<Cat>() on i.CatCode equals c.Code into g
                    from c in g.DefaultIfEmpty()
                    select new { i.Id, Cat = c == null ? -1 : c.Id })
                   .ToList().OrderBy(x => x.Id).Select(x => x.Id + ":" + x.Cat).ToList();
        Assert.Equal(new[] { "1:1", "2:2", "3:-1" }, rows);
    }
}
