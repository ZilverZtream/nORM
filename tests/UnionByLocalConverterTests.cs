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
/// `UnionBy(localSequence, keySelector)` merging a DB query with an in-memory list must bind each local
/// column's PROVIDER (stored) value via the value converter, so the appended rows compare and materialize on
/// the same representation as the DB source arm. The local-union lowering bound the raw CLR value, so an
/// appended converted column was run backwards through ConvertFromProvider (value corruption) and its dedup
/// key mismatched the stored source keys.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class UnionByLocalConverterTests
{
    private sealed class OffsetConverter : ValueConverter<int, int>
    {
        public override object? ConvertToProvider(int v) => v + 1000;
        public override object? ConvertFromProvider(int v) => Convert.ToInt32(v) - 1000;
    }

    [Table("UblWidget")]
    public sealed class Widget
    {
        [Key] public int Id { get; set; }
        public int Score { get; set; }   // converter (+1000 stored)
    }

    private static async Task<DbContext> CtxAsync()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE UblWidget (Id INTEGER PRIMARY KEY, Score INTEGER NOT NULL);";
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Widget>().HasKey(w => w.Id);
                mb.Entity<Widget>().Property(w => w.Score).HasConversion(new OffsetConverter());
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        await ctx.InsertAsync(new Widget { Id = 1, Score = 10 });
        await ctx.InsertAsync(new Widget { Id = 2, Score = 20 });
        return ctx;
    }

    [Fact]
    public async Task UnionBy_local_converter_column_round_trips()
    {
        await using var ctx = await CtxAsync();
        var local = new[] { new Widget { Id = 3, Score = 77 } };
        var got = (await ctx.Query<Widget>().OrderBy(w => w.Id).UnionBy(local, w => w.Id).ToListAsync())
            .OrderBy(w => w.Id).Select(w => $"{w.Id}:{w.Score}").ToList();
        Assert.Equal(new[] { "1:10", "2:20", "3:77" }, got);
    }

    [Fact]
    public async Task UnionBy_local_key_on_converter_column_dedups_on_stored_value()
    {
        await using var ctx = await CtxAsync();
        // local Score=10 collides with source Id=1's Score=10 -> UnionBy(key=Score) must drop the local dup.
        var local = new[] { new Widget { Id = 98, Score = 10 }, new Widget { Id = 99, Score = 30 } };
        var got = (await ctx.Query<Widget>().OrderBy(w => w.Id).UnionBy(local, w => w.Score).ToListAsync())
            .Select(w => w.Score).OrderBy(s => s).ToList();
        Assert.Equal(new[] { 10, 20, 30 }, got);
    }
}
