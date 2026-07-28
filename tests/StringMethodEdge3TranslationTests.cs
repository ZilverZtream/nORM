using System;
using System.Collections.Generic;
using System.Linq;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable
namespace nORM.Tests;

[Trait("Category", TestCategory.Fast)]
public class StringMethodEdge3TranslationTests
{
    [Table("SRow3")]
    public class SRow3
    {
        [Key] public int Id { get; set; }
        public string Text { get; set; } = "";
        public string? Nul { get; set; }
    }

    private static async Task<DbContext> NewCtx(SqliteConnection cn, params SRow3[] rows)
    {
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE SRow3 (Id INTEGER PRIMARY KEY, Text TEXT NOT NULL, Nul TEXT NULL);";
            cmd.ExecuteNonQuery();
        }
        var ctx = new DbContext(cn, new SqliteProvider());
        foreach (var r in rows) ctx.Add(r);
        await ctx.SaveChangesAsync();
        return ctx;
    }

    // WHERE-side: `$"{x.Nul}" == ""` must include the NULL row (interpolation renders null as "").
    // If nORM emits the bare column, `NULL = ''` is UNKNOWN and the row is silently DROPPED.
    [Fact]
    public async Task Interpolation_single_hole_null_in_where_matches_oracle()
    {
        var seed = new[]
        {
            new SRow3 { Id = 1, Text = "a", Nul = null },
            new SRow3 { Id = 2, Text = "b", Nul = "" },
            new SRow3 { Id = 3, Text = "c", Nul = "x" },
        };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow3>().Where(x => $"{x.Nul}" == "").Select(x => x.Id).ToList().OrderBy(i => i).ToArray();
        var oracle = seed.Where(x => $"{x.Nul}" == "").Select(x => x.Id).OrderBy(i => i).ToArray();
        Assert.Equal(oracle, actual); // {1,2}
    }

    // string.Concat single-arg over NULL: .NET string.Concat((object?)null) => "".
    [Fact]
    public async Task Concat_single_null_arg_matches_oracle()
    {
        var seed = new[] { new SRow3 { Id = 1, Text = "hi", Nul = null } };
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = await NewCtx(cn, seed);
        var actual = ctx.Query<SRow3>().Select(x => string.Concat(x.Nul)).ToList().Single();
        var oracle = seed.Select(x => string.Concat(x.Nul)).Single();
        Assert.Equal(oracle, actual); // ""
    }
}
