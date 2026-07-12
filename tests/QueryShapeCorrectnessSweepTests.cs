using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// Broad correctness sweep over common query shapes, guarding against the fast-path "accepted an
/// operator but silently dropped it" class of bug (see First(predicate) and Take fixes). Each case
/// asserts the exact result a correct translation must produce.
/// </summary>
[Xunit.Trait("Category", "Fast")]
public class QueryShapeCorrectnessSweepTests
{
    [Table("QsRow")]
    private class QsRow
    {
        [Key] public int Id { get; set; }
        public int Bucket { get; set; }
        public bool Active { get; set; }
    }

    private static (SqliteConnection Cn, DbContext Ctx) Create()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            var sb = new System.Text.StringBuilder("CREATE TABLE QsRow (Id INTEGER PRIMARY KEY, Bucket INTEGER NOT NULL, Active INTEGER NOT NULL);");
            for (int i = 1; i <= 10; i++)
                sb.Append($"INSERT INTO QsRow VALUES ({i},{i % 3},{(i % 2 == 0 ? 1 : 0)});");
            cmd.CommandText = sb.ToString();
            cmd.ExecuteNonQuery();
        }
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void Query_shapes_produce_correct_results()
    {
        var (cn, ctx) = Create();
        using var _cn = cn;
        using var _ctx = ctx;

        // Take / Skip / paging
        Assert.Equal(new[] { 1, 2, 3 }, ctx.Query<QsRow>().Take(3).ToList().Select(r => r.Id).ToArray());
        Assert.Equal(new[] { 4, 5, 6 }, ctx.Query<QsRow>().OrderBy(r => r.Id).Skip(3).Take(3).ToList().Select(r => r.Id).ToArray());
        Assert.Equal(new[] { 10, 9, 8 }, ctx.Query<QsRow>().OrderByDescending(r => r.Id).Take(3).ToList().Select(r => r.Id).ToArray());

        // First / FirstOrDefault with and without predicate
        Assert.Equal(7, ctx.Query<QsRow>().First(r => r.Id == 7).Id);
        Assert.Equal(1, ctx.Query<QsRow>().First().Id);
        Assert.Null(ctx.Query<QsRow>().FirstOrDefault(r => r.Id == 999));
        Assert.Equal(8, ctx.Query<QsRow>().OrderByDescending(r => r.Id).Where(r => r.Id < 9).First().Id);

        // Single with predicate
        Assert.Equal(5, ctx.Query<QsRow>().Single(r => r.Id == 5).Id);

        // Where + Take
        Assert.Equal(2, ctx.Query<QsRow>().Where(r => r.Active).Take(2).ToList().Count);
        Assert.All(ctx.Query<QsRow>().Where(r => r.Active).ToList(), r => Assert.True(r.Active));

        // Count / Any with predicate (Bucket==0 for Id 3,6,9)
        Assert.Equal(3, ctx.Query<QsRow>().Count(r => r.Bucket == 0));
        Assert.True(ctx.Query<QsRow>().Any(r => r.Id == 4));
        Assert.False(ctx.Query<QsRow>().Any(r => r.Id == 999));

        // Sequential different literals must not collide (plan-cache regression guard)
        Assert.Equal(2, ctx.Query<QsRow>().First(r => r.Id == 2).Id);
        Assert.Equal(9, ctx.Query<QsRow>().First(r => r.Id == 9).Id);
        Assert.Equal(3, ctx.Query<QsRow>().Count(r => r.Bucket == 0)); // 3,6,9
        Assert.Equal(4, ctx.Query<QsRow>().Count(r => r.Bucket == 1)); // 1,4,7,10
    }
}
