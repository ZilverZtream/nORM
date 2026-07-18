using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A projected aggregate over an owned (or many-to-many) collection reads the LIVE child/bridge table, so
/// under AsOf it would count the current era rather than the historical one the owner is reconstructed at —
/// disagreeing with the loaded collection (which reconstructs through the history window). Reconstructing the
/// era inside the aggregate subquery is a follow-up; until then the aggregate fails loud under an active
/// temporal scope instead of returning a silently mismatched count.
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class NavigationAggregateAsOfContractTests
{
    [Table("OaaPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    public class Line { [Key] public int Id { get; set; } public string Text { get; set; } = ""; }

    [Fact]
    public async Task owned_aggregate_under_asof()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OaaPost (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE OaaLine (Id INTEGER PRIMARY KEY, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Line>(p => p.Lines, tableName: "OaaLine", foreignKey: "PostId")
        };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        async Task<DateTime> ServerNowAsync()
        {
            using var c = cn.CreateCommand();
            c.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
            var text = (string)(await c.ExecuteScalarAsync())!;
            return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
        }

        var post = new Post { Id = 1, Title = "p" };
        post.Lines.Add(new Line { Id = 1, Text = "v1" });
        ctx.Add(post);
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync();   // ONE line at t1
        await Task.Delay(60);

        var tracked = (await ctx.Query<Post>().Where(p => p.Id == 1).ToListAsync()).Single();
        tracked.Lines.Add(new Line { Id = 2, Text = "extra" });   // add a 2nd line after t1
        await ctx.SaveChangesAsync();

        // The AsOf LOAD reconstructs one line at t1, but the aggregate reads the live table (2 lines). Rather
        // than silently disagree with the loaded collection, the AsOf aggregate fails loud.
        await Assert.ThrowsAsync<NormUnsupportedFeatureException>(async () =>
            await ctx.Query<Post>().AsOf(t1).Select(p => new { p.Id, N = p.Lines.Count() }).ToListAsync());
    }
}
