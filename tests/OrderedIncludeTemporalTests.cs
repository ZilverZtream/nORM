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
/// An ordered / top-N Include under AsOf must rank the HISTORY-window rows, not the live table: the
/// ROW_NUMBER window reads the same era as the root, so a post-snapshot high-score child neither leaks
/// into the graph nor displaces a historical child from the top-N (the #1 silent-era-mix hazard).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OrderedIncludeTemporalTests
{
    [Table("OitmBlog")]
    public class Blog
    {
        [Key] public int Id { get; set; }
        public string Name { get; set; } = "";
        public List<Post> Posts { get; set; } = new();
    }

    [Table("OitmPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public int BlogId { get; set; }
        public int Score { get; set; }
    }

    [Fact]
    public async Task Ordered_topN_include_under_as_of_ranks_the_historical_window()
    {
        var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OitmBlog (Id INTEGER PRIMARY KEY, Name TEXT NOT NULL);
                CREATE TABLE OitmPost (Id INTEGER PRIMARY KEY, BlogId INTEGER NOT NULL, Score INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Blog>().HasKey(b => b.Id);
                mb.Entity<Post>().HasKey(p => p.Id);
                mb.Entity<Blog>().HasMany(b => b.Posts).WithOne().HasForeignKey(p => p.BlogId, b => b.Id);
            }
        };
        opts.EnableTemporalVersioning();
        await using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        async Task<DateTime> ServerNowAsync()
        {
            using var cmd = cn.CreateCommand();
            cmd.CommandText = "SELECT strftime('%Y-%m-%d %H:%M:%f', 'now')";
            var text = (string)(await cmd.ExecuteScalarAsync())!;
            return DateTime.SpecifyKind(DateTime.Parse(text, CultureInfo.InvariantCulture, DateTimeStyles.None), DateTimeKind.Utc);
        }

        ctx.Add(new Blog { Id = 1, Name = "b" });
        ctx.Add(new Post { Id = 1, BlogId = 1, Score = 10 });
        ctx.Add(new Post { Id = 2, BlogId = 1, Score = 20 });
        ctx.Add(new Post { Id = 3, BlogId = 1, Score = 30 });
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync();   // historical top-2 by score = 30, 20
        await Task.Delay(60);

        // After the snapshot: add a higher-scored post and boost a low one past the historical winners.
        ctx.Add(new Post { Id = 4, BlogId = 1, Score = 100 });
        var p1 = ctx.Query<Post>().First(p => p.Id == 1);
        p1.Score = 999;
        await ctx.SaveChangesAsync();

        var historic = await ((INormQueryable<Blog>)ctx.Query<Blog>())
            .Include(b => b.Posts.OrderByDescending(p => p.Score).Take(2)).AsOf(t1).ToListAsync();

        // Must rank the t1 era: [30, 20]. The post-t1 score-100 post and the boosted 999 must not appear.
        Assert.Equal(new[] { 30, 20 }, historic.Single().Posts.Select(p => p.Score).ToArray());

        // Live ordered top-2 sees the new era: 999, 100.
        var live = await ((INormQueryable<Blog>)ctx.Query<Blog>())
            .Include(b => b.Posts.OrderByDescending(p => p.Score).Take(2)).ToListAsync();
        Assert.Equal(new[] { 999, 100 }, live.Single().Posts.Select(p => p.Score).ToArray());
    }
}
