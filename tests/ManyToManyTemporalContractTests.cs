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
/// Pins the many-to-many x temporal boundary: the association table is a raw, user-owned,
/// UNVERSIONED table, so the association membership at an AsOf timestamp is unknowable —
/// AsOf combined with a many-to-many Include fails loud at translation instead of silently
/// joining LIVE associations onto historical rows. Many-to-many includes without AsOf are
/// unaffected by temporal mode. (Versioning association tables is a possible future
/// product decision; until then the combination is deterministically rejected.)
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ManyToManyTemporalContractTests
{
    [Table("M2mT_Post")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("M2mT_Tag")]
    public class Tag
    {
        [Key] public int Id { get; set; }
        public string Label { get; set; } = "";
    }

    [Fact]
    public async Task As_of_with_m2m_include_probe()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE M2mT_Post (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE M2mT_Tag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL);
                CREATE TABLE M2mT_PostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("M2mT_PostTag", "PostId", "TagId");
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

        var tagA = new Tag { Id = 1, Label = "a" };
        var tagB = new Tag { Id = 2, Label = "b" };
        var post = new Post { Id = 1, Title = "p1" };
        post.Tags.Add(tagA);
        ctx.Add(tagA);
        ctx.Add(tagB);
        ctx.Add(post);
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync();   // post has tag A only
        await Task.Delay(60);

        var tracked = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        tracked.Tags.Add(tagB);
        await ctx.SaveChangesAsync();      // association change: now A + B

        // Current (no AsOf): A + B — the many-to-many include is unaffected by temporal mode.
        var current = ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).ToList().Single();
        Assert.Equal(new[] { "a", "b" }, current.Tags.OrderBy(t => t.Id).Select(t => t.Label).ToArray());

        // AsOf + many-to-many Include must FAIL LOUD: the association table is a raw,
        // unversioned table, so the association membership at t1 is unknowable — silently
        // joining LIVE associations onto historical rows would mix eras.
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ((INormQueryable<Post>)ctx.Query<Post>()).Include(p => p.Tags).AsOf(t1).ToListAsync());
        Assert.Contains("association table is not versioned", ex.Message, StringComparison.Ordinal);

        // The association table has no history table (raw table, no mapping).
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE name = 'M2mT_PostTag_History'";
            Assert.Equal(0, Convert.ToInt32(cmd.ExecuteScalar()));
        }
    }
}
