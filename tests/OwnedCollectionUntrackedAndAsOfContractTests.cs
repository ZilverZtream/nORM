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
/// Pins owned-collection loading contracts found by adversarial probing: (1) owned rows load
/// on UNTRACKED reads — owned data is part of the entity, not of change tracking, and
/// AsNoTracking/NoTracking-default reads previously returned owners with silently EMPTY
/// collections; (2) the OwnsMany table-name override reaches the owned type's own mapping,
/// so the temporal bootstrap targets the configured child table instead of crashing on the
/// CLR-name default; (3) AsOf over an owner with owned collections fails loud — owned
/// history rows do not carry the owner key, so era reconstruction of the collection is
/// impossible until owned temporal history includes the FK (tracked follow-up).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class OwnedCollectionUntrackedAndAsOfContractTests
{
    [Table("OcaPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public string Title { get; set; } = "";
        public List<Line> Lines { get; set; } = new();
    }

    // Deliberately NO [Table] attribute: the OwnsMany(tableName: "OcaLine") configuration
    // must reach the owned type's own mapping, or the temporal bootstrap targets a
    // nonexistent CLR-name table and history-window reads break.
    public class Line
    {
        [Key] public int Id { get; set; }
        public string Text { get; set; } = "";
    }

    [Fact]
    public async Task Owned_collection_under_as_of_probe()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var _cn = cn;
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE OcaPost (Id INTEGER PRIMARY KEY, Title TEXT NOT NULL);
                CREATE TABLE OcaLine (Id INTEGER PRIMARY KEY, PostId INTEGER NOT NULL, Text TEXT NOT NULL);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions
        {
            OnModelCreating = mb => mb.Entity<Post>().OwnsMany<Line>(p => p.Lines, tableName: "OcaLine", foreignKey: "PostId")
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

        var post = new Post { Id = 1, Title = "p" };
        post.Lines.Add(new Line { Id = 1, Text = "v1" });
        ctx.Add(post);
        await ctx.SaveChangesAsync();
        await Task.Delay(60);
        var t1 = await ServerNowAsync();   // post has ONE line: "v1"
        await Task.Delay(60);

        var tracked = (await ctx.Query<Post>().Where(p => p.Id == 1).ToListAsync()).Single();
        tracked.Lines.Single().Text = "v2";           // edit after t1
        tracked.Lines.Add(new Line { Id = 2, Text = "extra" });   // add after t1
        await ctx.SaveChangesAsync();

        // Tracked fast-path read (property-equality Where) loads owned rows.
        var live = (await ctx.Query<Post>().Where(p => p.Id == 1).ToListAsync()).Single();
        Assert.Equal(new[] { "v2", "extra" }, live.Lines.OrderBy(l => l.Id).Select(l => l.Text).ToArray());

        // Tracked full-pipeline read (OrderBy defeats the fast path) loads owned rows.
        var liveFull = (await ctx.Query<Post>().OrderBy(p => p.Title).ToListAsync()).Single();
        Assert.Equal(new[] { "v2", "extra" }, liveFull.Lines.OrderBy(l => l.Id).Select(l => l.Text).ToArray());

        // UNTRACKED read loads owned rows too: owned data is part of the entity, not of
        // change tracking - AsNoTracking previously returned owners with EMPTY collections.
        var liveNt = (await ((INormQueryable<Post>)ctx.Query<Post>()).AsNoTracking().OrderBy(p => p.Title).ToListAsync()).Single();
        Assert.Equal(new[] { "v2", "extra" }, liveNt.Lines.OrderBy(l => l.Id).Select(l => l.Text).ToArray());

        // AsOf over an owner with owned collections FAILS LOUD: the owned history rows do
        // not carry the owner FK (the FK exists only in the physical child table, not in
        // the property-backed mapping the temporal DDL mirrors), so the owned rows at t1
        // cannot be correlated to owners — silently returning empty or era-mixed
        // collections would be a wrong result. (Carrying the owner key in owned history
        // is the tracked follow-up that will replace this boundary with reconstruction.)
        var ex = await Assert.ThrowsAsync<NormUnsupportedFeatureException>(() =>
            ctx.Query<Post>().AsOf(t1).ToListAsync());
        Assert.Contains("owned", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
}
