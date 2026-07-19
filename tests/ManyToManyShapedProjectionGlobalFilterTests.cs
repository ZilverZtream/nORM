using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using Xunit;

#nullable enable

namespace nORM.Tests;

/// <summary>
/// A soft-delete (or any user) global filter registered on the related entity must restrict which rows a
/// many-to-many SHAPED PROJECTION loads, exactly as it does for a many-to-many Include — the split-query
/// related fetch ANDs the same global-filter fragment. Without it, a filtered-out related row would leak into
/// the projected collection. Pins the projection path for the interaction the relation path already covers
/// (<see cref="ShapedCollectionAnonymousProjectionInteractionTests"/>).
/// </summary>
[Trait("Category", TestCategory.Fast)]
public class ManyToManyShapedProjectionGlobalFilterTests
{
    [Table("MgfPost")]
    public class Post
    {
        [Key] public int Id { get; set; }
        public List<Tag> Tags { get; set; } = new();
    }

    [Table("MgfTag")]
    public class Tag { [Key] public int Id { get; set; } public string Label { get; set; } = ""; public bool IsDeleted { get; set; } }

    [Fact]
    public void Shaped_m2m_projection_applies_the_related_entity_global_filter()
    {
        using var cn = new SqliteConnection("Data Source=:memory:"); cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = """
                CREATE TABLE MgfPost (Id INTEGER PRIMARY KEY);
                CREATE TABLE MgfTag (Id INTEGER PRIMARY KEY, Label TEXT NOT NULL, IsDeleted INTEGER NOT NULL);
                CREATE TABLE MgfPostTag (PostId INTEGER NOT NULL, TagId INTEGER NOT NULL);
                INSERT INTO MgfPost VALUES (1);
                INSERT INTO MgfTag VALUES (1,'live',0),(2,'dead',1);
                INSERT INTO MgfPostTag VALUES (1,1),(1,2);
                """;
            cmd.ExecuteNonQuery();
        }
        var opts = new DbContextOptions();
        opts.AddGlobalFilter<Tag>(t => !t.IsDeleted);
        opts.OnModelCreating = mb =>
        {
            mb.Entity<Post>().HasKey(p => p.Id);
            mb.Entity<Tag>().HasKey(t => t.Id);
            mb.Entity<Post>().HasMany<Tag>(p => p.Tags).WithMany().UsingTable("MgfPostTag", "PostId", "TagId");
        };
        using var ctx = new DbContext(cn, new SqliteProvider(), opts, ownsConnection: false);

        var rows = ctx.Query<Post>().Select(p => new { p.Id, Tags = p.Tags.ToList() }).ToList();
        // The soft-deleted 'dead' tag is associated with post 1 but excluded by the global filter.
        Assert.Equal(new[] { "live" }, rows.Single().Tags.Select(t => t.Label));
    }
}
