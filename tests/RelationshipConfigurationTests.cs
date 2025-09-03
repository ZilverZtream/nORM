using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class RelationshipConfigurationTests
{
    private class Blog
    {
        public int Key { get; set; }
        public ICollection<Post> Posts { get; set; } = new List<Post>();
    }

    private class Post
    {
        [Key]
        public int Id { get; set; }
        public int ParentKey { get; set; }
        public Blog? Parent { get; set; }
    }

    [Fact]
    public void Fluent_relationship_configuration_is_used()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Blog>()
                    .HasKey(b => b.Key)
                    .HasMany(b => b.Posts)
                    .WithOne(p => p.Parent)
                    .HasForeignKey(p => p.ParentKey);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var blogMap = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(Blog) })!;

        Assert.True(blogMap.Relations.ContainsKey(nameof(Blog.Posts)));
        var rel = blogMap.Relations[nameof(Blog.Posts)];
        Assert.Equal(typeof(Post), rel.DependentType);
        Assert.Equal("Key", rel.PrincipalKey.PropName);
        Assert.Equal("ParentKey", rel.ForeignKey.PropName);
    }
}

