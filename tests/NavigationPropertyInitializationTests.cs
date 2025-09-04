using System.Collections.Generic;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class NavigationPropertyInitializationTests
{
    private class Blog
    {
        public int Id { get; set; }
        public ICollection<Post> Posts { get; set; } = new List<Post>();
    }

    private class Post
    {
        public int Id { get; set; }
    }

    [Fact]
    public void EnableLazyLoading_DoesNotOverwritePopulatedCollection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var blog = new Blog
        {
            Posts = new List<Post> { new Post { Id = 1 } }
        };

        blog.EnableLazyLoading(ctx);

        Assert.IsType<List<Post>>(blog.Posts);
        Assert.Single(blog.Posts);
    }
}
