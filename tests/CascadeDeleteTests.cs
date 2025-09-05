using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class CascadeDeleteTests
{
    private class Blog
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public ICollection<Post> Posts { get; set; } = new List<Post>();
    }

    private class Post
    {
        [Key]
        [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public int Id { get; set; }
        public int BlogId { get; set; }
        public Blog? Blog { get; set; }
    }

    [Fact]
    public async Task ChangeTracker_removes_cascaded_children()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Blog(Id INTEGER PRIMARY KEY AUTOINCREMENT);";
            cmd.ExecuteNonQuery();
        }
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText = "CREATE TABLE Post(Id INTEGER PRIMARY KEY AUTOINCREMENT, BlogId INTEGER NOT NULL, FOREIGN KEY(BlogId) REFERENCES Blog(Id) ON DELETE CASCADE);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());

        var blog = new Blog();
        ctx.Add(blog);
        await ctx.SaveChangesAsync();

        var post1 = new Post { BlogId = blog.Id };
        var post2 = new Post { BlogId = blog.Id };
        ctx.Add(post1);
        ctx.Add(post2);
        blog.Posts.Add(post1);
        blog.Posts.Add(post2);
        await ctx.SaveChangesAsync();

        ctx.Remove(blog);
        await ctx.SaveChangesAsync();

        Assert.Empty(ctx.ChangeTracker.Entries);
    }
}
