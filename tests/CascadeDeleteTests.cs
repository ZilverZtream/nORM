using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
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

    private class Node
    {
        [Key]
        public int Id { get; set; }
        public int ParentId { get; set; }
        public List<Node> Children { get; set; } = new();
    }

    private static TableMapping CreateNodeMapping()
    {
        var provider = new SqliteProvider();
        var pk = new Column(typeof(Node).GetProperty(nameof(Node.Id))!, provider, null);
        var fk = new Column(typeof(Node).GetProperty(nameof(Node.ParentId))!, provider, null);
        var mapping = (TableMapping)RuntimeHelpers.GetUninitializedObject(typeof(TableMapping));

        typeof(TableMapping).GetField(nameof(TableMapping.Type))!.SetValue(mapping, typeof(Node));
        typeof(TableMapping).GetField(nameof(TableMapping.Provider))!.SetValue(mapping, provider);
        typeof(TableMapping).GetField(nameof(TableMapping.EscTable))!.SetValue(mapping, "");
        typeof(TableMapping).GetField(nameof(TableMapping.Columns))!.SetValue(mapping, new[] { pk, fk });
        typeof(TableMapping).GetField(nameof(TableMapping.ColumnsByName))!
            .SetValue(mapping, new Dictionary<string, Column> { [nameof(Node.Id)] = pk, [nameof(Node.ParentId)] = fk });
        typeof(TableMapping).GetField(nameof(TableMapping.KeyColumns))!.SetValue(mapping, new[] { pk });
        typeof(TableMapping).GetField(nameof(TableMapping.Relations))!
            .SetValue(mapping, new Dictionary<string, TableMapping.Relation>());
        typeof(TableMapping).GetField(nameof(TableMapping.TphMappings))!
            .SetValue(mapping, new Dictionary<object, TableMapping>());

        var relations = (Dictionary<string, TableMapping.Relation>)typeof(TableMapping)
            .GetField(nameof(TableMapping.Relations))!
            .GetValue(mapping)!;
        var navProp = typeof(Node).GetProperty(nameof(Node.Children))!;
        relations[nameof(Node.Children)] = new TableMapping.Relation(navProp, typeof(Node), pk, fk, true);
        return mapping;
    }

    [Fact]
    public void CascadeDelete_handles_cycles_without_overflow()
    {
        var mapping = CreateNodeMapping();
        var tracker = new ChangeTracker(new DbContextOptions());

        var a = new Node { Id = 1 };
        var b = new Node { Id = 2 };
        a.Children.Add(b);
        b.Children.Add(a);

        tracker.Track(a, EntityState.Unchanged, mapping);
        tracker.Track(b, EntityState.Unchanged, mapping);

        tracker.Remove(a, true);

        Assert.Empty(tracker.Entries);
    }

    [Fact]
    public void CascadeDelete_respects_max_depth()
    {
        var mapping = CreateNodeMapping();
        var tracker = new ChangeTracker(new DbContextOptions());

        var maxDepth = (int)typeof(ChangeTracker)
            .GetField("MaxCascadeDepth", BindingFlags.NonPublic | BindingFlags.Static)!
            .GetValue(null)!;

        var root = new Node { Id = 0 };
        var current = root;
        var nodes = new List<Node> { root };

        int nextId = 1;
        for (int i = 0; i < maxDepth + 5; i++)
        {
            var child = new Node { Id = nextId++, ParentId = current.Id };
            current.Children.Add(child);
            nodes.Add(child);
            tracker.Track(child, EntityState.Unchanged, mapping);
            current = child;
        }

        tracker.Track(root, EntityState.Unchanged, mapping);
        tracker.Remove(root, true);

        var expectedRemaining = nodes.Count - (maxDepth + 1);
        Assert.Equal(expectedRemaining, tracker.Entries.Count());
    }
}
