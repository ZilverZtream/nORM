using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class SelectManyTests : TestBase
{
    private class Blog
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public List<Post> Posts { get; set; } = new();
    }

    private class Post
    {
        public int Id { get; set; }
        public int BlogId { get; set; }
        public string Title { get; set; } = string.Empty;
        public bool Active { get; set; }
    }

    private class Customer
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string City { get; set; } = string.Empty;
        public List<Invoice> Invoices { get; set; } = new();
    }

    private class Invoice
    {
        public int Id { get; set; }
        public int CustomerId { get; set; }
        public decimal Amount { get; set; }
    }

    public static IEnumerable<object[]> Providers()
    {
        foreach (ProviderKind provider in Enum.GetValues<ProviderKind>())
            yield return new object[] { provider };
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void SelectMany_with_navigation_property_creates_inner_join(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, parameters, elementType) = TranslateQuery<Blog, Post>(
            q => q.SelectMany(b => b.Posts),
            connection,
            provider);

        var t0 = provider.Escape("T0");
        var t1 = provider.Escape("T1");
        var blogTable = provider.Escape("Blog");
        var postTable = provider.Escape("Post");

        var postCols = string.Join(", ", new[]
        {
            $"{t1}.{provider.Escape("Id")}",
            $"{t1}.{provider.Escape("BlogId")}",
            $"{t1}.{provider.Escape("Title")}",
            $"{t1}.{provider.Escape("Active")}"
        });

        var expected = $"SELECT {postCols} FROM {blogTable} {t0} INNER JOIN {postTable} {t1} ON {t0}.{provider.Escape("Id")} = {t1}.{provider.Escape("BlogId")}";

        Assert.Equal(expected, sql);
        Assert.Empty(parameters);
        Assert.Equal(typeof(Post), elementType);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void SelectMany_with_navigation_and_result_selector(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, parameters, elementType) = TranslateQuery<Blog, object>(
            q => q.SelectMany(b => b.Posts, (b, p) => new { BlogName = b.Name, PostTitle = p.Title }),
            connection,
            provider);

        var t0 = provider.Escape("T0");
        var t1 = provider.Escape("T1");
        var blogTable = provider.Escape("Blog");
        var postTable = provider.Escape("Post");

        // The result selector should create a projection
        Assert.Contains("SELECT", sql);
        Assert.Contains("INNER JOIN", sql);
        Assert.Contains(provider.Escape("Name"), sql);
        Assert.Contains(provider.Escape("Title"), sql);
        Assert.Empty(parameters);
    }

    [Theory]
    [MemberData(nameof(Providers))]
    public void SelectMany_cross_join_without_navigation(ProviderKind providerKind)
    {
        var setup = CreateProvider(providerKind);
        using var connection = setup.Connection;
        var provider = setup.Provider;

        var (sql, parameters, elementType) = TranslateQuery<Blog, Blog>(
            q => q.SelectMany(b => q),
            connection,
            provider);

        var t0 = provider.Escape("T0");
        var t1 = provider.Escape("T1");
        var blogTable = provider.Escape("Blog");

        Assert.Contains("CROSS JOIN", sql);
        Assert.Contains(blogTable, sql);
        Assert.Empty(parameters);
        Assert.Equal(typeof(Blog), elementType);
    }

    [Fact]
    public void SelectMany_with_navigation_executes_correctly()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Blog(Id INTEGER, Name TEXT);" +
                "CREATE TABLE Post(Id INTEGER, BlogId INTEGER, Title TEXT, Active INTEGER);" +
                "INSERT INTO Blog VALUES(1,'Tech Blog');" +
                "INSERT INTO Blog VALUES(2,'Food Blog');" +
                "INSERT INTO Post VALUES(1,1,'First Post',1);" +
                "INSERT INTO Post VALUES(2,1,'Second Post',1);" +
                "INSERT INTO Post VALUES(3,2,'Food Post',0);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = ctx.Query<Blog>()
            .SelectMany(b => b.Posts)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Contains(results, p => p.Title == "First Post");
        Assert.Contains(results, p => p.Title == "Second Post");
        Assert.Contains(results, p => p.Title == "Food Post");
    }

    [Fact]
    public void SelectMany_with_navigation_and_result_selector_executes_correctly()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Blog(Id INTEGER, Name TEXT);" +
                "CREATE TABLE Post(Id INTEGER, BlogId INTEGER, Title TEXT, Active INTEGER);" +
                "INSERT INTO Blog VALUES(1,'Tech Blog');" +
                "INSERT INTO Blog VALUES(2,'Food Blog');" +
                "INSERT INTO Post VALUES(1,1,'First Post',1);" +
                "INSERT INTO Post VALUES(2,1,'Second Post',1);" +
                "INSERT INTO Post VALUES(3,2,'Food Post',0);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = ctx.Query<Blog>()
            .SelectMany(b => b.Posts, (b, p) => new { BlogName = b.Name, PostTitle = p.Title })
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Contains(results, r => r.BlogName == "Tech Blog" && r.PostTitle == "First Post");
        Assert.Contains(results, r => r.BlogName == "Tech Blog" && r.PostTitle == "Second Post");
        Assert.Contains(results, r => r.BlogName == "Food Blog" && r.PostTitle == "Food Post");
    }

    [Fact]
    public void SelectMany_with_where_clause_on_nested_collection()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Blog(Id INTEGER, Name TEXT);" +
                "CREATE TABLE Post(Id INTEGER, BlogId INTEGER, Title TEXT, Active INTEGER);" +
                "INSERT INTO Blog VALUES(1,'Tech Blog');" +
                "INSERT INTO Blog VALUES(2,'Food Blog');" +
                "INSERT INTO Post VALUES(1,1,'First Post',1);" +
                "INSERT INTO Post VALUES(2,1,'Second Post',1);" +
                "INSERT INTO Post VALUES(3,2,'Food Post',0);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = ctx.Query<Blog>()
            .SelectMany(b => b.Posts.Where(p => p.Active))
            .ToList();

        Assert.Equal(2, results.Count);
        Assert.All(results, p => Assert.True(p.Active));
    }

    [Fact]
    public void SelectMany_flattens_customers_invoices_in_london()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Customer(Id INTEGER, Name TEXT, City TEXT);" +
                "CREATE TABLE Invoice(Id INTEGER, CustomerId INTEGER, Amount REAL);" +
                "INSERT INTO Customer VALUES(1,'Alice','London');" +
                "INSERT INTO Customer VALUES(2,'Bob','Paris');" +
                "INSERT INTO Customer VALUES(3,'Charlie','London');" +
                "INSERT INTO Invoice VALUES(1,1,100.00);" +
                "INSERT INTO Invoice VALUES(2,1,150.00);" +
                "INSERT INTO Invoice VALUES(3,2,200.00);" +
                "INSERT INTO Invoice VALUES(4,3,250.00);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        var results = ctx.Query<Customer>()
            .Where(c => c.City == "London")
            .SelectMany(c => c.Invoices)
            .ToList();

        Assert.Equal(3, results.Count);
        Assert.Contains(results, i => i.CustomerId == 1 && i.Amount == 100);
        Assert.Contains(results, i => i.CustomerId == 1 && i.Amount == 150);
        Assert.Contains(results, i => i.CustomerId == 3 && i.Amount == 250);
        Assert.DoesNotContain(results, i => i.CustomerId == 2);
    }

    [Fact]
    public void SelectMany_with_transparent_identifier_in_chained_select()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using (var cmd = cn.CreateCommand())
        {
            cmd.CommandText =
                "CREATE TABLE Blog(Id INTEGER, Name TEXT);" +
                "CREATE TABLE Post(Id INTEGER, BlogId INTEGER, Title TEXT, Active INTEGER);" +
                "INSERT INTO Blog VALUES(1,'Tech Blog');" +
                "INSERT INTO Post VALUES(1,1,'First Post',1);";
            cmd.ExecuteNonQuery();
        }

        using var ctx = new DbContext(cn, new SqliteProvider());
        // This creates a transparent identifier
        var results = ctx.Query<Blog>()
            .SelectMany(b => b.Posts, (b, p) => new { Blog = b, Post = p })
            .Select(x => new { x.Blog.Name, x.Post.Title })
            .ToList();

        Assert.Single(results);
        Assert.Equal("Tech Blog", results[0].Name);
        Assert.Equal("First Post", results[0].Title);
    }
}
