using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class RelationshipConfigurationTests
{
    private class Blog
    {
        public int Key { get; set; }
        public ICollection<Post> Posts { get; set; } = new List<Post>();
        public BlogProfile? Profile { get; set; }
    }

    private class Post
    {
        [Key]
        public int Id { get; set; }
        public int ParentKey { get; set; }
        public Blog? Parent { get; set; }
    }

    private class BlogProfile
    {
        [Key]
        public int Id { get; set; }
        public int BlogKey { get; set; }
        public Blog? Blog { get; set; }
    }

    private class TenantAccount
    {
        public int TenantId { get; set; }
        public int AccountNo { get; set; }
        public TenantAccountProfile? Profile { get; set; }
    }

    private class TenantAccountProfile
    {
        [Key]
        public int Id { get; set; }
        public int TenantId { get; set; }
        public int AccountNo { get; set; }
        public TenantAccount? Account { get; set; }
    }

    private class TenantOrder
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public ICollection<TenantOrderLine> Lines { get; set; } = new List<TenantOrderLine>();
    }

    private class TenantOrderLine
    {
        public int TenantId { get; set; }
        public int OrderId { get; set; }
        public int LineNo { get; set; }
    }

    private class Author
    {
        [Key]
        public int Id { get; set; }
        [NotMapped]
        public ICollection<Book> Books { get; set; } = new List<Book>();
    }

    private class Book
    {
        [Key]
        public int Id { get; set; }
        [NotMapped]
        public ICollection<Author> Authors { get; set; } = new List<Author>();
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

    [Fact]
    public void Fluent_relationship_configuration_with_explicit_principal_key_is_used()
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
                    .HasForeignKey(p => p.ParentKey, b => b.Key);
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

    [Fact]
    public void Fluent_one_to_one_relationship_configuration_is_used()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Blog>()
                    .HasKey(b => b.Key)
                    .HasOne(b => b.Profile)
                    .WithOne(p => p.Blog)
                    .HasForeignKey(p => p.BlogKey, b => b.Key, cascadeDelete: false);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var blogMap = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(Blog) })!;

        Assert.True(blogMap.Relations.ContainsKey(nameof(Blog.Profile)));
        var rel = blogMap.Relations[nameof(Blog.Profile)];
        Assert.Equal(typeof(BlogProfile), rel.DependentType);
        Assert.Equal("Key", rel.PrincipalKey.PropName);
        Assert.Equal("BlogKey", rel.ForeignKey.PropName);
        Assert.False(rel.CascadeDelete);
    }

    [Fact]
    public void Fluent_composite_one_to_one_relationship_configuration_preserves_ordered_key_pairs()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<TenantAccount>().HasKey(a => new { a.TenantId, a.AccountNo });
                mb.Entity<TenantAccountProfile>().HasKey(p => p.Id);
                mb.Entity<TenantAccount>()
                    .HasOne(a => a.Profile)
                    .WithOne(p => p.Account)
                    .HasForeignKey(p => new { p.TenantId, p.AccountNo }, a => new { a.TenantId, a.AccountNo }, cascadeDelete: false);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var accountMap = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(TenantAccount) })!;

        var rel = accountMap.Relations[nameof(TenantAccount.Profile)];
        Assert.True(rel.IsComposite);
        Assert.False(rel.CascadeDelete);
        Assert.Equal(typeof(TenantAccountProfile), rel.DependentType);
        Assert.Equal(new[] { "TenantId", "AccountNo" }, rel.PrincipalKeys.Select(c => c.PropName).ToArray());
        Assert.Equal(new[] { "TenantId", "AccountNo" }, rel.ForeignKeys.Select(c => c.PropName).ToArray());
    }

    [Fact]
    public void Fluent_relationship_configuration_preserves_cascade_delete_flag()
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
                    .HasForeignKey(p => p.ParentKey, b => b.Key, cascadeDelete: false);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var blogMap = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(Blog) })!;

        var rel = blogMap.Relations[nameof(Blog.Posts)];
        Assert.False(rel.CascadeDelete);
    }

    [Fact]
    public void Fluent_relationship_configuration_preserves_database_referential_actions()
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
                    .HasForeignKey(p => p.ParentKey, b => b.Key, ReferentialAction.SetNull, ReferentialAction.Restrict);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var blogMap = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(Blog) })!;

        var rel = blogMap.Relations[nameof(Blog.Posts)];
        Assert.False(rel.CascadeDelete);
        Assert.Equal(ReferentialAction.SetNull, rel.OnDelete);
        Assert.Equal(ReferentialAction.Restrict, rel.OnUpdate);
    }

    [Fact]
    public void Fluent_relationship_configuration_preserves_constraint_name()
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
                    .HasForeignKey(p => p.ParentKey, b => b.Key, constraintName: "FK_Blog_Post_ParentKey", cascadeDelete: false);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var blogMap = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(Blog) })!;

        var rel = blogMap.Relations[nameof(Blog.Posts)];
        Assert.False(rel.CascadeDelete);
        Assert.Equal("FK_Blog_Post_ParentKey", rel.ConstraintName);
    }

    [Fact]
    public void Fluent_composite_relationship_configuration_preserves_ordered_key_pairs()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<TenantOrder>().HasKey(o => new { o.TenantId, o.OrderId });
                mb.Entity<TenantOrderLine>().HasKey(l => new { l.TenantId, l.OrderId, l.LineNo });
                mb.Entity<TenantOrder>()
                    .HasMany(o => o.Lines)
                    .WithOne()
                    .HasForeignKey(l => new { l.TenantId, l.OrderId }, o => new { o.TenantId, o.OrderId }, cascadeDelete: false);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var orderMap = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(TenantOrder) })!;

        var rel = orderMap.Relations[nameof(TenantOrder.Lines)];
        Assert.True(rel.IsComposite);
        Assert.False(rel.CascadeDelete);
        Assert.Equal(new[] { "TenantId", "OrderId" }, rel.PrincipalKeys.Select(c => c.PropName).ToArray());
        Assert.Equal(new[] { "TenantId", "OrderId" }, rel.ForeignKeys.Select(c => c.PropName).ToArray());
        Assert.Equal("TenantId", rel.PrincipalKey.PropName);
        Assert.Equal("TenantId", rel.ForeignKey.PropName);
    }

    [Fact]
    public void Fluent_composite_relationship_configuration_infers_matching_composite_principal_key()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<TenantOrder>().HasKey(o => new { o.TenantId, o.OrderId });
                mb.Entity<TenantOrderLine>().HasKey(l => new { l.TenantId, l.OrderId, l.LineNo });
                mb.Entity<TenantOrder>()
                    .HasMany(o => o.Lines)
                    .WithOne()
                    .HasForeignKey(l => new { l.TenantId, l.OrderId });
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var orderMap = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(TenantOrder) })!;

        var rel = orderMap.Relations[nameof(TenantOrder.Lines)];
        Assert.True(rel.IsComposite);
        Assert.Equal(new[] { "TenantId", "OrderId" }, rel.PrincipalKeys.Select(c => c.PropName).ToArray());
        Assert.Equal(new[] { "TenantId", "OrderId" }, rel.ForeignKeys.Select(c => c.PropName).ToArray());
    }

    [Fact]
    public void Fluent_many_to_many_configuration_preserves_join_table_schema()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Author>()
                    .HasMany<Book>(a => a.Books)
                    .WithMany(b => b.Authors)
                    .UsingTable("AuthorBook", "AuthorId", "BookId", schema: "aux");
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var authorMap = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(Author) })!;

        var join = Assert.Single(authorMap.ManyToManyJoins);
        Assert.Equal("AuthorBook", join.TableName);
        Assert.Equal("aux", join.SchemaName);
        Assert.Equal("\"aux\".\"AuthorBook\"", join.EscTableName);
    }

    [Fact]
    public void Fluent_many_to_many_configuration_preserves_referential_actions()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<Author>()
                    .HasMany<Book>(a => a.Books)
                    .WithMany(b => b.Authors)
                    .UsingTable(
                        "AuthorBook",
                        new[] { "AuthorId" },
                        new[] { "BookId" },
                        ReferentialAction.Cascade,
                        ReferentialAction.Cascade,
                        ReferentialAction.Restrict,
                        ReferentialAction.NoAction);
            }
        };

        using var ctx = new DbContext(cn, new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var authorMap = (TableMapping)getMapping.Invoke(ctx, new object[] { typeof(Author) })!;

        var join = Assert.Single(authorMap.ManyToManyJoins);
        Assert.Equal(ReferentialAction.Cascade, join.LeftOnDelete);
        Assert.Equal(ReferentialAction.Cascade, join.LeftOnUpdate);
        Assert.Equal(ReferentialAction.Restrict, join.RightOnDelete);
        Assert.Equal(ReferentialAction.NoAction, join.RightOnUpdate);
    }

    [Fact]
    public void Fluent_many_to_many_configuration_rejects_whitespace_join_table_schema()
    {
        var options = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                Assert.Throws<ArgumentException>(() =>
                    mb.Entity<Author>()
                        .HasMany<Book>(a => a.Books)
                        .WithMany(b => b.Authors)
                        .UsingTable("AuthorBook", "AuthorId", "BookId", schema: " "));
            }
        };

        using var ctx = new DbContext(new SqliteConnection("Data Source=:memory:"), new SqliteProvider(), options);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        _ = getMapping.Invoke(ctx, new object[] { typeof(Author) });
    }
}

