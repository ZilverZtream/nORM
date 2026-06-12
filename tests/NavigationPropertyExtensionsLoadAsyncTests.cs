using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Configuration;
using nORM.Core;
using nORM.Enterprise;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using MigrationRunners = nORM.Migration;
using Xunit;

#nullable enable

namespace nORM.Tests;

[Xunit.Trait("Category", "Fast")]
public class NavigationPropertyExtensionsLoadAsyncTests
{
    private static (SqliteConnection Cn, DbContext Ctx) MakeNavDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CovBoost_Author (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '');
            CREATE TABLE CovBoost_Book   (Id INTEGER PRIMARY KEY AUTOINCREMENT, AuthorId INTEGER NOT NULL, Title TEXT NOT NULL DEFAULT '');
            INSERT INTO CovBoost_Author (Name) VALUES ('Tolkien');
            INSERT INTO CovBoost_Book   (AuthorId, Title) VALUES (1, 'The Hobbit');
            INSERT INTO CovBoost_Book   (AuthorId, Title) VALUES (1, 'LOTR');";
        cmd.ExecuteNonQuery();
        // the explicit relation (AuthorId) instead of the inferred convention (CovAuthorId).
        var opts = new DbContextOptions
        {
            OnModelCreating = mb =>
            {
                mb.Entity<CovAuthor>()
                    .HasMany(a => a.Books)
                    .WithOne()
                    .HasForeignKey(b => b.AuthorId, a => a.Id);
            }
        };
        var ctx = new DbContext(cn, new SqliteProvider(), opts);
        // Force mapping registration so relations are populated before tests run.
        _ = ctx.GetMapping(typeof(CovAuthor));
        _ = ctx.GetMapping(typeof(CovBook));
        return (cn, ctx);
    }

    [Fact]
    public async Task LoadAsync_Collection_NoNavContext_Throws()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = new CovAuthor { Id = 1, Name = "Test" };
        await Assert.ThrowsAsync<NormUsageException>(() =>
            author.LoadAsync(a => a.Books));
    }

    [Fact]
    public async Task LoadAsync_CollectionNav_WithNavContext_LoadsBooksFromDb()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = await ctx.Query<CovAuthor>().FirstAsync();
        Assert.NotNull(author);

        // ProcessEntity calls EnableLazyLoading with entity typed as object, creating
        // a NavContext with EntityType=typeof(object). Replace with correct EntityType.
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(
            author, new NavigationContext(ctx, typeof(CovAuthor)));

        await author.LoadAsync(a => a.Books);
        Assert.NotEmpty(author.Books);
    }

    [Fact]
    public async Task LoadAsync_AlreadyLoaded_DoesNotReload()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = await ctx.Query<CovAuthor>().FirstAsync();
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(
            author, new NavigationContext(ctx, typeof(CovAuthor)));

        // First load
        await author.LoadAsync(a => a.Books);
        Assert.NotEmpty(author.Books); // verify first load worked

        // Replace Books with empty list to detect if second call re-loads
        author.Books = new List<CovBook>();

        await author.LoadAsync(a => a.Books);

        // Books should still be empty (no re-load)
        Assert.Empty(author.Books);
    }

    [Fact]
    public void LoadNavigationProperty_Sync_LoadsCollection()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = ctx.Query<CovAuthor>().First();
        var booksProperty = typeof(CovAuthor).GetProperty("Books")!;

        // Replace the navContext (created with EntityType=object by ProcessEntity) with one
        // that has the correct EntityType so GetMapping resolves the Books relation.
        var navContext = new NavigationContext(ctx, typeof(CovAuthor));
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(author, navContext);

        // Sync wrapper over async
        NavigationPropertyExtensions.LoadNavigationProperty(author, booksProperty, navContext, CancellationToken.None);

        Assert.NotEmpty(author.Books);
    }

    [Fact]
    public async Task LoadAsync_ReferenceNav_ThrowsWithoutNavContext()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var entity = new CovRefEntity { Id = 1, Name = "Test" };
        // No nav context
        await Assert.ThrowsAsync<NormUsageException>(() =>
            entity.LoadAsync(e => e.LazyRef));
    }

    [Fact]
    public async Task IsLoaded_AfterLoadAsync_ReturnsTrue()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = await ctx.Query<CovAuthor>().FirstAsync();
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(
            author, new NavigationContext(ctx, typeof(CovAuthor)));

        Assert.False(author.IsLoaded(a => a.Books));
        await author.LoadAsync(a => a.Books);
        Assert.True(author.IsLoaded(a => a.Books));
    }

    [Fact]
    public async Task LoadAsync_CollectionNav_ICollectionOverload_Works()
    {
        var (cn, ctx) = MakeNavDb();
        using var _cn = cn; using var _ctx = ctx;

        var author = await ctx.Query<CovAuthor>().FirstAsync();
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(
            author, new NavigationContext(ctx, typeof(CovAuthor)));

        // to the ICollection<TProperty> overload (more specific than TProperty? overload).
        await author.LoadAsync(a => a.Books);

        Assert.NotEmpty(author.Books);
    }
}

// Covers: CreateILMaterializer parameterized-ctor path, nullable enum IL path,
//         enum IL path, CreateSyncMaterializer generic overload, fast materializer
