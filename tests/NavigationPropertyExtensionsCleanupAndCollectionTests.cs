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
public class NavigationPropertyExtensionsCleanupAndCollectionTests
{
    // Helper: build a pre-loaded LazyNavigationCollection backed by a real List<CovBook>
    // No DB queries are made because IsLoaded("Books") is true from the start.
    private static (LazyNavigationCollection<CovBook> Lazy, CovAuthor Author, List<CovBook> Books) MakePreloaded()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());

        var b1 = new CovBook { Id = 1, AuthorId = 1, Title = "Alpha" };
        var b2 = new CovBook { Id = 2, AuthorId = 1, Title = "Beta" };
        var books = new List<CovBook> { b1, b2 };

        var author = new CovAuthor { Id = 1, Name = "Auth" };
        author.Books = books; // pre-populate the property

        var booksProperty = typeof(CovAuthor).GetProperty("Books")!;
        var navCtx = new NavigationContext(ctx, typeof(CovAuthor));
        navCtx.MarkAsLoaded("Books"); // mark as loaded so no DB hit occurs

        var lazy = new LazyNavigationCollection<CovBook>(author, booksProperty, navCtx);
        return (lazy, author, books);
    }

    [Fact]
    public void CleanupNavigationContext_WithRegisteredEntity_RemovesIt()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());

        var entity = new CovAuthor { Id = 99, Name = "Cleanup" };
        var navCtx = new NavigationContext(ctx, typeof(CovAuthor));
        NavigationPropertyExtensions._navigationContexts.AddOrUpdate(entity, navCtx);

        Assert.True(NavigationPropertyExtensions._navigationContexts.TryGetValue(entity, out _));
        NavigationPropertyExtensions.CleanupNavigationContext(entity);
        Assert.False(NavigationPropertyExtensions._navigationContexts.TryGetValue(entity, out _));
    }

    [Fact]
    public void CleanupNavigationContext_WithNonRegisteredEntity_IsNoOp()
    {
        var entity = new CovAuthor { Id = 88, Name = "Unregistered" };
        // Should not throw even when entity is not registered
        var ex = Record.Exception(() => NavigationPropertyExtensions.CleanupNavigationContext(entity));
        Assert.Null(ex);
    }

    [Fact]
    public void IsLoaded_WhenNoNavContext_ReturnsFalse()
    {
        var entity = new CovAuthor { Id = 77, Name = "NoCtx" };
        // Not registered in _navigationContexts
        Assert.False(entity.IsLoaded(a => a.Books));
    }

    [Fact]
    public void LazyNavCollection_Count_ReturnsItemCount()
    {
        var (lazy, _, books) = MakePreloaded();
        Assert.Equal(books.Count, lazy.Count);
    }

    [Fact]
    public void LazyNavCollection_Add_AddsItem()
    {
        var (lazy, _, books) = MakePreloaded();
        var newBook = new CovBook { Id = 3, Title = "Gamma" };
        lazy.Add(newBook);
        Assert.Equal(3, lazy.Count);
        Assert.Contains(newBook, books);
    }

    [Fact]
    public void LazyNavCollection_Clear_ClearsAllItems()
    {
        var (lazy, _, books) = MakePreloaded();
        lazy.Clear();
        Assert.Empty(lazy);
        Assert.Empty(books);
    }

    [Fact]
    public void LazyNavCollection_Contains_ReturnsTrueForExisting()
    {
        var (lazy, _, books) = MakePreloaded();
        Assert.Contains(books[0], lazy);
        Assert.DoesNotContain(new CovBook { Id = 99, Title = "Missing" }, lazy);
    }

    [Fact]
    public void LazyNavCollection_CopyTo_CopiesItems()
    {
        var (lazy, _, books) = MakePreloaded();
        var array = new CovBook[books.Count];
        lazy.CopyTo(array, 0);
        Assert.Equal(books[0], array[0]);
        Assert.Equal(books[1], array[1]);
    }

    [Fact]
    public void LazyNavCollection_Remove_RemovesItem()
    {
        var (lazy, _, books) = MakePreloaded();
        var removed = lazy.Remove(books[0]);
        Assert.True(removed);
        Assert.Single(lazy);
    }

    [Fact]
    public void LazyNavCollection_IsReadOnly_ReturnsFalse()
    {
        var (lazy, _, _) = MakePreloaded();
        Assert.False(lazy.IsReadOnly);
    }

    [Fact]
    public void LazyNavCollection_GetEnumerator_Enumerates()
    {
        var (lazy, _, books) = MakePreloaded();
        var items = new List<CovBook>();
        foreach (var item in lazy) items.Add(item);
        Assert.Equal(books.Count, items.Count);
    }

    [Fact]
    public void LazyNavCollection_NonGenericGetEnumerator_Enumerates()
    {
        var (lazy, _, books) = MakePreloaded();
        int count = 0;
        var enumerator = ((System.Collections.IEnumerable)lazy).GetEnumerator();
        while (enumerator.MoveNext()) count++;
        Assert.Equal(books.Count, count);
    }

    [Fact]
    public async Task LazyNavCollection_GetAsyncEnumerator_Enumerates()
    {
        var (lazy, _, books) = MakePreloaded();
        var items = new List<CovBook>();
        await foreach (var item in lazy) items.Add(item);
        Assert.Equal(books.Count, items.Count);
    }

    [Fact]
    public void LazyNavCollection_IndexOf_ReturnsIndex()
    {
        var (lazy, _, books) = MakePreloaded();
        Assert.Equal(0, lazy.IndexOf(books[0]));
        Assert.Equal(1, lazy.IndexOf(books[1]));
        Assert.Equal(-1, lazy.IndexOf(new CovBook { Id = 99 }));
    }

    [Fact]
    public void LazyNavCollection_Insert_InsertsAtIndex()
    {
        var (lazy, _, books) = MakePreloaded();
        var newBook = new CovBook { Id = 5, Title = "Inserted" };
        lazy.Insert(0, newBook);
        Assert.Equal(3, lazy.Count);
        Assert.Equal(newBook, lazy[0]);
    }

    [Fact]
    public void LazyNavCollection_RemoveAt_RemovesAtIndex()
    {
        var (lazy, _, books) = MakePreloaded();
        var secondBook = books[1]; // save reference before removal
        lazy.RemoveAt(0);
        Assert.Single(lazy);
        Assert.Equal(secondBook, lazy[0]);
    }

    [Fact]
    public void LazyNavCollection_Indexer_GetSet()
    {
        var (lazy, _, books) = MakePreloaded();
        var retrieved = lazy[0];
        Assert.Equal(books[0], retrieved);

        var newBook = new CovBook { Id = 10, Title = "New" };
        lazy[0] = newBook;
        Assert.Equal(newBook, lazy[0]);
    }
}

// Covers: LazyNavigationReference.SetValue, GetValueAsync (pre-loaded path),
//         implicit Task<T?> operator, NavigationContext.MarkAsUnloaded
