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
public class LazyNavReferenceBoostTests
{
    private static (LazyNavigationReference<CovItem> Ref, CovRefEntity Entity, NavigationContext NavCtx) MakeRef()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());
        var entity = new CovRefEntity { Id = 1, Name = "RefTest" };
        var prop = typeof(CovRefEntity).GetProperty("LazyRef")!;
        var navCtx = new NavigationContext(ctx, typeof(CovRefEntity));
        var reference = new LazyNavigationReference<CovItem>(entity, prop, navCtx);
        return (reference, entity, navCtx);
    }

    [Fact]
    public void SetValue_SetsInternalValueAndMarksLoaded()
    {
        var (reference, _, navCtx) = MakeRef();
        var item = new CovItem { Id = 42, Name = "RefItem" };

        reference.SetValue(item);

        Assert.True(navCtx.IsLoaded("LazyRef"));
    }

    [Fact]
    public async Task GetValueAsync_AfterSetValue_ReturnsSameInstance()
    {
        var (reference, _, _) = MakeRef();
        var item = new CovItem { Id = 7, Name = "Loaded" };

        reference.SetValue(item);
        var result = await reference.GetValueAsync();

        Assert.Equal(item, result);
    }

    [Fact]
    public async Task ImplicitTaskOperator_ReturnsValue()
    {
        var (reference, _, _) = MakeRef();
        var item = new CovItem { Id = 5, Name = "Implicit" };
        reference.SetValue(item);

        Task<CovItem?> task = reference; // implicit operator
        var result = await task;

        Assert.Equal(item, result);
    }

    [Fact]
    public void SetValue_Null_SetsNullAndMarksLoaded()
    {
        var (reference, _, navCtx) = MakeRef();
        reference.SetValue(null);
        Assert.True(navCtx.IsLoaded("LazyRef"));
    }

    [Fact]
    public void NavigationContext_MarkAsUnloaded_RemovesFromLoaded()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());
        var navCtx = new NavigationContext(ctx, typeof(CovAuthor));

        navCtx.MarkAsLoaded("Books");
        Assert.True(navCtx.IsLoaded("Books"));

        navCtx.MarkAsUnloaded("Books");
        Assert.False(navCtx.IsLoaded("Books"));
    }

    [Fact]
    public void NavigationContext_Dispose_ClearsLoadedProperties()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        var ctx = new DbContext(cn, new SqliteProvider());
        var navCtx = new NavigationContext(ctx, typeof(CovAuthor));
        navCtx.MarkAsLoaded("Books");
        navCtx.Dispose();
        // After dispose, IsLoaded should return false
        Assert.False(navCtx.IsLoaded("Books"));
    }
}

// Covers: CacheStats getter, SchemaCacheStats getter, CreateMaterializer<T> generic,
//         CreateSchemaAwareMaterializer, ConvertDbValue null/nullable/enum paths
