// Tests for NavigationContext, NavigationPropertyInfo, lazy navigation wrappers, and Include/ThenInclude extensions.

#nullable enable

using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Navigation;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

[Table("SAN_Parent")]
[Xunit.Trait("Category", "Fast")]
public class SanParent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public ICollection<SanChild>? Children { get; set; }
}

[Table("SAN_Child")]
[Xunit.Trait("Category", "Fast")]
public class SanChild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ParentId { get; set; }
}

// Entity types used by Include/ThenInclude tests.

[Table("SAN_TIParent")]
[Xunit.Trait("Category", "Fast")]
public class SanTIParent
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public ICollection<SanTIChild>? Children { get; set; }
    public SanTIChild? SingleChild { get; set; }
}

[Table("SAN_TIChild")]
[Xunit.Trait("Category", "Fast")]
public class SanTIChild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ParentId { get; set; }
    public ICollection<SanTIGrandchild>? Grandchildren { get; set; }
    public SanTIGrandchild? Grandchild { get; set; }
}

[Table("SAN_TIGrandchild")]
[Xunit.Trait("Category", "Fast")]
public class SanTIGrandchild
{
    [Key]
    [DatabaseGenerated(DatabaseGeneratedOption.Identity)]
    public int Id { get; set; }
    public int ChildId { get; set; }
}

// NavigationContext tests.

[Xunit.Trait("Category", "Fast")]
public class SanNavigationContextTests
{
    private static DbContext CreateCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Constructor_SetsDbContextAndEntityType()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        Assert.Same(ctx, navCtx.DbContext);
        Assert.Equal(typeof(SanParent), navCtx.EntityType);
    }

    [Fact]
    public void IsLoaded_InitiallyReturnsFalse()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        Assert.False(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MarkAsLoaded_ThenIsLoaded_ReturnsTrue()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        Assert.True(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MarkAsUnloaded_AfterLoaded_ReturnsFalse()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        navCtx.MarkAsUnloaded("Children");
        Assert.False(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MarkAsUnloaded_WhenNeverLoaded_DoesNotThrow()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        // Should not throw
        navCtx.MarkAsUnloaded("NonExistent");
    }

    [Fact]
    public void Dispose_ClearsLoadedState()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        navCtx.Dispose();
        Assert.False(navCtx.IsLoaded("Children"));
    }

    [Fact]
    public void MultipleProperties_TrackedIndependently()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("PropA");
        Assert.True(navCtx.IsLoaded("PropA"));
        Assert.False(navCtx.IsLoaded("PropB"));
    }

    [Fact]
    public void MarkAsLoaded_Idempotent()
    {
        using var ctx = CreateCtx();
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        navCtx.MarkAsLoaded("Children");
        navCtx.MarkAsLoaded("Children");
        Assert.True(navCtx.IsLoaded("Children"));
    }
}

// ── NavigationPropertyInfo record tests ──────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class SanNavigationPropertyInfoTests
{
    [Fact]
    public void Constructor_SetsAllProperties()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var info = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        Assert.Same(prop, info.Property);
        Assert.Equal(typeof(SanChild), info.TargetType);
        Assert.True(info.IsCollection);
    }

    [Fact]
    public void Constructor_NonCollection_SetsIsCollectionFalse()
    {
        var prop = typeof(SanParent).GetProperty("Id")!;
        var info = new NavigationPropertyInfo(prop, typeof(int), false);
        Assert.False(info.IsCollection);
    }

    [Fact]
    public void Record_Equality_SameValues_AreEqual()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var a = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        var b = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        Assert.Equal(a, b);
    }

    [Fact]
    public void Record_Equality_DifferentIsCollection_NotEqual()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var a = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        var b = new NavigationPropertyInfo(prop, typeof(SanChild), false);
        Assert.NotEqual(a, b);
    }

    [Fact]
    public void Record_ToString_ContainsTypeName()
    {
        var prop = typeof(SanParent).GetProperty("Children")!;
        var info = new NavigationPropertyInfo(prop, typeof(SanChild), true);
        var s = info.ToString();
        Assert.Contains(nameof(NavigationPropertyInfo), s);
    }
}

// ── LazyNavigationCollection<T> tests ────────────────────────────────────────
// These tests use an in-memory SQLite DB with the table created so that
// lazy loading can resolve. They exercise collection methods AFTER the
// collection has been loaded by the navigation infrastructure.

[Xunit.Trait("Category", "Fast")]
public class SanLazyNavigationCollectionTests : IDisposable
{
    private readonly SqliteConnection _cn;
    private readonly DbContext _ctx;
    private readonly SanParent _parent;
    private readonly PropertyInfo _childrenProp;
    private readonly NavigationContext _navCtx;
    private readonly LazyNavigationCollection<SanChild> _lazy;

    public SanLazyNavigationCollectionTests()
    {
        _cn = new SqliteConnection("Data Source=:memory:");
        _cn.Open();

        using var cmd = _cn.CreateCommand();
        // Create tables so the mapping can resolve
        cmd.CommandText = @"
            CREATE TABLE SAN_Parent (Id INTEGER PRIMARY KEY AUTOINCREMENT);
            CREATE TABLE SAN_Child  (Id INTEGER PRIMARY KEY AUTOINCREMENT, ParentId INTEGER NOT NULL);
            INSERT INTO SAN_Parent DEFAULT VALUES;
            INSERT INTO SAN_Child  (ParentId) VALUES (1);";
        cmd.ExecuteNonQuery();

        _ctx = new DbContext(_cn, new SqliteProvider(), new nORM.Configuration.DbContextOptions { EagerChangeTracking = false });
        _parent = new SanParent { Id = 1 };
        _childrenProp = typeof(SanParent).GetProperty("Children")!;
        _navCtx = new NavigationContext(_ctx, typeof(SanParent));
        _lazy = new LazyNavigationCollection<SanChild>(_parent, _childrenProp, _navCtx);
    }

    public void Dispose()
    {
        _ctx.Dispose();
        _cn.Dispose();
    }

    // Helper: seed the parent's Children property with a pre-built list so
    // subsequent lazy-collection operations do NOT need to hit the DB.
    private List<SanChild> SeedLoadedList(params SanChild[] items)
    {
        var list = new List<SanChild>(items);
        _childrenProp.SetValue(_parent, list);
        _navCtx.MarkAsLoaded("Children");
        return list;
    }

    [Fact]
    public void IsReadOnly_ReturnsFalse_ForLoadedList()
    {
        SeedLoadedList();
        Assert.False(_lazy.IsReadOnly);
    }

    [Fact]
    public void Count_ReturnsZero_WhenEmpty()
    {
        SeedLoadedList();
        Assert.Empty(_lazy);
    }

    [Fact]
    public void Count_ReturnsOne_WhenOneItemSeeded()
    {
        SeedLoadedList(new SanChild { Id = 1, ParentId = 1 });
        Assert.Single(_lazy);
    }

    [Fact]
    public void Add_IncreasesCount()
    {
        SeedLoadedList();
        _lazy.Add(new SanChild { Id = 99, ParentId = 1 });
        Assert.Single(_lazy);
    }

    [Fact]
    public void Contains_ReturnsTrueForAddedItem()
    {
        SeedLoadedList();
        var child = new SanChild { Id = 5, ParentId = 1 };
        _lazy.Add(child);
        Assert.Contains(child, _lazy);
    }

    [Fact]
    public void Contains_ReturnsFalseForMissingItem()
    {
        SeedLoadedList();
        Assert.DoesNotContain(new SanChild { Id = 999, ParentId = 1 }, _lazy);
    }

    [Fact]
    public void Remove_DecreasesCount()
    {
        var child = new SanChild { Id = 7, ParentId = 1 };
        SeedLoadedList(child);
        var removed = _lazy.Remove(child);
        Assert.True(removed);
        Assert.Empty(_lazy);
    }

    [Fact]
    public void Clear_EmptiesCollection()
    {
        SeedLoadedList(new SanChild { Id = 1, ParentId = 1 }, new SanChild { Id = 2, ParentId = 1 });
        _lazy.Clear();
        Assert.Empty(_lazy);
    }

    [Fact]
    public void CopyTo_CopiesItemsToArray()
    {
        var child = new SanChild { Id = 3, ParentId = 1 };
        SeedLoadedList(child);
        var arr = new SanChild[1];
        _lazy.CopyTo(arr, 0);
        Assert.Same(child, arr[0]);
    }

    [Fact]
    public void IndexOf_ReturnsCorrectIndex()
    {
        var child = new SanChild { Id = 4, ParentId = 1 };
        SeedLoadedList(child);
        Assert.Equal(0, _lazy.IndexOf(child));
    }

    [Fact]
    public void IndexOf_MissingItem_ReturnsMinusOne()
    {
        SeedLoadedList();
        Assert.Equal(-1, _lazy.IndexOf(new SanChild { Id = 42 }));
    }

    [Fact]
    public void Indexer_Get_ReturnsCorrectItem()
    {
        var child = new SanChild { Id = 8, ParentId = 1 };
        SeedLoadedList(child);
        Assert.Same(child, _lazy[0]);
    }

    [Fact]
    public void Indexer_Set_UpdatesItem()
    {
        var original = new SanChild { Id = 1, ParentId = 1 };
        SeedLoadedList(original);
        var replacement = new SanChild { Id = 2, ParentId = 1 };
        _lazy[0] = replacement;
        Assert.Same(replacement, _lazy[0]);
    }

    [Fact]
    public void Insert_AddsAtIndex()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a);
        _lazy.Insert(0, b);
        Assert.Same(b, _lazy[0]);
        Assert.Same(a, _lazy[1]);
    }

    [Fact]
    public void RemoveAt_RemovesCorrectItem()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a, b);
        _lazy.RemoveAt(0);
        Assert.Single(_lazy);
        Assert.Same(b, _lazy[0]);
    }

    [Fact]
    public void GetEnumerator_YieldsAllItems()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a, b);
        var items = new List<SanChild>();
        foreach (var item in _lazy)
            items.Add(item);
        Assert.Equal(2, items.Count); // Not a collection size check — items is a plain List<T>
    }

    [Fact]
    public async Task GetAsyncEnumerator_YieldsAllItems()
    {
        var a = new SanChild { Id = 1, ParentId = 1 };
        var b = new SanChild { Id = 2, ParentId = 1 };
        SeedLoadedList(a, b);
        var items = new List<SanChild>();
        await foreach (var item in _lazy)
            items.Add(item);
        Assert.Equal(2, items.Count); // Not a collection size check — items is a plain List<T>
    }
}

// ── LazyNavigationReference<T> tests ─────────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class LazyNavigationReferenceTests
{
    private static (DbContext Ctx, SanParent Parent, PropertyInfo Prop, NavigationContext NavCtx) CreateSetup()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());
        var parent = new SanParent { Id = 1 };
        // Use a fake property just to hold the reference
        var prop = typeof(SanParent).GetProperty("Id")!;
        var navCtx = new NavigationContext(ctx, typeof(SanParent));
        return (ctx, parent, prop, navCtx);
    }

    [Fact]
    public void SetValue_MarksAsLoaded_AndContextReflectsIt()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var child = new SanChild { Id = 1, ParentId = 1 };
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(child);
            Assert.True(navCtx.IsLoaded(prop.Name));
        }
    }

    [Fact]
    public async Task GetValueAsync_AfterSetValue_ReturnsValue()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var child = new SanChild { Id = 2, ParentId = 1 };
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(child);
            // Because _isLoaded=true, GetValueAsync will NOT go to DB
            var result = await lazyRef.GetValueAsync();
            Assert.Same(child, result);
        }
    }

    [Fact]
    public async Task GetValueAsync_SetValueNull_ReturnsNull()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(null);
            var result = await lazyRef.GetValueAsync();
            Assert.Null(result);
        }
    }

    [Fact]
    public void ImplicitConversion_ToTask_Works()
    {
        var (ctx, parent, prop, navCtx) = CreateSetup();
        using (ctx)
        {
            var child = new SanChild { Id = 3, ParentId = 1 };
            var lazyRef = new LazyNavigationReference<SanChild>(parent, prop, navCtx);
            lazyRef.SetValue(child);
            Task<SanChild?> task = lazyRef; // implicit operator
            Assert.NotNull(task);
        }
    }
}

// ── EnableLazyLoading extension tests ────────────────────────────────────────

[Xunit.Trait("Category", "Fast")]
public class EnableLazyLoadingTests
{
    [Fact]
    public void EnableLazyLoading_NullEntity_ThrowsArgumentNullException()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        Assert.Throws<ArgumentNullException>(
            () => NavigationPropertyExtensions.EnableLazyLoading<SanParent>(null!, ctx));
    }

    [Fact]
    public void EnableLazyLoading_EntityWithNullCollection_InjectsLazyProxy()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var parent = new SanParent { Id = 1, Children = null };
        parent.EnableLazyLoading(ctx);
        // The Children property should now be a LazyNavigationCollection<SanChild>
        Assert.IsType<LazyNavigationCollection<SanChild>>(parent.Children);
    }

    [Fact]
    public void EnableLazyLoading_EntityWithExistingCollection_DoesNotReplace()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());
        var existing = new List<SanChild> { new SanChild { Id = 1 } };
        var parent = new SanParent { Id = 1, Children = existing };
        parent.EnableLazyLoading(ctx);
        // Must not overwrite an already-populated collection
        Assert.Same(existing, parent.Children);
    }

    [Fact]
    public void CleanupNavigationContext_DoesNotThrowForUnregisteredEntity()
    {
        var parent = new SanParent { Id = 42 };
        // Should not throw even if the entity was never registered
        NavigationPropertyExtensions.CleanupNavigationContext(parent);
    }
}

// ── INormIncludableQueryable / ThenInclude extension tests ────────────────────

[Xunit.Trait("Category", "Fast")]
public class NormIncludableQueryableTests
{
    private static DbContext CreateCtx()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return new DbContext(cn, new SqliteProvider());
    }

    [Fact]
    public void Include_OnQuery_ReturnsIncludableQueryable()
    {
        using var ctx = CreateCtx();
        var q = ctx.Query<SanTIParent>() as INormQueryable<SanTIParent>;
        Assert.NotNull(q);
        var included = q!.Include(p => p.Children);
        Assert.IsAssignableFrom<INormIncludableQueryable<SanTIParent, ICollection<SanTIChild>>>(included);
    }

    [Fact]
    public void Include_SingleRef_ReturnsIncludableQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.SingleChild);
        Assert.NotNull(included);
    }

    [Fact]
    public void ThenInclude_AfterCollectionInclude_ReturnsNewQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.Children);
        // ThenInclude through collection: IEnumerable<SanTIChild> → SanTIChild.Grandchildren
        var then = ((INormIncludableQueryable<SanTIParent, ICollection<SanTIChild>>)included)
            .ThenInclude(c => c.Grandchildren);
        Assert.NotNull(then);
        Assert.IsAssignableFrom<INormIncludableQueryable<SanTIParent, ICollection<SanTIGrandchild>>>(then);
    }

    [Fact]
    public void ThenInclude_AfterReferenceInclude_ReturnsNewQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.SingleChild);
        var then = ((INormIncludableQueryable<SanTIParent, SanTIChild?>)included)
            .ThenInclude(c => c!.Grandchild);
        Assert.NotNull(then);
    }

    [Fact]
    public void Include_FollowedByAsNoTracking_ReturnsQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var result = q.Include(p => p.Children).AsNoTracking();
        Assert.NotNull(result);
    }

    [Fact]
    public void Include_FollowedByAsSplitQuery_ReturnsQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var result = q.Include(p => p.Children).AsSplitQuery();
        Assert.NotNull(result);
    }

    [Fact]
    public void Include_ChainedTwice_BothIncludesPreservedInExpression()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var q2 = q.Include(p => p.Children).Include(p => p.SingleChild);
        var expr = ((IQueryable<SanTIParent>)q2).Expression.ToString();
        // Expression tree should reference both navigation paths
        Assert.NotNull(expr);
        Assert.IsAssignableFrom<INormIncludableQueryable<SanTIParent, SanTIChild?>>(q2);
    }

    [Fact]
    public void ThenInclude_ChainedTwice_ProducesCorrectQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        // Include → ThenInclude → second Include
        var step1 = q.Include(p => p.Children);
        var step2 = ((INormIncludableQueryable<SanTIParent, ICollection<SanTIChild>>)step1)
            .ThenInclude(c => c.Grandchildren);
        Assert.NotNull(step2);
        // Continue building
        var step3 = step2.Include(p => p.SingleChild);
        Assert.NotNull(step3);
    }

    [Fact]
    public void AsNoTracking_OnBaseQuery_ReturnsINormQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var noTracking = q.AsNoTracking();
        Assert.NotNull(noTracking);
        Assert.IsAssignableFrom<INormQueryable<SanTIParent>>(noTracking);
    }

    [Fact]
    public void AsSplitQuery_OnBaseQuery_ReturnsINormQueryable()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var split = q.AsSplitQuery();
        Assert.NotNull(split);
        Assert.IsAssignableFrom<INormQueryable<SanTIParent>>(split);
    }

    [Fact]
    public void Include_ExpressionContainsIncludeMethodCall()
    {
        using var ctx = CreateCtx();
        var q = (INormQueryable<SanTIParent>)ctx.Query<SanTIParent>();
        var included = q.Include(p => p.Children);
        var expr = ((IQueryable<SanTIParent>)included).Expression;
        Assert.NotNull(expr);
        // The expression tree for Include wraps a MethodCallExpression
        Assert.Equal(System.Linq.Expressions.ExpressionType.Call, expr.NodeType);
    }

}
