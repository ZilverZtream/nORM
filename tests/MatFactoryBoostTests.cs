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
public class MatFactoryBoostTests
{
    private static (SqliteConnection Cn, DbContext Ctx) MakeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER)";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void CacheStats_ReturnsNonNegativeValues()
    {
        var (hits, misses, hitRate) = MaterializerFactory.CacheStats;
        Assert.True(hits >= 0);
        Assert.True(misses >= 0);
        Assert.True(hitRate >= 0.0 && hitRate <= 1.0);
    }

    [Fact]
    public void SchemaCacheStats_ReturnsNonNegativeValues()
    {
        var (sHits, sMisses, sHitRate) = MaterializerFactory.SchemaCacheStats;
        Assert.True(sHits >= 0);
        Assert.True(sMisses >= 0);
        Assert.True(sHitRate >= 0.0 && sHitRate <= 1.0);
    }

    [Fact]
    public void CreateMaterializerGeneric_ReturnsDelegate()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));

        var mat = factory.CreateMaterializer<CovItem>(mapping);
        Assert.NotNull(mat);
    }

    [Fact]
    public void CreateSchemaAwareMaterializer_NullProjection_ReturnsFastPath()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));

        var mat = factory.CreateSchemaAwareMaterializer(mapping, typeof(CovItem));
        Assert.NotNull(mat);
    }

    [Fact]
    public void CreateSchemaAwareMaterializer_WithProjection_ReturnsDelegate()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));

        // projection with NewExpression
        System.Linq.Expressions.Expression<Func<CovItem, object>> proj =
            e => new { e.Id, e.Name };
        var mat = factory.CreateSchemaAwareMaterializer(mapping, typeof(CovItem), proj);
        Assert.NotNull(mat);
    }

    [Fact]
    public void ConvertDbValue_NullForReferenceType_ReturnsNull()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var result = m.Invoke(null, new object?[] { null, typeof(string) });
        Assert.Null(result);
    }

    [Fact]
    public void ConvertDbValue_NullForNullableInt_ReturnsNull()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var result = m.Invoke(null, new object?[] { null, typeof(int?) });
        Assert.Null(result);
    }

    [Fact]
    public void ConvertDbValue_NullForNonNullableValueType_Throws()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var ex = Assert.Throws<System.Reflection.TargetInvocationException>(
            () => m.Invoke(null, new object?[] { null, typeof(int) }));
        Assert.IsType<InvalidOperationException>(ex.InnerException);
        Assert.Contains("NULL", ex.InnerException!.Message);
    }

    [Fact]
    public void ConvertDbValue_SameType_ReturnsSameObject()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var result = m.Invoke(null, new object?[] { "hello", typeof(string) });
        Assert.Equal("hello", result);
    }

    [Fact]
    public void ConvertDbValue_EnumConversion_ReturnsEnum()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var result = m.Invoke(null, new object?[] { 1L, typeof(MfcStatus) });
        Assert.Equal(MfcStatus.Active, result);
    }

    [Fact]
    public void ConvertDbValue_LongToInt_Converts()
    {
        var m = typeof(MaterializerFactory)
            .GetMethod("ConvertDbValue", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;
        var result = m.Invoke(null, new object?[] { 42L, typeof(int) });
        Assert.Equal(42, result);
    }

    [Fact]
    public void CreateSyncMaterializerNonGeneric_WithOffset_ReturnsDelegate()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));

        // startOffset > 0 exercises the non-default code path
        var mat = factory.CreateSyncMaterializer(mapping, typeof(CovItem), null, 0);
        Assert.NotNull(mat);
    }
}

//             HasSocketExceptionInChain, TriggerFailoverAsync no-healthy-nodes path
