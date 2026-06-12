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
public class MaterializerFactoryILPathTests
{
    [Fact]
    public void PrecompileAndQuery_NoCtorEntity_UsesILParameterizedCtorPath()
    {
        // PrecompileCommonPatterns calls CreateILMaterializer<T> which takes the
        // parameterized-ctor path (lines 309-377)
        MaterializerFactory.PrecompileCommonPatterns<CovNoCtorEntity>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CovBoost_NoCtor (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT NOT NULL DEFAULT '');
            INSERT INTO CovBoost_NoCtor (Name) VALUES ('ILTest');";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<CovNoCtorEntity>().ToList();
        Assert.Single(list);
        Assert.Equal("ILTest", list[0].Name);
    }

    [Fact]
    public void PrecompileAndQuery_NullableEnumEntity_UsesNullableEnumILPath()
    {
        MaterializerFactory.PrecompileCommonPatterns<MfcNullableEnumEntity>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE MFC_NullableEnum (Id INTEGER PRIMARY KEY AUTOINCREMENT, Status INTEGER);
            INSERT INTO MFC_NullableEnum (Status) VALUES (1);
            INSERT INTO MFC_NullableEnum (Status) VALUES (NULL);";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcNullableEnumEntity>().OrderBy(e => e.Id).ToList();
        Assert.Equal(2, list.Count);
        Assert.Equal(MfcStatus.Active, list[0].Status);
        Assert.Null(list[1].Status);
    }

    [Fact]
    public void PrecompileAndQuery_EnumEntity_UsesEnumILPath()
    {
        MaterializerFactory.PrecompileCommonPatterns<MfcEnumEntity>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE MFC_Enum (Id INTEGER PRIMARY KEY AUTOINCREMENT, Status INTEGER NOT NULL DEFAULT 0);
            INSERT INTO MFC_Enum (Status) VALUES (2);";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var list = ctx.Query<MfcEnumEntity>().ToList();
        Assert.Single(list);
        Assert.Equal(MfcStatus.Inactive, list[0].Status);
    }

    [Fact]
    public void CreateSyncMaterializer_Generic_ReturnsStronglyTyped()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER);
            INSERT INTO CovBoost_Item (Name, Value, IsActive) VALUES ('Test', 42, 1);";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));

        // CreateSyncMaterializer<T> (generic typed overload)
        var syncMat = factory.CreateSyncMaterializer<CovItem>(mapping);
        Assert.NotNull(syncMat);

        // Execute with an actual reader to verify it works
        using var readCmd = cn.CreateCommand();
        readCmd.CommandText = "SELECT Id, Name, Value, IsActive FROM CovBoost_Item";
        using var reader = readCmd.ExecuteReader();
        Assert.True(reader.Read());
        var item = syncMat(reader);
        Assert.NotNull(item);
        Assert.Equal("Test", item.Name);
        Assert.Equal(42, item.Value);
    }

    [Fact]
    public void CreateSyncMaterializer_WithFastMaterializerPrecompiled_UsesCache()
    {
        MaterializerFactory.PrecompileCommonPatterns<CovItem>();

        using var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = @"
            CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER);
            INSERT INTO CovBoost_Item VALUES (1, 'Cached', 10, 1);";
        cmd.ExecuteNonQuery();

        using var ctx = new DbContext(cn, new SqliteProvider());
        var factory = new MaterializerFactory();
        var mapping = ctx.GetMapping(typeof(CovItem));
        var mat = factory.CreateSyncMaterializer<CovItem>(mapping);

        using var readCmd = cn.CreateCommand();
        readCmd.CommandText = "SELECT Id, Name, Value, IsActive FROM CovBoost_Item";
        using var reader = readCmd.ExecuteReader();
        reader.Read();
        var item = mat(reader);
        Assert.Equal("Cached", item.Name);
    }
}

// Covers: BulkBatchSize/MaxRecursionDepth/MaxGroupJoinSize validation throws,
//         CommandTimeout getter/setter, Validate() method branches,
//         AddGlobalFilter<TEntity>(Expression<Func<TEntity,bool>>) overload
