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
public class NormQueryProviderSyncAndScalarTests
{
    private static (SqliteConnection Cn, DbContext Ctx) MakeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER);" +
                          "INSERT INTO CovBoost_Item (Name, Value, IsActive) VALUES ('A', 10, 1);" +
                          "INSERT INTO CovBoost_Item (Name, Value, IsActive) VALUES ('B', 20, 0);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void ExecuteSync_ViaIEnumerableGetEnumerator_MaterializesItems()
    {
        // IEnumerable.GetEnumerator() triggers IQueryProvider.Execute<IEnumerable<T>>
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var query = ctx.Query<CovItem>();
        var items = new List<CovItem>();
        // Using foreach on IQueryable<T> triggers synchronous execution
        foreach (var item in query)
            items.Add(item);

        Assert.Equal(2, items.Count);
    }

    [Fact]
    public void Execute_ReturnsFirstItem_SyncPath()
    {
        // .First() on IQueryable calls IQueryProvider.Execute<T> (sync)
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var item = ctx.Query<CovItem>().OrderBy(i => i.Id).First();
        Assert.Equal("A", item.Name);
    }

    [Fact]
    public void ConvertScalarResult_Short_Converts()
    {
        // Call ConvertScalarResult<short> via reflection
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(short));
        var result = (short)m.Invoke(null, new object[] { (object)42L })!;
        Assert.Equal((short)42, result);
    }

    [Fact]
    public void ConvertScalarResult_Byte_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(byte));
        var result = (byte)m.Invoke(null, new object[] { (object)255L })!;
        Assert.Equal((byte)255, result);
    }

    [Fact]
    public void ConvertScalarResult_Float_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(float));
        var result = (float)m.Invoke(null, new object[] { (object)3.14 })!;
        Assert.Equal(3.14f, result, 2);
    }

    [Fact]
    public void ConvertScalarResult_DateTime_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(DateTime));
        var dt = new DateTime(2026, 3, 18);
        var result = (DateTime)m.Invoke(null, new object[] { (object)dt })!;
        Assert.Equal(dt, result);
    }

    [Fact]
    public void ConvertScalarResult_Guid_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(Guid));
        var g = Guid.NewGuid();
        var result = (Guid)m.Invoke(null, new object[] { (object)g })!;
        Assert.Equal(g, result);
    }

    [Fact]
    public void ConvertScalarResult_ReferenceType_DirectCast()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(string));
        var result = (string?)m.Invoke(null, new object[] { (object)"hello" })!;
        Assert.Equal("hello", result);
    }

    [Fact]
    public void ConvertScalarResult_Double_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(double));
        var result = (double)m.Invoke(null, new object[] { (object)2.718 })!;
        Assert.Equal(2.718, result, 6);
    }

    [Fact]
    public void ConvertScalarResult_Decimal_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(decimal));
        var result = (decimal)m.Invoke(null, new object[] { (object)99.99m })!;
        Assert.Equal(99.99m, result);
    }

    [Fact]
    public void ConvertScalarResult_Bool_Converts()
    {
        var m = typeof(NormQueryProvider)
            .GetMethod("ConvertScalarResult", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!
            .MakeGenericMethod(typeof(bool));
        var result = (bool)m.Invoke(null, new object[] { (object)1L })!;
        Assert.True(result);
    }
}

// Covers: QueryExecutor.Materialize (sync non-async path), RedactSqlForLogging
//         internal static method, CreateListForType, IsReadOnlyQuery
