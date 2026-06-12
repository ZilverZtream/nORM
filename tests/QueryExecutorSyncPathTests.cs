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
public class QueryExecutorSyncPathTests
{
    private static (SqliteConnection Cn, DbContext Ctx) MakeDb()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        using var cmd = cn.CreateCommand();
        cmd.CommandText = "CREATE TABLE CovBoost_Item (Id INTEGER PRIMARY KEY AUTOINCREMENT, Name TEXT, Value INTEGER, IsActive INTEGER);" +
                          "INSERT INTO CovBoost_Item (Name, Value, IsActive) VALUES ('SyncTest', 55, 1);";
        cmd.ExecuteNonQuery();
        return (cn, new DbContext(cn, new SqliteProvider()));
    }

    [Fact]
    public void QueryExecutor_CreateListForType_ReturnsTypedList()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        // Access via the internal wrapper method through the include processor
        var executor = new QueryExecutor(ctx,
            new IncludeProcessor(ctx));
        var list = executor.CreateListForType(typeof(CovItem), 10);
        Assert.NotNull(list);
        Assert.IsAssignableFrom<System.Collections.IList>(list);
    }

    [Fact]
    public void QueryExecutor_RedactSqlForLogging_RedactsStringLiterals()
    {
        var m = typeof(QueryExecutor)
            .GetMethod("RedactSqlForLogging",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        var sql = "SELECT * FROM T WHERE Name = 'secret_value'";
        var result = (string)m.Invoke(null, new object[] { sql })!;
        Assert.DoesNotContain("secret_value", result);
        Assert.Contains("[redacted]", result);
    }

    [Fact]
    public void QueryExecutor_RedactSqlForLogging_RedactsNationalString()
    {
        var m = typeof(QueryExecutor)
            .GetMethod("RedactSqlForLogging",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        var sql = "SELECT * FROM T WHERE Name = N'unicode_secret'";
        var result = (string)m.Invoke(null, new object[] { sql })!;
        Assert.DoesNotContain("unicode_secret", result);
    }

    [Fact]
    public void QueryExecutor_RedactSqlForLogging_NullOrEmpty_ReturnsInput()
    {
        var m = typeof(QueryExecutor)
            .GetMethod("RedactSqlForLogging",
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static)!;

        Assert.Equal("", (string)m.Invoke(null, new object[] { "" })!);
        Assert.Null(m.Invoke(null, new object?[] { null }));
    }

    [Fact]
    public void SyncMaterialize_ViaForeachOnIQueryable_WorksCorrectly()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        var items = new List<CovItem>();
        foreach (var item in ctx.Query<CovItem>())
            items.Add(item);
        Assert.Single(items);
        Assert.Equal("SyncTest", items[0].Name);
    }

    [Fact]
    public void SyncMaterialize_NoTrackingQuery_SkipsChangeTracking()
    {
        var (cn, ctx) = MakeDb();
        using var _cn = cn; using var _ctx = ctx;

        // AsNoTracking sets NoTracking flag which changes ProcessEntity behavior
        var q = (INormQueryable<CovItem>)ctx.Query<CovItem>();
        var items = q.AsNoTracking().ToList();
        Assert.Single(items);
    }
}
