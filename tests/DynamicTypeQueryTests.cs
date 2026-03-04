using System;
using System.Linq;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// MM-1: Verifies that the dynamic type cache uses a composite key that includes the
/// provider type and connection string hash so that two contexts pointing to different
/// databases with the same table name each receive their own distinct CLR type.
/// </summary>
public class DynamicTypeQueryTests
{
    private static (SqliteConnection cn, DbContext ctx) CreateContextWithSchema(string dbName, string[] columnDefs)
    {
        // Use named in-memory SQLite databases so each context has a distinct connection string
        var cn = new SqliteConnection($"Data Source={dbName};Mode=Memory;Cache=Shared");
        cn.Open();
        var ctx = new DbContext(cn, new SqliteProvider());

        var cols = string.Join(", ", columnDefs);
        using var cmd = cn.CreateCommand();
        cmd.CommandText = $"CREATE TABLE IF NOT EXISTS Users ({cols})";
        cmd.ExecuteNonQuery();

        return (cn, ctx);
    }

    [Fact]
    public void TwoContextsDifferentSchemas_SameTableName_GetDistinctTypes()
    {
        var db1Name = $"mm1_test_a_{Guid.NewGuid():N}";
        var db2Name = $"mm1_test_b_{Guid.NewGuid():N}";

        // Context 1: Users table with Id + Name
        var (cn1, ctx1) = CreateContextWithSchema(db1Name, new[]
        {
            "Id INTEGER PRIMARY KEY",
            "Name TEXT"
        });

        // Context 2: Users table with Id + Email (different schema, different DB name)
        var (cn2, ctx2) = CreateContextWithSchema(db2Name, new[]
        {
            "Id INTEGER PRIMARY KEY",
            "Email TEXT"
        });

        using var _cn1 = cn1;
        using var _ctx1 = ctx1;
        using var _cn2 = cn2;
        using var _ctx2 = ctx2;

        var type1 = ctx1.Query("Users").ElementType;
        var type2 = ctx2.Query("Users").ElementType;

        // Each context must get its own distinct CLR type (different schemas → different types)
        Assert.NotSame(type1, type2);

        // Verify type1 has Name property and type2 has Email property
        var props1 = type1.GetProperties().Select(p => p.Name).ToHashSet();
        var props2 = type2.GetProperties().Select(p => p.Name).ToHashSet();

        Assert.Contains("Name", props1);
        Assert.DoesNotContain("Email", props1);

        Assert.Contains("Email", props2);
        Assert.DoesNotContain("Name", props2);
    }

    [Fact]
    public void SameContextSameTable_ReturnsSameType()
    {
        var dbName = $"mm1_test_same_{Guid.NewGuid():N}";
        var (cn, ctx) = CreateContextWithSchema(dbName, new[]
        {
            "Id INTEGER PRIMARY KEY",
            "Title TEXT"
        });

        using var _cn = cn;
        using var _ctx = ctx;

        var type1 = ctx.Query("Users").ElementType;
        var type2 = ctx.Query("Users").ElementType;

        // Same context, same table name → the Lazy cache must return the same type
        Assert.Same(type1, type2);
    }
}
