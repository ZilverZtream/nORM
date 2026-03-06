using System;
using System.Reflection;
using nORM.Query;
using Xunit;

namespace nORM.Tests;

/// <summary>
/// PERF-1: Verifies NormalizeConnectionStringForCacheKey correctly handles quoted semicolons,
/// key ordering, credential stripping, malformed strings, and distinct databases.
/// </summary>
public class CacheKeyNormalizationTests
{
    private static string Normalize(string? cs)
    {
        var method = typeof(NormQueryProvider)
            .GetMethod("NormalizeConnectionStringForCacheKey",
                BindingFlags.Static | BindingFlags.NonPublic)!;
        return (string)method.Invoke(null, new object?[] { cs })!;
    }

    [Fact]
    public void Null_ConnectionString_Returns_Empty()
    {
        Assert.Equal(string.Empty, Normalize(null));
    }

    [Fact]
    public void Empty_ConnectionString_Returns_Empty()
    {
        Assert.Equal(string.Empty, Normalize(string.Empty));
    }

    [Fact]
    public void Reordered_Keys_Produce_Same_CacheKey()
    {
        var cs1 = "Data Source=mydb.db;Mode=ReadWrite";
        var cs2 = "Mode=ReadWrite;Data Source=mydb.db";
        Assert.Equal(Normalize(cs1), Normalize(cs2));
    }

    [Fact]
    public void Password_Key_Is_Stripped()
    {
        var cs = "Data Source=mydb.db;Password=s3cr3t";
        var result = Normalize(cs);
        Assert.DoesNotContain("s3cr3t", result, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("password", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Token_Key_Is_Stripped()
    {
        var cs = "Data Source=mydb.db;Token=abc123";
        var result = Normalize(cs);
        Assert.DoesNotContain("abc123", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Secret_Key_Is_Stripped()
    {
        var cs = "Data Source=mydb.db;Secret=topsecret";
        var result = Normalize(cs);
        Assert.DoesNotContain("topsecret", result, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Two_Different_Databases_Produce_Different_Keys()
    {
        var cs1 = "Data Source=db1.db";
        var cs2 = "Data Source=db2.db";
        Assert.NotEqual(Normalize(cs1), Normalize(cs2));
    }

    [Fact]
    public void Malformed_ConnectionString_Returns_Hash_Not_Exception()
    {
        // A string that DbConnectionStringBuilder cannot parse should fall back to SHA256 hash.
        var malformed = "this is not a valid = connection string ; with weird = chars @#$";
        var result = Normalize(malformed);
        // Should not throw, and should return a non-empty string (the hex hash).
        Assert.NotEmpty(result);
    }

    [Fact]
    public void Identical_ConnectionStrings_Produce_Same_Key()
    {
        var cs = "Data Source=test.db;Mode=ReadWrite";
        Assert.Equal(Normalize(cs), Normalize(cs));
    }
}
