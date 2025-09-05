using System.Reflection;
using System.Threading;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class QueryTranslatorThreadLocalTests
{
    [Fact]
    public void Dispose_clears_threadlocal_without_replacing_it()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;

        var threadLocalField = translatorType.GetField("_threadLocalTranslator", BindingFlags.NonPublic | BindingFlags.Static)!;
        var rentMethod = translatorType.GetMethod("Rent", BindingFlags.NonPublic | BindingFlags.Static)!;
        var disposeMethod = translatorType.GetMethod("Dispose")!;

        var threadLocalBefore = threadLocalField.GetValue(null)!;

        var translator = rentMethod.Invoke(null, new object[] { ctx });

        disposeMethod.Invoke(translator, null);

        var threadLocalAfter = threadLocalField.GetValue(null)!;

        Assert.Same(threadLocalBefore, threadLocalAfter);

        var threadLocalType = typeof(ThreadLocal<>).MakeGenericType(translatorType);
        var valueProp = threadLocalType.GetProperty("Value")!;
        var value = valueProp.GetValue(threadLocalAfter);
        Assert.Null(value);
    }
}

