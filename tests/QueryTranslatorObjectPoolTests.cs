using System.Reflection;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;
using Xunit;

namespace nORM.Tests;

public class QueryTranslatorObjectPoolTests
{
    [Fact]
    public void Dispose_returns_translator_to_pool()
    {
        using var cn = new SqliteConnection("Data Source=:memory:");
        using var ctx = new DbContext(cn, new SqliteProvider());

        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var rentMethod = translatorType.GetMethod("Rent", BindingFlags.NonPublic | BindingFlags.Static)!;
        var disposeMethod = translatorType.GetMethod("Dispose")!;

        var translator1 = rentMethod.Invoke(null, new object[] { ctx });
        disposeMethod.Invoke(translator1, null);

        var translator2 = rentMethod.Invoke(null, new object[] { ctx });

        Assert.Same(translator1, translator2);
    }
}

