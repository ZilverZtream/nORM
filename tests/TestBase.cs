using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;
using Microsoft.Data.SqlClient;
using Microsoft.Data.Sqlite;
using nORM.Core;
using nORM.Providers;

namespace nORM.Tests;

public abstract class TestBase
{
    protected (string Sql, Dictionary<string, object> Params) Translate<T>(Expression<Func<T, bool>> expr, DbConnection connection, DatabaseProvider provider) where T : class, new()
    {
        using var ctx = new DbContext(connection, provider);
        var getMapping = typeof(DbContext).GetMethod("GetMapping", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!;
        var mapping = getMapping.Invoke(ctx, new object[] { typeof(T) });
        var visitorType = typeof(DbContext).Assembly.GetType("nORM.Query.ExpressionToSqlVisitor", true)!;
        var visitor = Activator.CreateInstance(visitorType, ctx, mapping, ctx.Provider, expr.Parameters[0], "T0", null)!;
        var sql = (string)visitorType.GetMethod("Translate")!.Invoke(visitor, new object[] { expr.Body })!;
        var parameters = (Dictionary<string, object>)visitorType.GetMethod("GetParameters")!.Invoke(visitor, null)!;
        return (sql, parameters);
    }

    protected static (DbConnection Connection, DatabaseProvider Provider) CreateProvider(ProviderKind kind)
    {
        return kind switch
        {
            ProviderKind.Sqlite => (new SqliteConnection("Data Source=:memory:"), new SqliteProvider()),
            ProviderKind.SqlServer => (new SqlConnection("Server=(localdb)\\mssqllocaldb;Integrated Security=true;"), new SqlServerProvider()),
            _ => throw new NotSupportedException()
        };
    }
}

public enum ProviderKind
{
    Sqlite,
    SqlServer
}
