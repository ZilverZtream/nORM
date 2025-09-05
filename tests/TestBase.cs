using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
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
        var visitor = Activator.CreateInstance(visitorType, ctx, mapping, ctx.Provider, expr.Parameters[0], provider.Escape("T0"), null, null, null)!;
        var sql = (string)visitorType.GetMethod("Translate")!.Invoke(visitor, new object[] { expr.Body })!;
        var parameters = (Dictionary<string, object>)visitorType.GetMethod("GetParameters")!.Invoke(visitor, null)!;
        return (sql, parameters);
    }

    protected static (DbConnection Connection, DatabaseProvider Provider) CreateProvider(ProviderKind kind)
    {
        return kind switch
        {
            ProviderKind.Sqlite => (CreateOpenConnection(), new SqliteProvider()),
            ProviderKind.SqlServer => (CreateOpenConnection(), new SqlServerProvider()),
            ProviderKind.MySql => (CreateOpenConnection(), new MySqlProvider(new SqliteParameterFactory())),
            _ => throw new NotSupportedException()
        };
    }

    private static DbConnection CreateOpenConnection()
    {
        var cn = new SqliteConnection("Data Source=:memory:");
        cn.Open();
        return cn;
    }

    protected (string Sql, Dictionary<string, object> Params, Type ElementType) TranslateQuery<T, TResult>(
        Func<IQueryable<T>, IQueryable<TResult>> build,
        DbConnection connection,
        DatabaseProvider provider) where T : class, new()
    {
        using var ctx = new DbContext(connection, provider);
        var query = build(ctx.Query<T>());
        var expr = query.Expression;
        var translatorType = typeof(DbContext).Assembly.GetType("nORM.Query.QueryTranslator", true)!;
        var translator = Activator.CreateInstance(translatorType, ctx)!;
        var plan = translatorType.GetMethod("Translate")!.Invoke(translator, new object[] { expr })!;
        var sql = (string)plan.GetType().GetProperty("Sql")!.GetValue(plan)!;
        var parameters = (IReadOnlyDictionary<string, object>)plan.GetType().GetProperty("Parameters")!.GetValue(plan)!;
        var elementType = (Type)plan.GetType().GetProperty("ElementType")!.GetValue(plan)!;
        return (sql, new Dictionary<string, object>(parameters), elementType);
    }
}

public enum ProviderKind
{
    Sqlite,
    SqlServer,
    MySql
}
