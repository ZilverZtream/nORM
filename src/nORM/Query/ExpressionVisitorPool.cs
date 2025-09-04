using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Query;

internal static class ExpressionVisitorPool
{
    private static readonly ObjectPool<ExpressionToSqlVisitor> _pool =
        new DefaultObjectPool<ExpressionToSqlVisitor>(new VisitorPooledObjectPolicy());

    public static ExpressionToSqlVisitor Get(
        DbContext ctx,
        TableMapping mapping,
        DatabaseProvider provider,
        ParameterExpression parameter,
        string tableAlias,
        Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>? correlated = null,
        List<string>? compiledParams = null,
        Dictionary<ParameterExpression, string>? paramMap = null)
    {
        var visitor = _pool.Get();
        visitor.Initialize(ctx, mapping, provider, parameter, tableAlias, correlated, compiledParams, paramMap);
        return visitor;
    }

    public static void Return(ExpressionToSqlVisitor visitor)
    {
        visitor.Reset();
        _pool.Return(visitor);
    }
}

internal sealed class VisitorPooledObjectPolicy : PooledObjectPolicy<ExpressionToSqlVisitor>
{
    public override ExpressionToSqlVisitor Create() => new ExpressionToSqlVisitor();

    public override bool Return(ExpressionToSqlVisitor obj) => true;
}
