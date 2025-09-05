using System.Collections.Generic;
using System.Linq.Expressions;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;

namespace nORM.Query;

internal readonly struct VisitorContext
{
    public readonly DbContext Context;
    public readonly TableMapping Mapping;
    public readonly DatabaseProvider Provider;
    public readonly ParameterExpression Parameter;
    public readonly string TableAlias;
    public readonly Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>? Correlated;
    public readonly List<string>? CompiledParams;
    public readonly Dictionary<ParameterExpression, string>? ParamMap;

    public VisitorContext(
        DbContext context,
        TableMapping mapping,
        DatabaseProvider provider,
        ParameterExpression parameter,
        string tableAlias,
        Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>? correlated,
        List<string>? compiledParams,
        Dictionary<ParameterExpression, string>? paramMap)
    {
        Context = context;
        Mapping = mapping;
        Provider = provider;
        Parameter = parameter;
        TableAlias = tableAlias;
        Correlated = correlated;
        CompiledParams = compiledParams;
        ParamMap = paramMap;
    }
}

internal static class ExpressionVisitorPool
{
    private static readonly ObjectPool<ExpressionToSqlVisitor> _pool =
        new DefaultObjectPool<ExpressionToSqlVisitor>(new VisitorPooledObjectPolicy());

    public static ExpressionToSqlVisitor Get(in VisitorContext context)
    {
        var visitor = _pool.Get();
        visitor.Initialize(in context);
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
