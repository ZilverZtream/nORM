using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Core;
using nORM.Internal;
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

internal static class FastExpressionVisitorPool
{
    [ThreadStatic]
    private static ExpressionToSqlVisitor? _threadLocalVisitor;

    private static readonly ConcurrentDictionary<MemberInfo, Delegate> _memberAccessorCache = new();

    public static ExpressionToSqlVisitor Get(in VisitorContext context)
    {
        var visitor = _threadLocalVisitor;
        if (visitor == null)
        {
            visitor = new ExpressionToSqlVisitor();
            _threadLocalVisitor = visitor;
        }

        visitor.Initialize(in context);
        return visitor;
    }

    public static void Return(ExpressionToSqlVisitor visitor)
    {
        visitor.Reset();
        // Keep in thread-local storage, no actual return needed
    }

    public static object? GetMemberValue(MemberInfo member, object? instance)
    {
        if (!_memberAccessorCache.TryGetValue(member, out var del))
        {
            var objParam = Expression.Parameter(typeof(object), "obj");
            var typedParam = Expression.Convert(objParam, member.DeclaringType!);
            Expression body = member switch
            {
                PropertyInfo pi => Expression.Property(typedParam, pi),
                FieldInfo fi => Expression.Field(typedParam, fi),
                _ => throw new NotSupportedException("Member must be a field or property.")
            };
            body = Expression.Convert(body, typeof(object));
            var lambda = Expression.Lambda<Func<object?, object?>>(body, objParam);
            var compiled = ExpressionCompiler.CompileExpression(lambda);
            _memberAccessorCache.TryAdd(member, compiled);
            del = compiled;
        }

        return ((Func<object?, object?>)del)(instance);
    }
}
