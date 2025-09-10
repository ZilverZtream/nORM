using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using Microsoft.Extensions.ObjectPool;
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
    private sealed class VisitorPolicy : PooledObjectPolicy<ExpressionToSqlVisitor>
    {
        /// <summary>
        /// Creates a new <see cref="ExpressionToSqlVisitor"/> instance when the
        /// pool requires additional objects.
        /// </summary>
        /// <returns>A freshly instantiated visitor ready for initialization.</returns>
        public override ExpressionToSqlVisitor Create() => new();

        /// <summary>
        /// Resets a visitor before it is returned to the pool for reuse.
        /// </summary>
        /// <param name="obj">The visitor being returned.</param>
        /// <returns>Always <c>true</c> to indicate the object may be reused.</returns>
        public override bool Return(ExpressionToSqlVisitor obj)
        {
            obj.Reset();
            return true;
        }
    }

    private static readonly ObjectPool<ExpressionToSqlVisitor> _pool =
        new DefaultObjectPool<ExpressionToSqlVisitor>(new VisitorPolicy(), Environment.ProcessorCount * 2);

    private static readonly ConcurrentDictionary<MemberInfo, Delegate> _memberAccessorCache = new();

    /// <summary>
    /// Obtains a visitor from the pool and initializes it with the supplied
    /// context.
    /// </summary>
    /// <param name="context">Configuration used to initialize the visitor.</param>
    /// <returns>An <see cref="ExpressionToSqlVisitor"/> instance ready for use.</returns>
    public static ExpressionToSqlVisitor Get(in VisitorContext context)
    {
        var visitor = _pool.Get();
        visitor.Initialize(in context);
        return visitor;
    }

    /// <summary>
    /// Returns a visitor to the pool after resetting its internal state.
    /// </summary>
    /// <param name="visitor">The visitor to recycle.</param>
    public static void Return(ExpressionToSqlVisitor visitor)
    {
        visitor.FastReset();
        _pool.Return(visitor);
    }

    /// <summary>
    /// Retrieves the value of a field or property using a cached delegate for
    /// optimal performance.
    /// </summary>
    /// <param name="member">The member whose value should be read.</param>
    /// <param name="instance">The object instance to read from; <c>null</c> for
    /// static members.</param>
    /// <returns>The boxed member value or <c>null</c> if the member cannot be
    /// evaluated.</returns>
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
