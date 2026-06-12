using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        private bool IsPostMaterializeTailMode => _groupJoinInfo != null || _postMaterializeElementType != null;

        private Type? CurrentPostMaterializeElementType => _postMaterializeElementType ?? _groupJoinInfo?.ResultType;

        private void AppendPostMaterializeTransform(System.Func<DbContext, System.Collections.IList, System.Collections.IList> transform, Type resultElementType)
        {
            var previous = _postMaterializeTransform;
            _postMaterializeTransform = previous == null
                ? transform
                : (ctx, rows) => transform(ctx, previous(ctx, rows));
            _postMaterializeElementType = resultElementType;
        }

        private void AppendPostMaterializeInnerJoin(Expression innerQuery, LambdaExpression outerKeySelector, LambdaExpression innerKeySelector, LambdaExpression resultSelector)
        {
            var outerType = CurrentPostMaterializeElementType ?? _projection?.Body.Type ?? _mapping.Type;
            if (outerType == null || outerKeySelector.Parameters.Count != 1 || outerKeySelector.Parameters[0].Type != outerType)
                return;

            var innerElementType = GetElementType(innerQuery);
            var outerItem = Expression.Parameter(typeof(object), "outer");
            var outerKeyBody = new nORM.Internal.ParameterReplacer(outerKeySelector.Parameters[0], Expression.Convert(outerItem, outerType)).Visit(outerKeySelector.Body)!;
            var outerKeyReader = Expression.Lambda<System.Func<object, object>>(Expression.Convert(outerKeyBody, typeof(object)), outerItem).Compile();

            var innerItem = Expression.Parameter(typeof(object), "inner");
            var innerKeyBody = new nORM.Internal.ParameterReplacer(innerKeySelector.Parameters[0], Expression.Convert(innerItem, innerElementType)).Visit(innerKeySelector.Body)!;
            var innerKeyReader = Expression.Lambda<System.Func<object, object>>(Expression.Convert(innerKeyBody, typeof(object)), innerItem).Compile();

            var outerArg = Expression.Parameter(typeof(object), "outerArg");
            var innerArg = Expression.Parameter(typeof(object), "innerArg");
            var resultBody = new nORM.Internal.ParameterReplacer(resultSelector.Parameters[0], Expression.Convert(outerArg, outerType)).Visit(resultSelector.Body)!;
            resultBody = new nORM.Internal.ParameterReplacer(resultSelector.Parameters[1], Expression.Convert(innerArg, innerElementType)).Visit(resultBody)!;
            var projector = Expression.Lambda<System.Func<object, object, object>>(Expression.Convert(resultBody, typeof(object)), outerArg, innerArg).Compile();
            var resultType = resultSelector.Body.Type;

            AppendPostMaterializeTransform((ctx, rows) =>
            {
                var resolvedInnerQuery = new PostMaterializeQuerySourceReplacer(ctx).Visit(innerQuery)!;
                var innerEnumerable = Expression.Lambda<System.Func<System.Collections.IEnumerable>>(
                    Expression.Convert(resolvedInnerQuery, typeof(System.Collections.IEnumerable))).Compile();
                var inners = innerEnumerable().Cast<object>().ToArray();
                var output = CreateRuntimeList(resultType, rows.Count);
                foreach (var outer in rows.Cast<object>())
                {
                    var outerKey = outerKeyReader(outer);
                    foreach (var inner in inners)
                    {
                        if (object.Equals(outerKey, innerKeyReader(inner)))
                            output.Add(projector(outer, inner));
                    }
                }
                return output;
            }, resultType);
        }

        private void AppendPostMaterializeGroupJoin(Expression innerQuery, LambdaExpression outerKeySelector, LambdaExpression innerKeySelector, LambdaExpression resultSelector)
        {
            var outerType = CurrentPostMaterializeElementType;
            if (outerType == null || outerKeySelector.Parameters.Count != 1 || outerKeySelector.Parameters[0].Type != outerType)
                return;

            var innerElementType = GetElementType(innerQuery);
            var outerItem = Expression.Parameter(typeof(object), "outer");
            var outerKeyBody = new nORM.Internal.ParameterReplacer(outerKeySelector.Parameters[0], Expression.Convert(outerItem, outerType)).Visit(outerKeySelector.Body)!;
            var outerKeyReader = Expression.Lambda<System.Func<object, object>>(Expression.Convert(outerKeyBody, typeof(object)), outerItem).Compile();

            var innerItem = Expression.Parameter(typeof(object), "inner");
            var innerKeyBody = new nORM.Internal.ParameterReplacer(innerKeySelector.Parameters[0], Expression.Convert(innerItem, innerElementType)).Visit(innerKeySelector.Body)!;
            var innerKeyReader = Expression.Lambda<System.Func<object, object>>(Expression.Convert(innerKeyBody, typeof(object)), innerItem).Compile();

            var outerArg = Expression.Parameter(typeof(object), "outerArg");
            var groupArg = Expression.Parameter(typeof(object), "groupArg");
            var resultBody = new nORM.Internal.ParameterReplacer(resultSelector.Parameters[0], Expression.Convert(outerArg, outerType)).Visit(resultSelector.Body)!;
            resultBody = new nORM.Internal.ParameterReplacer(resultSelector.Parameters[1], Expression.Convert(groupArg, resultSelector.Parameters[1].Type)).Visit(resultBody)!;
            var projector = Expression.Lambda<System.Func<object, object, object>>(Expression.Convert(resultBody, typeof(object)), outerArg, groupArg).Compile();
            var resultType = resultSelector.Body.Type;

            AppendPostMaterializeTransform((ctx, rows) =>
            {
                var resolvedInnerQuery = new PostMaterializeQuerySourceReplacer(ctx).Visit(innerQuery)!;
                var innerEnumerable = Expression.Lambda<System.Func<System.Collections.IEnumerable>>(
                    Expression.Convert(resolvedInnerQuery, typeof(System.Collections.IEnumerable))).Compile();
                var inners = innerEnumerable().Cast<object>().ToArray();
                var output = CreateRuntimeList(resultType, rows.Count);
                foreach (var outer in rows.Cast<object>())
                {
                    var outerKey = outerKeyReader(outer);
                    var matches = CreateRuntimeList(innerElementType, 0);
                    foreach (var inner in inners)
                    {
                        if (object.Equals(outerKey, innerKeyReader(inner)))
                            matches.Add(inner);
                    }
                    output.Add(projector(outer, matches));
                }
                return output;
            }, resultType);
        }

        private void AppendPostMaterializeGroupBy(LambdaExpression keySelector, LambdaExpression? elementSelector = null)
        {
            var inputType = CurrentPostMaterializeElementType ?? keySelector.Parameters[0].Type;
            var keyType = keySelector.Body.Type;
            var elementType = elementSelector?.Body.Type ?? inputType;
            var objParam = Expression.Parameter(typeof(object), "row");
            var castRow = Expression.Convert(objParam, inputType);
            var keyBody = new ParameterReplacer(keySelector.Parameters[0], castRow).Visit(keySelector.Body)!;
            var boxedKey = keyType.IsValueType ? (Expression)Expression.Convert(keyBody, typeof(object)) : keyBody;
            var keyFunc = Expression.Lambda<Func<object, object?>>(boxedKey, objParam).Compile();

            Func<object, object?> elementFunc;
            if (elementSelector != null)
            {
                var elementBody = new ParameterReplacer(elementSelector.Parameters[0], castRow).Visit(elementSelector.Body)!;
                var boxedElement = elementType.IsValueType ? (Expression)Expression.Convert(elementBody, typeof(object)) : elementBody;
                elementFunc = Expression.Lambda<Func<object, object?>>(boxedElement, objParam).Compile();
            }
            else
            {
                elementFunc = static row => row;
            }

            var groupingType = typeof(IGrouping<,>).MakeGenericType(keyType, elementType);
            var concreteType = typeof(ClientGrouping<,>).MakeGenericType(keyType, elementType);
            var groupingCtor = concreteType.GetConstructor(new[] { keyType, typeof(IEnumerable<>).MakeGenericType(elementType) })!;
            var castMethod = typeof(Enumerable).GetMethod(nameof(Enumerable.Cast))!.MakeGenericMethod(elementType);
            var resultListType = typeof(List<>).MakeGenericType(groupingType);
            var nullSentinel = new object();

            AppendPostMaterializeTransform((ctx, rows) =>
            {
                var keyOrder = new List<object?>();
                var buckets = new Dictionary<object, List<object>>(EqualityComparer<object>.Default);
                foreach (var row in rows)
                {
                    var key = row == null ? null : keyFunc(row);
                    var dictKey = (object?)key ?? nullSentinel;
                    if (!buckets.TryGetValue(dictKey, out var bucket))
                    {
                        bucket = new List<object>();
                        buckets[dictKey] = bucket;
                        keyOrder.Add(key);
                    }
                    bucket.Add(elementFunc(row!)!);
                }

                var result = (System.Collections.IList)Activator.CreateInstance(resultListType, keyOrder.Count)!;
                foreach (var keyObj in keyOrder)
                {
                    var dictKey = (object?)keyObj ?? nullSentinel;
                    var items = castMethod.Invoke(null, new object?[] { buckets[dictKey] })!;
                    var grouping = groupingCtor.Invoke(new object?[] { keyObj, items })!;
                    result.Add(grouping);
                }
                return result;
            }, groupingType);
        }

        private void AppendPostMaterializeSelectMany(LambdaExpression collectionSelector, LambdaExpression? resultSelector)
        {
            var inputType = CurrentPostMaterializeElementType ?? collectionSelector.Parameters[0].Type;
            var collectionType = GetSequenceElementType(collectionSelector.Body.Type);
            var resultType = resultSelector?.Body.Type ?? collectionType;
            var objParam = Expression.Parameter(typeof(object), "row");
            var castRow = Expression.Convert(objParam, inputType);
            var collectionBody = new ParameterReplacer(collectionSelector.Parameters[0], castRow).Visit(collectionSelector.Body)!;
            var boxedCollection = Expression.Convert(collectionBody, typeof(System.Collections.IEnumerable));
            var collectionFunc = Expression.Lambda<Func<object, System.Collections.IEnumerable?>>(boxedCollection, objParam).Compile();

            Func<object, object?, object?> resultFunc;
            if (resultSelector != null)
            {
                var outerObj = Expression.Parameter(typeof(object), "outer");
                var innerObj = Expression.Parameter(typeof(object), "inner");
                var outerCast = Expression.Convert(outerObj, inputType);
                var innerCast = collectionType.IsValueType ? (Expression)Expression.Convert(innerObj, collectionType) : Expression.TypeAs(innerObj, collectionType);
                var body = new ParameterReplacer(resultSelector.Parameters[0], outerCast).Visit(resultSelector.Body)!;
                body = new ParameterReplacer(resultSelector.Parameters[1], innerCast).Visit(body)!;
                var boxed = resultType.IsValueType ? (Expression)Expression.Convert(body, typeof(object)) : body;
                resultFunc = Expression.Lambda<Func<object, object?, object?>>(boxed, outerObj, innerObj).Compile();
            }
            else
            {
                resultFunc = static (_, inner) => inner;
            }

            AppendPostMaterializeTransform((ctx, rows) =>
            {
                var listType = typeof(List<>).MakeGenericType(resultType);
                var result = (System.Collections.IList)Activator.CreateInstance(listType)!;
                foreach (var row in rows)
                {
                    var values = collectionFunc(row!);
                    if (values == null)
                        continue;
                    foreach (var value in values)
                        result.Add(resultFunc(row!, value));
                }
                return result;
            }, resultType);
        }

        private static Type GetSequenceElementType(Type type)
        {
            if (type.IsArray)
                return type.GetElementType()!;
            if (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(IEnumerable<>))
                return type.GetGenericArguments()[0];
            var iface = type.GetInterfaces().FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
            return iface?.GetGenericArguments()[0] ?? typeof(object);
        }

        private void AppendPostMaterializeSelect(LambdaExpression selector)
        {
            var inputType = CurrentPostMaterializeElementType;
            if (inputType == null || selector.Parameters.Count != 1 || selector.Parameters[0].Type != inputType)
                return;

            var resultType = selector.Body.Type;
            AppendPostMaterializeTransform((ctx, rows) =>
            {
                var item = Expression.Parameter(typeof(object), "item");
                var clientBody = new PostMaterializeQuerySourceReplacer(ctx).Visit(selector.Body)!;
                var body = new nORM.Internal.ParameterReplacer(selector.Parameters[0], Expression.Convert(item, inputType)).Visit(clientBody)!;
                var projector = Expression.Lambda<System.Func<object, object>>(Expression.Convert(body, typeof(object)), item).Compile();
                var output = CreateRuntimeList(resultType, rows.Count);
                var seen = new HashSet<object>();
                foreach (var row in rows)
                {
                    HydratePostMaterializeObject(ctx, row, seen);
                    output.Add(projector(row!));
                }
                return output;
            }, resultType);
        }

        private void AppendPostMaterializeOrder(LambdaExpression keySelector, bool ascending, bool thenBy = false)
        {
            var inputType = CurrentPostMaterializeElementType;
            if (inputType == null || keySelector.Parameters.Count != 1 || keySelector.Parameters[0].Type != inputType)
                return;

            var item = Expression.Parameter(typeof(object), "item");
            var body = new nORM.Internal.ParameterReplacer(keySelector.Parameters[0], Expression.Convert(item, inputType)).Visit(keySelector.Body)!;
            var keyReader = Expression.Lambda<System.Func<object, object>>(Expression.Convert(body, typeof(object)), item).Compile();

            if (!thenBy || _postMaterializeOrderings == null)
            {
                _postMaterializeOrderPrefixTransform = _postMaterializeTransform;
                _postMaterializeOrderings = new List<(System.Func<object, object> KeyReader, bool Ascending)>(4);
            }
            _postMaterializeOrderings.Add((keyReader, ascending));

            System.Collections.IList ApplyOrdering(System.Collections.IList rows)
            {
                IOrderedEnumerable<object>? ordered = null;
                foreach (var ordering in _postMaterializeOrderings!)
                {
                    ordered = ordered == null
                        ? ordering.Ascending
                            ? rows.Cast<object>().OrderBy(ordering.KeyReader)
                            : rows.Cast<object>().OrderByDescending(ordering.KeyReader)
                        : ordering.Ascending
                            ? ordered.ThenBy(ordering.KeyReader)
                            : ordered.ThenByDescending(ordering.KeyReader);
                }

                var orderedRows = (ordered ?? rows.Cast<object>().OrderBy(static x => 0)).ToArray();
                var output = CreateRuntimeList(inputType, orderedRows.Length);
                foreach (var row in orderedRows)
                    output.Add(row);
                return output;
            }

            var prefix = _postMaterializeOrderPrefixTransform;
            _postMaterializeTransform = prefix == null
                ? (ctx, rows) => ApplyOrdering(rows)
                : (ctx, rows) => ApplyOrdering(prefix(ctx, rows));
            _postMaterializeElementType = inputType;
        }

        private void AppendPostMaterializeTake(int count)
        {
            var inputType = CurrentPostMaterializeElementType;
            if (inputType == null)
                return;

            AppendPostMaterializeTransform((ctx, rows) =>
            {
                var take = Math.Min(Math.Max(count, 0), rows.Count);
                var output = CreateRuntimeList(inputType, take);
                for (var i = 0; i < take; i++)
                    output.Add(rows[i]);
                return output;
            }, inputType);
        }

        private static void HydratePostMaterializeObject(DbContext ctx, object? value, HashSet<object> seen)
        {
            if (value == null || value is string || !value.GetType().IsClass)
                return;
            if (!seen.Add(value))
                return;

            var type = value.GetType();
            if (ctx.IsMapped(type))
            {
                var map = ctx.GetMapping(type);
                foreach (var relation in map.Relations.Values)
                {
                    var current = relation.NavProp.GetValue(value) as System.Collections.IList;
                    if (current == null || current.Count == 0)
                    {
                        var pk = GetHydrationRelationKeyValue(relation.PrincipalKeys, value);
                        var loaded = CreateRuntimeList(relation.DependentType, 0);
                        foreach (var child in EnumerateQuery(ctx, relation.DependentType))
                        {
                            var fk = GetHydrationRelationKeyValue(relation.ForeignKeys, child);
                            if (object.Equals(pk, fk))
                                loaded.Add(child);
                        }
                        relation.NavProp.SetValue(value, loaded);
                        current = loaded;
                    }
                    foreach (var child in current)
                        HydratePostMaterializeObject(ctx, child, seen);
                }

                foreach (var prop in type.GetProperties().Where(p => p.CanRead && p.CanWrite && p.GetIndexParameters().Length == 0))
                {
                    var navType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                    if (navType == typeof(string) || !navType.IsClass || !ctx.IsMapped(navType) || typeof(System.Collections.IEnumerable).IsAssignableFrom(navType))
                        continue;
                    if (prop.GetValue(value) != null)
                        continue;

                    var relatedMap = ctx.GetMapping(navType);
                    var fkProp = type.GetProperty(prop.Name + "Id") ?? type.GetProperty(navType.Name + "Id");
                    if (fkProp != null && relatedMap.KeyColumns.Length == 1)
                    {
                        var fk = fkProp.GetValue(value);
                        foreach (var related in EnumerateQuery(ctx, navType))
                        {
                            if (object.Equals(fk, relatedMap.KeyColumns[0].Getter(related)))
                            {
                                prop.SetValue(value, related);
                                HydratePostMaterializeObject(ctx, related, seen);
                                break;
                            }
                        }
                        continue;
                    }

                    var principalName = type.Name;
                    if (principalName.Length > 3 && principalName.StartsWith("Crr", StringComparison.Ordinal))
                        principalName = principalName.Substring(3);
                    var inverseFk = relatedMap.Columns.FirstOrDefault(c => string.Equals(c.PropName, principalName + "Id", StringComparison.OrdinalIgnoreCase));
                    if (inverseFk != null && map.KeyColumns.Length == 1)
                    {
                        var pk = map.KeyColumns[0].Getter(value);
                        foreach (var related in EnumerateQuery(ctx, navType))
                        {
                            if (object.Equals(pk, inverseFk.Getter(related)))
                            {
                                prop.SetValue(value, related);
                                HydratePostMaterializeObject(ctx, related, seen);
                                break;
                            }
                        }
                    }
                }
            }
            else
            {
                foreach (var prop in type.GetProperties().Where(p => p.CanRead && p.GetIndexParameters().Length == 0))
                {
                    var propValue = prop.GetValue(value);
                    if (propValue is System.Collections.IEnumerable en && propValue is not string)
                    {
                        foreach (var item in en)
                            HydratePostMaterializeObject(ctx, item, seen);
                    }
                    else
                    {
                        HydratePostMaterializeObject(ctx, propValue, seen);
                    }
                }
            }
        }

        private static System.Collections.IEnumerable EnumerateQuery(DbContext ctx, Type entityType)
        {
            var queryMethod = typeof(NormQueryable).GetMethod(nameof(NormQueryable.Query))!.MakeGenericMethod(entityType);
            return (System.Collections.IEnumerable)queryMethod.Invoke(null, new object[] { ctx })!;
        }

        private static object? GetHydrationRelationKeyValue(IReadOnlyList<Column> columns, object entity)
        {
            if (columns.Count == 1)
                return columns[0].Getter(entity);

            var values = new object?[columns.Count];
            for (var i = 0; i < columns.Count; i++)
            {
                values[i] = columns[i].Getter(entity);
                if (values[i] == null)
                    return null;
            }

            return new HydrationRelationKey(values);
        }

        private sealed class HydrationRelationKey : IEquatable<HydrationRelationKey>
        {
            private readonly object?[] _values;

            public HydrationRelationKey(object?[] values) => _values = values;

            public bool Equals(HydrationRelationKey? other)
            {
                if (other == null || other._values.Length != _values.Length)
                    return false;
                for (var i = 0; i < _values.Length; i++)
                {
                    if (!object.Equals(_values[i], other._values[i]))
                        return false;
                }
                return true;
            }

            public override bool Equals(object? obj) => Equals(obj as HydrationRelationKey);

            public override int GetHashCode()
            {
                var hash = new HashCode();
                foreach (var value in _values)
                    hash.Add(value);
                return hash.ToHashCode();
            }
        }

        private sealed class PostMaterializeQuerySourceReplacer : ExpressionVisitor
        {
            private readonly DbContext _ctx;
            public PostMaterializeQuerySourceReplacer(DbContext ctx) => _ctx = ctx;

            protected override Expression VisitConstant(ConstantExpression node)
            {
                if (node.Value != null
                    && typeof(IQueryable).IsAssignableFrom(node.Type)
                    && node.Value.GetType().FullName?.StartsWith("nORM.Core.NormQueryable", StringComparison.Ordinal) == true)
                {
                    var entityType = node.Type.GetGenericArguments().FirstOrDefault()
                        ?? node.Value.GetType().GetInterfaces()
                            .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>))
                            ?.GetGenericArguments()[0];

                    if (entityType != null)
                        return BuildCurrentContextQueryable(entityType);
                }

                return base.VisitConstant(node);
            }

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.DeclaringType == typeof(NormQueryable)
                    && node.Method.Name == nameof(NormQueryable.Query)
                    && node.Method.IsGenericMethod)
                {
                    var entityType = node.Method.GetGenericArguments()[0];
                    return BuildCurrentContextQueryable(entityType);
                }

                return base.VisitMethodCall(node);
            }

            private Expression BuildCurrentContextQueryable(Type entityType)
            {
                var items = EnumerateQuery(_ctx, entityType).Cast<object>().ToArray();
                var list = CreateRuntimeList(entityType, items.Length);
                foreach (var item in items)
                    list.Add(item);
                var queryable = typeof(Queryable).GetMethods()
                    .First(m => m.Name == nameof(Queryable.AsQueryable) && m.IsGenericMethod && m.GetParameters().Length == 1)
                    .MakeGenericMethod(entityType)
                    .Invoke(null, new object[] { list })!;
                return Expression.Constant(queryable, typeof(IQueryable<>).MakeGenericType(entityType));
            }
        }

        private static System.Collections.IList CreateRuntimeList(Type elementType, int capacity)
        {
            return (System.Collections.IList)Activator.CreateInstance(typeof(List<>).MakeGenericType(elementType), capacity)!;
        }
    }
}
