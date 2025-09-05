using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Mapping;
using nORM.SourceGeneration;

namespace nORM.Query
{
    /// <summary>
    /// Creates and caches materializers used to project <see cref="DbDataReader"/> rows into objects.
    /// </summary>
    internal sealed class MaterializerFactory
    {
        private static readonly ConcurrentLruCache<(int MappingTypeHash, int TargetTypeHash, int ProjectionHash, string TableName), Func<DbDataReader, CancellationToken, Task<object>>> _cache
            = new(maxSize: 1000, timeToLive: TimeSpan.FromMinutes(10));

        // Cache constructor info and delegates to avoid repeated reflection in hot paths
        private static readonly ConcurrentDictionary<Type, ConstructorInfo> _constructorCache = new();
        private static readonly ConcurrentDictionary<Type, Func<object?[], object>> _constructorDelegates = new();

        internal static (long Hits, long Misses, double HitRate) CacheStats
            => (_cache.Hits, _cache.Misses, _cache.HitRate);

        public Func<DbDataReader, CancellationToken, Task<object>> CreateMaterializer(TableMapping mapping, Type targetType, LambdaExpression? projection = null)
        {
            var projectionHash = projection != null ? ExpressionFingerprint.Compute(projection) : 0;
            var cacheKey = (mapping.Type.GetHashCode(), targetType.GetHashCode(), projectionHash, mapping.TableName);

            if (projection == null && CompiledMaterializerStore.TryGet(targetType, out var precompiled))
            {
                _cache.GetOrAdd(cacheKey, _ => precompiled);
                return precompiled;
            }

            return _cache.GetOrAdd(cacheKey, _ =>
            {
                var sync = CreateMaterializerInternal(mapping, targetType, projection);
                ValidateMaterializer(sync, mapping, targetType);
                return (reader, ct) => Task.FromResult(sync(reader));
            });
        }

        private Func<DbDataReader, object> CreateMaterializerInternal(TableMapping mapping, Type targetType, LambdaExpression? projection = null, bool ignoreTph = false)
        {
            if (!ignoreTph && mapping.DiscriminatorColumn != null && mapping.TphMappings.Count > 0 && projection == null)
            {
                var discIndex = Array.IndexOf(mapping.Columns, mapping.DiscriminatorColumn);
                var baseMat = CreateMaterializerInternal(mapping, targetType, null, true);
                var derivedMats = mapping.TphMappings.ToDictionary(
                    kvp => kvp.Key,
                    kvp =>
                    {
                        var dmap = kvp.Value;
                        var indices = dmap.Columns.Select(c => Array.FindIndex(mapping.Columns, bc => bc.Prop.Name == c.Prop.Name)).ToArray();
                        return (Func<DbDataReader, object>)(reader =>
                        {
                            var entity = Activator.CreateInstance(dmap.Type)!;
                            for (int i = 0; i < dmap.Columns.Length; i++)
                            {
                                var col = dmap.Columns[i];
                                var idx = indices[i];
                                if (reader.IsDBNull(idx)) continue;
                                var readerMethod = Methods.GetReaderMethod(col.Prop.PropertyType);
                                var value = readerMethod.Invoke(reader, new object[] { idx });
                                if (readerMethod == Methods.GetValue)
                                    value = Convert.ChangeType(value!, col.Prop.PropertyType);
                                col.Setter(entity, value);
                            }
                            return entity;
                        });
                    });
                return reader =>
                {
                    var disc = reader.GetValue(discIndex);
                    if (disc != null && derivedMats.TryGetValue(disc, out var mat))
                        return mat(reader);
                    return baseMat(reader);
                };
            }

            // Handle simple scalar types directly
            if (targetType.IsPrimitive || targetType == typeof(decimal) || targetType == typeof(string))
            {
                return reader =>
                {
                    if (reader.IsDBNull(0))
                        return targetType.IsValueType ? Activator.CreateInstance(targetType)! : null!;
                    var read = Methods.GetReaderMethod(targetType);
                    var value = read.Invoke(reader, new object[] { 0 });
                    if (read == Methods.GetValue)
                        value = Convert.ChangeType(value!, targetType);
                    return value!;
                };
            }

            var columns = projection == null
                ? mapping.Columns
                : ExtractColumnsFromProjection(mapping, projection);

            var parameterlessCtor = targetType.GetConstructor(Type.EmptyTypes);

            if (parameterlessCtor != null && columns.All(c => c.Prop.DeclaringType == targetType && c.Prop.GetSetMethod() != null))
            {
                return CreateOptimizedMaterializer(columns, targetType);
            }
            if (parameterlessCtor != null)
            {
                return reader =>
                {
                    var entity = parameterlessCtor.Invoke(null);
                    for (int i = 0; i < columns.Length; i++)
                    {
                        if (reader.IsDBNull(i)) continue;
                        var col = columns[i];
                        var readerMethod = Methods.GetReaderMethod(col.Prop.PropertyType);
                        var value = readerMethod.Invoke(reader, new object[] { i });
                        if (readerMethod == Methods.GetValue)
                            value = Convert.ChangeType(value!, col.Prop.PropertyType);
                        col.Setter(entity, value);
                    }
                    return entity!;
                };
            }

            // Constructor with parameters (record types, anonymous types, etc.)
            var ctor = GetCachedConstructor(targetType, columns);
            var ctorParams = ctor.GetParameters();
            var ctorDelegate = _constructorDelegates.GetOrAdd(targetType, _ => CreateConstructorDelegate(ctor));

            return reader =>
            {
                var args = new object?[columns.Length];
                for (int i = 0; i < columns.Length; i++)
                {
                    if (reader.IsDBNull(i))
                    {
                        args[i] = ctorParams[i].ParameterType.IsValueType ? Activator.CreateInstance(ctorParams[i].ParameterType) : null;
                        continue;
                    }
                    var paramType = ctorParams[i].ParameterType;
                    var readerMethod = Methods.GetReaderMethod(paramType);
                    var value = readerMethod.Invoke(reader, new object[] { i });
                    if (readerMethod == Methods.GetValue)
                        value = Convert.ChangeType(value!, paramType);
                    args[i] = value;
                }
                return ctorDelegate(args)!;
            };
        }

        private static ConstructorInfo GetCachedConstructor(Type type, Column[] columns)
        {
            return _constructorCache.GetOrAdd(type, t =>
            {
                return t.GetConstructors()
                    .FirstOrDefault(c =>
                    {
                        var parameters = c.GetParameters();
                        if (parameters.Length != columns.Length) return false;

                        for (int i = 0; i < parameters.Length; i++)
                        {
                            if (!string.Equals(parameters[i].Name, columns[i].Prop.Name, StringComparison.OrdinalIgnoreCase))
                                return false;
                        }
                        return true;
                    }) ?? throw new InvalidOperationException($"No suitable constructor for {type}");
            });
        }

        private static Func<object?[], object> CreateConstructorDelegate(ConstructorInfo ctor)
        {
            var argsParam = Expression.Parameter(typeof(object[]), "args");
            var ctorParams = ctor.GetParameters();
            var argExprs = new Expression[ctorParams.Length];
            for (int i = 0; i < ctorParams.Length; i++)
            {
                var index = Expression.Constant(i);
                var access = Expression.ArrayIndex(argsParam, index);
                var convert = Expression.Convert(access, ctorParams[i].ParameterType);
                argExprs[i] = convert;
            }
            var newExpr = Expression.New(ctor, argExprs);
            var body = Expression.Convert(newExpr, typeof(object));
            var lambda = Expression.Lambda<Func<object?[], object>>(body, argsParam);
            return lambda.Compile();
        }

        private static Column[] ExtractColumnsFromProjection(TableMapping mapping, LambdaExpression projection)
        {
            if (projection.Body is NewExpression newExpr)
            {
                var cols = new List<Column>(newExpr.Arguments.Count);
                for (int i = 0; i < newExpr.Arguments.Count; i++)
                {
                    var arg = newExpr.Arguments[i];
                    if (arg is MemberExpression m)
                    {
                        // Try to resolve against the current mapping first
                        var col = mapping.Columns.FirstOrDefault(c => c.Prop.Name == m.Member.Name);
                        if (col != null)
                        {
                            cols.Add(col);
                        }
                        else if (m.Member is PropertyInfo pi)
                        {
                            // Create a lightweight column for properties from other mappings
                            cols.Add(new Column(pi, mapping.Provider, null));
                        }
                    }
                    else if (arg is ParameterExpression p)
                    {
                        var memberName = newExpr.Members![i].Name;
                        cols.Add(new Column(memberName, p.Type, mapping.Type, mapping.Provider, memberName));
                    }
                }
                return cols.ToArray();
            }
            return mapping.Columns;
        }

        private static Func<DbDataReader, object> CreateOptimizedMaterializer(Column[] columns, Type targetType)
        {
            var readerParam = Expression.Parameter(typeof(DbDataReader), "reader");
            var entityVar = Expression.Variable(targetType, "entity");
            var expressions = new List<Expression>
            {
                Expression.Assign(entityVar, Expression.New(targetType))
            };

            for (int i = 0; i < columns.Length; i++)
            {
                var column = columns[i];
                var isNullCheck = Expression.Call(readerParam, Methods.IsDbNull, Expression.Constant(i));
                var getValue = GetOptimizedReaderCall(readerParam, column.Prop.PropertyType, i);
                var setProperty = Expression.Call(entityVar, column.Prop.GetSetMethod()!, getValue);
                var conditionalSet = Expression.IfThen(Expression.Not(isNullCheck), setProperty);
                expressions.Add(conditionalSet);
            }

            expressions.Add(Expression.Convert(entityVar, typeof(object)));
            var block = Expression.Block(new[] { entityVar }, expressions);
            return Expression.Lambda<Func<DbDataReader, object>>(block, readerParam).Compile();
        }

        private static void ValidateMaterializer(Func<DbDataReader, object> materializer, TableMapping mapping, Type targetType)
        {
            _ = targetType;
            using var reader = new ValidationDbDataReader(mapping.Columns.Length);
            materializer(reader);
        }

        private sealed class ValidationDbDataReader : DbDataReader
        {
            private readonly int _fieldCount;

            public ValidationDbDataReader(int fieldCount)
            {
                _fieldCount = fieldCount;
            }

            public override int FieldCount => _fieldCount;

            public override bool IsDBNull(int ordinal) => true;

            public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => Task.FromResult(true);

            public override object GetValue(int ordinal) => DBNull.Value;

            public override int GetValues(object[] values)
            {
                Array.Fill(values, DBNull.Value);
                return Math.Min(values.Length, _fieldCount);
            }

            public override string GetName(int ordinal) => throw new NotSupportedException();

            public override int GetOrdinal(string name) => throw new NotSupportedException();

            public override string GetDataTypeName(int ordinal) => nameof(Object);

            public override Type GetFieldType(int ordinal) => typeof(object);

            public override bool HasRows => false;

            public override bool IsClosed => false;

            public override int RecordsAffected => 0;

            public override object this[int ordinal] => DBNull.Value;

            public override object this[string name] => DBNull.Value;

            public override IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();

            public override bool Read() => false;

            public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(false);

            public override bool NextResult() => false;

            public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);

            public override int Depth => 0;

            public override int VisibleFieldCount => _fieldCount;

            public override bool GetBoolean(int ordinal) => default;
            public override byte GetByte(int ordinal) => default;
            public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => 0;
            public override char GetChar(int ordinal) => default;
            public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => 0;
            public override Guid GetGuid(int ordinal) => default;
            public override short GetInt16(int ordinal) => default;
            public override int GetInt32(int ordinal) => default;
            public override long GetInt64(int ordinal) => default;
            public override DateTime GetDateTime(int ordinal) => default;
            public override decimal GetDecimal(int ordinal) => default;
            public override double GetDouble(int ordinal) => default;
            public override float GetFloat(int ordinal) => default;
            public override string GetString(int ordinal) => string.Empty;

            public override T GetFieldValue<T>(int ordinal) => default!;

            public override Task<T> GetFieldValueAsync<T>(int ordinal, CancellationToken cancellationToken) => Task.FromResult(default(T)!);

            public override System.Data.DataTable GetSchemaTable() => throw new NotSupportedException();
        }

        private static Expression GetOptimizedReaderCall(ParameterExpression reader, Type propertyType, int index)
        {
            var underlyingType = Nullable.GetUnderlyingType(propertyType) ?? propertyType;

            Expression call = underlyingType.Name switch
            {
                nameof(Int32) => Expression.Call(reader, Methods.GetInt32, Expression.Constant(index)),
                nameof(String) => Expression.Call(reader, Methods.GetString, Expression.Constant(index)),
                nameof(DateTime) => Expression.Call(reader, Methods.GetDateTime, Expression.Constant(index)),
                nameof(Boolean) => Expression.Call(reader, Methods.GetBoolean, Expression.Constant(index)),
                _ => Expression.Call(reader, Methods.GetValue, Expression.Constant(index))
            };

            if (call.Type != propertyType)
                call = Expression.Convert(call, propertyType);

            return call;
        }

    }
}
