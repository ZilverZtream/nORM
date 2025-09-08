using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Mapping;
using nORM.SourceGeneration;
using nORM.Core;

namespace nORM.Query
{
    /// <summary>
    /// Creates and caches materializers used to project <see cref="DbDataReader"/> rows into objects.
    /// </summary>
    internal sealed class MaterializerFactory
    {
        private const int DefaultCacheSize = 1000;
        private static readonly ConcurrentLruCache<(int MappingTypeHash, int TargetTypeHash, int ProjectionHash, string TableName), Func<DbDataReader, CancellationToken, Task<object>>> _cache
            = new(maxSize: DefaultCacheSize, timeToLive: TimeSpan.FromMinutes(10));

        // Cache constructor info and delegates to avoid repeated reflection in hot paths
        private static readonly ConcurrentDictionary<Type, ConstructorInfo> _constructorCache = new();
        private static readonly ConcurrentDictionary<Type, Func<object?[], object>> _constructorDelegates = new();
        private static readonly ConcurrentDictionary<Type, ConstructorInfo?> _parameterlessCtorCache = new();
        private static readonly ConcurrentDictionary<Type, Func<object>> _parameterlessCtorDelegates = new();
        private static readonly ConcurrentDictionary<Type, bool> _simpleTypeCache = new();
        private static readonly ConcurrentDictionary<Type, Func<DbDataReader, object>> _fastMaterializers = new();
        private static readonly ConcurrentDictionary<Type, Action<object, DbDataReader>[]> _setterCache = new();

        private static bool IsSimpleType(Type type)
            => _simpleTypeCache.GetOrAdd(type, static t => t.IsPrimitive || t == typeof(decimal) || t == typeof(string));

        internal static (long Hits, long Misses, double HitRate) CacheStats
            => (_cache.Hits, _cache.Misses, _cache.HitRate);

        public static void PrecompileCommonPatterns<T>() where T : class
        {
            var key = typeof(T);
            if (!_fastMaterializers.ContainsKey(key))
            {
                _fastMaterializers[key] = CreateILMaterializer<T>();
            }
        }

        private static object? ConvertDbValue(object dbValue, Type targetType)
        {
            if (dbValue == null || dbValue is DBNull)
            {
                return null;
            }

            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

            if (dbValue.GetType() == underlyingType)
            {
                return dbValue;
            }

            return Convert.ChangeType(dbValue, underlyingType);
        }

        private static Func<DbDataReader, object> CreateILMaterializer<T>() where T : class
        {
            var type = typeof(T);
            var method = new DynamicMethod("Materialize", typeof(object), new[] { typeof(DbDataReader) }, typeof(MaterializerFactory), true);
            var il = method.GetILGenerator();

            var parameterlessCtor = type.GetConstructor(Type.EmptyTypes);
            if (parameterlessCtor != null)
            {
                var props = type.GetProperties(BindingFlags.Instance | BindingFlags.Public);

                il.DeclareLocal(type); // local 0: entity
                il.Emit(OpCodes.Newobj, parameterlessCtor);
                il.Emit(OpCodes.Stloc_0);

                for (int i = 0; i < props.Length; i++)
                {
                    var prop = props[i];
                    if (!prop.CanWrite) continue;

                    var skip = il.DefineLabel();
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, i);
                    il.Emit(OpCodes.Callvirt, Methods.IsDbNull);
                    il.Emit(OpCodes.Brtrue_S, skip);

                    il.Emit(OpCodes.Ldloc_0);
                    il.Emit(OpCodes.Ldarg_0);
                    il.Emit(OpCodes.Ldc_I4, i);
                    var readerMethod = Methods.GetReaderMethod(prop.PropertyType);
                    il.Emit(OpCodes.Callvirt, readerMethod);

                    var pType = prop.PropertyType;
                    var underlying = Nullable.GetUnderlyingType(pType);
                    if (underlying != null)
                    {
                        if (readerMethod == Methods.GetValue)
                        {
                            il.Emit(OpCodes.Ldtoken, underlying);
                            il.Emit(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle))!);
                            il.Emit(OpCodes.Call, typeof(Convert).GetMethod(nameof(Convert.ChangeType), new[] { typeof(object), typeof(Type) })!);
                            il.Emit(OpCodes.Unbox_Any, underlying);
                        }
                        else if (readerMethod.ReturnType != underlying)
                        {
                            il.Emit(OpCodes.Unbox_Any, underlying);
                        }
                        var ctorNullable = pType.GetConstructor(new[] { underlying })!;
                        il.Emit(OpCodes.Newobj, ctorNullable);
                    }
                    else if (pType.IsValueType)
                    {
                        if (readerMethod.ReturnType == typeof(object))
                        {
                            il.Emit(OpCodes.Ldtoken, pType);
                            il.Emit(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle))!);
                            il.Emit(OpCodes.Call, typeof(Convert).GetMethod(nameof(Convert.ChangeType), new[] { typeof(object), typeof(Type) })!);
                        }
                        il.Emit(OpCodes.Unbox_Any, pType);
                    }
                    else if (readerMethod.ReturnType != pType)
                    {
                        if (readerMethod == Methods.GetValue)
                        {
                            il.Emit(OpCodes.Ldtoken, pType);
                            il.Emit(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle))!);
                            il.Emit(OpCodes.Call, typeof(Convert).GetMethod(nameof(Convert.ChangeType), new[] { typeof(object), typeof(Type) })!);
                        }
                        il.Emit(OpCodes.Castclass, pType);
                    }

                    il.Emit(OpCodes.Callvirt, prop.GetSetMethod()!);
                    il.MarkLabel(skip);
                }

                il.Emit(OpCodes.Ldloc_0);
                il.Emit(OpCodes.Ret);
                return (Func<DbDataReader, object>)method.CreateDelegate(typeof(Func<DbDataReader, object>));
            }

            // Parameterized constructor path
            var ctor = type.GetConstructors().OrderByDescending(c => c.GetParameters().Length).First();
            var parameters = ctor.GetParameters();
            var getOrdinal = typeof(DbDataReader).GetMethod(nameof(DbDataReader.GetOrdinal))!;
            var ordinals = new LocalBuilder[parameters.Length];

            for (int i = 0; i < parameters.Length; i++)
            {
                ordinals[i] = il.DeclareLocal(typeof(int));
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldstr, parameters[i].Name!);
                il.Emit(OpCodes.Callvirt, getOrdinal);
                il.Emit(OpCodes.Stloc, ordinals[i]);
            }

            var convertMethod = typeof(MaterializerFactory).GetMethod(nameof(ConvertDbValue), BindingFlags.NonPublic | BindingFlags.Static)!;

            for (int i = 0; i < parameters.Length; i++)
            {
                var param = parameters[i];
                var pType = param.ParameterType;
                var skip = il.DefineLabel();
                var end = il.DefineLabel();

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldloc, ordinals[i]);
                il.Emit(OpCodes.Callvirt, Methods.IsDbNull);
                il.Emit(OpCodes.Brtrue_S, skip);

                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldloc, ordinals[i]);
                il.Emit(OpCodes.Callvirt, Methods.GetValue);
                il.Emit(OpCodes.Ldtoken, pType);
                il.Emit(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle))!);
                il.Emit(OpCodes.Call, convertMethod);

                if (pType.IsValueType)
                {
                    il.Emit(OpCodes.Unbox_Any, pType);
                }
                else
                {
                    il.Emit(OpCodes.Castclass, pType);
                }

                il.Emit(OpCodes.Br_S, end);

                il.MarkLabel(skip);
                if (pType.IsValueType)
                {
                    var loc = il.DeclareLocal(pType);
                    il.Emit(OpCodes.Ldloca_S, loc);
                    il.Emit(OpCodes.Initobj, pType);
                    il.Emit(OpCodes.Ldloc, loc);
                }
                else
                {
                    il.Emit(OpCodes.Ldnull);
                }
                il.MarkLabel(end);
            }

            il.Emit(OpCodes.Newobj, ctor);
            il.Emit(OpCodes.Ret);

            return (Func<DbDataReader, object>)method.CreateDelegate(typeof(Func<DbDataReader, object>));
        }

        public Func<DbDataReader, CancellationToken, Task<object>> CreateMaterializer(TableMapping mapping, Type targetType, LambdaExpression? projection = null)
        {
            // CHECK FOR COMPILED MATERIALIZER FIRST
            if (projection == null && CompiledMaterializerStore.TryGet(targetType, out var compiled))
            {
                return compiled;
            }

            // Also check the cache with the new key structure
            var projectionHash = projection != null ? ExpressionFingerprint.Compute(projection) : 0;
            var cacheKey = (mapping.Type.GetHashCode(), targetType.GetHashCode(), projectionHash, mapping.TableName);

            return _cache.GetOrAdd(cacheKey, _ =>
            {
                // For simple entity materialization without projection, prefer fast materializers
                if (projection == null && _fastMaterializers.TryGetValue(targetType, out var fast))
                {
                    return (reader, ct) =>
                    {
                        ct.ThrowIfCancellationRequested();
                        return Task.FromResult(fast(reader));
                    };
                }

                // Fall back to existing reflection-based approach
                var sync = CreateMaterializerInternal(mapping, targetType, projection);
                ValidateMaterializer(sync, mapping, targetType);
                return (reader, ct) =>
                {
                    ct.ThrowIfCancellationRequested();
                    return Task.FromResult(sync(reader));
                };
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
                        var indices = dmap.Columns.Select(c => Array.IndexOf(mapping.Columns, mapping.ColumnsByName[c.Prop.Name])).ToArray();
                        var getters = dmap.Columns.Select((c, i) => CreateReaderGetter(c.Prop.PropertyType, indices[i])).ToArray();
                        return (Func<DbDataReader, object>)(reader =>
                        {
                            var entity = Activator.CreateInstance(dmap.Type)!;
                            for (int i = 0; i < dmap.Columns.Length; i++)
                            {
                                var idx = indices[i];
                                if (reader.IsDBNull(idx)) continue;
                                var value = getters[i](reader);
                                dmap.Columns[i].Setter(entity, value);
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
            if (IsSimpleType(targetType))
            {
                var getter = CreateReaderGetter(targetType, 0);
                return reader =>
                {
                    if (reader.IsDBNull(0))
                        return targetType.IsValueType ? Activator.CreateInstance(targetType)! : null!;
                    return getter(reader)!;
                };
            }

            var columns = projection == null
                ? mapping.Columns
                : ExtractColumnsFromProjection(mapping, projection);

            var parameterlessCtor = _parameterlessCtorCache.GetOrAdd(targetType, t => t.GetConstructor(Type.EmptyTypes));

            if (parameterlessCtor != null && columns.All(c => c.Prop.DeclaringType == targetType && c.Prop.GetSetMethod() != null))
            {
                return CreateOptimizedMaterializer(columns, targetType);
            }
            if (parameterlessCtor != null)
            {
                var parameterlessCtorDelegate = _parameterlessCtorDelegates.GetOrAdd(targetType, t =>
                {
                    var newExpr = Expression.New(t);
                    var body = Expression.Convert(newExpr, typeof(object));
                    return Expression.Lambda<Func<object>>(body).Compile();
                });

                var properties = targetType.GetProperties();
                var canOptimize = columns.Length <= properties.Length;
                for (int i = 0; i < columns.Length && i < properties.Length; i++)
                {
                    if (columns[i].Prop != properties[i])
                    {
                        canOptimize = false;
                        break;
                    }
                }

                if (canOptimize)
                {
                    var setters = GetOptimizedSetters(targetType);
                    return reader =>
                    {
                        var entity = parameterlessCtorDelegate();
                        for (int i = 0; i < columns.Length && i < setters.Length; i++)
                        {
                            setters[i](entity, reader);
                        }
                        return entity!;
                    };
                }

                var getters = columns.Select((c, i) => CreateReaderGetter(c.Prop.PropertyType, i)).ToArray();
                return reader =>
                {
                    var entity = parameterlessCtorDelegate();
                    for (int i = 0; i < columns.Length; i++)
                    {
                        if (reader.IsDBNull(i)) continue;
                        var col = columns[i];
                        var value = getters[i](reader);
                        col.Setter(entity, value);
                    }
                    return entity!;
                };
            }

            // Constructor with parameters (record types, anonymous types, etc.)
            var ctor = GetCachedConstructor(targetType, columns);
            var ctorParams = ctor.GetParameters();
            var ctorDelegate = _constructorDelegates.GetOrAdd(targetType, _ => CreateConstructorDelegate(ctor));
            var paramGetters = ctorParams.Select((p, i) => CreateReaderGetter(p.ParameterType, i)).ToArray();

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
                    args[i] = paramGetters[i](reader);
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
                        if (mapping.ColumnsByName.TryGetValue(m.Member.Name, out var col))
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

        private static Func<DbDataReader, object> CreateReaderGetter(Type type, int index)
        {
            var readerParam = Expression.Parameter(typeof(DbDataReader), "reader");
            var valueExpr = GetOptimizedReaderCall(readerParam, type, index);
            var body = Expression.Convert(valueExpr, typeof(object));
            return Expression.Lambda<Func<DbDataReader, object>>(body, readerParam).Compile();
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
                nameof(Int32)    => Expression.Call(reader, Methods.GetInt32, Expression.Constant(index)),
                nameof(String)   => Expression.Call(reader, Methods.GetString, Expression.Constant(index)),
                nameof(DateTime) => Expression.Call(reader, Methods.GetDateTime, Expression.Constant(index)),
                nameof(Boolean)  => Expression.Call(reader, Methods.GetBoolean, Expression.Constant(index)),
                nameof(Decimal)  => Expression.Call(reader, Methods.GetDecimal, Expression.Constant(index)),
                _ => Expression.Convert(
                        Expression.Call(reader, Methods.GetValue, Expression.Constant(index)),
                        underlyingType)
            };

            if (call.Type != propertyType)
                call = Expression.Convert(call, propertyType);

            return call;
        }

        private static Action<object, DbDataReader>[] GetOptimizedSetters<T>()
            => GetOptimizedSetters(typeof(T));

        private static Action<object, DbDataReader>[] GetOptimizedSetters(Type type)
        {
            return _setterCache.GetOrAdd(type, t =>
            {
                var properties = t.GetProperties();
                var setters = new Action<object, DbDataReader>[properties.Length];

                for (int i = 0; i < properties.Length; i++)
                {
                    var prop = properties[i];
                    // Create compiled delegate instead of using PropertyInfo.SetValue
                    setters[i] = CreateOptimizedSetter(prop, i);
                }

                return setters;
            });
        }

        private static Action<object, DbDataReader> CreateOptimizedSetter(PropertyInfo property, int index)
        {
            var targetParam = Expression.Parameter(typeof(object), "target");
            var readerParam = Expression.Parameter(typeof(DbDataReader), "reader");

            var isDbNull = Expression.Call(readerParam, Methods.IsDbNull, Expression.Constant(index));
            var getValue = GetOptimizedReaderCall(readerParam, property.PropertyType, index);
            var assign = Expression.Call(Expression.Convert(targetParam, property.DeclaringType!), property.GetSetMethod()!, getValue);
            var body = Expression.IfThen(Expression.Not(isDbNull), assign);

            return Expression.Lambda<Action<object, DbDataReader>>(body, targetParam, readerParam).Compile();
        }

    }
}
