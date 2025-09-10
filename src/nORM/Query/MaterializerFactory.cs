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
    /// Optimized for JOIN scenarios with robust column mapping.
    /// </summary>
    internal sealed class MaterializerFactory
    {
        private const int DefaultCacheSize = 2000; // Increased for JOIN scenarios
        private static readonly ConcurrentLruCache<MaterializerCacheKey, Func<DbDataReader, CancellationToken, Task<object>>> _cache
            = new(maxSize: DefaultCacheSize, timeToLive: TimeSpan.FromMinutes(15));

        // Separate cache for schema-specific mappings to avoid conflicts
        private static readonly ConcurrentLruCache<SchemaCacheKey, OrdinalMapping> _schemaCache
            = new(maxSize: DefaultCacheSize, timeToLive: TimeSpan.FromMinutes(30));

        // Cache constructor info and delegates to avoid repeated reflection in hot paths
        private static readonly ConcurrentDictionary<Type, ConstructorInfo> _constructorCache = new();
        private static readonly ConcurrentDictionary<Type, Func<object?[], object>> _constructorDelegates = new();
        private static readonly ConcurrentDictionary<Type, ConstructorInfo?> _parameterlessCtorCache = new();
        private static readonly ConcurrentDictionary<Type, Func<object>> _parameterlessCtorDelegates = new();
        private static readonly ConcurrentDictionary<Type, bool> _simpleTypeCache = new();
        private static readonly ConcurrentDictionary<Type, Func<DbDataReader, object>> _fastMaterializers = new();
        private static readonly ConcurrentDictionary<Type, Action<object, DbDataReader>[]> _setterCache = new();
        private static readonly ConcurrentDictionary<Type, PropertyInfo[]> _propertiesCache = new();

        // Pre-computed type conversion delegates for better performance
        private static readonly ConcurrentDictionary<(Type From, Type To), Func<object, object>> _conversionCache = new();

        private static bool IsSimpleType(Type type)
            => _simpleTypeCache.GetOrAdd(type, static t => t.IsPrimitive || t == typeof(decimal) || t == typeof(string));

        internal static (long Hits, long Misses, double HitRate) CacheStats
            => (_cache.Hits, _cache.Misses, _cache.HitRate);

        internal static (long SchemaHits, long SchemaMisses, double SchemaHitRate) SchemaCacheStats
            => (_schemaCache.Hits, _schemaCache.Misses, _schemaCache.HitRate);

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
                return targetType.IsValueType ? Activator.CreateInstance(targetType) : null;
            }

            var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;
            if (dbValue.GetType() == underlyingType)
            {
                return dbValue;
            }

            // Use cached conversion delegate for better performance
            var conversionKey = (dbValue.GetType(), underlyingType);
            var converter = _conversionCache.GetOrAdd(conversionKey, key =>
            {
                var (from, to) = key;
                try
                {
                    var param = Expression.Parameter(typeof(object), "value");
                    var convert = Expression.Convert(
                        Expression.Call(typeof(Convert), nameof(Convert.ChangeType),
                            null,
                            Expression.Convert(param, from),
                            Expression.Constant(to)),
                        typeof(object));
                    return Expression.Lambda<Func<object, object>>(convert, param).Compile();
                }
                catch
                {
                    // Fallback to runtime conversion
                    return value => Convert.ChangeType(value, to);
                }
            });

            try
            {
                return converter(dbValue);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException(
                    $"Failed to convert {dbValue.GetType().Name} value '{dbValue}' to {underlyingType.Name}: {ex.Message}", ex);
            }
        }

        private static Func<DbDataReader, object> CreateILMaterializer<T>() where T : class
        {
            var type = typeof(T);
            var method = new DynamicMethod("Materialize", typeof(object), new[] { typeof(DbDataReader) }, typeof(MaterializerFactory), true);
            var il = method.GetILGenerator();
            var parameterlessCtor = type.GetConstructor(Type.EmptyTypes);

            if (parameterlessCtor != null)
            {
                var props = _propertiesCache.GetOrAdd(type, t => t.GetProperties(BindingFlags.Instance | BindingFlags.Public));
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
            var ctors = type.GetConstructors();
            if (ctors.Length == 0)
                throw new InvalidOperationException($"No constructors found for type {type.FullName}");

            var ctor = ctors.OrderByDescending(c => c.GetParameters().Length).First();
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

        /// <summary>
        /// Creates a materializer delegate that converts a <see cref="DbDataReader"/> row into the target type.
        /// </summary>
        /// <param name="mapping">Mapping describing the source table schema.</param>
        /// <param name="targetType">CLR type to materialize.</param>
        /// <param name="projection">Optional projection expression selecting specific members.</param>
        /// <returns>A delegate that asynchronously materializes objects from a data reader.</returns>
        public Func<DbDataReader, CancellationToken, Task<object>> CreateMaterializer(
            TableMapping mapping,
            Type targetType,
            LambdaExpression? projection = null)
        {
            ArgumentNullException.ThrowIfNull(mapping);
            ArgumentNullException.ThrowIfNull(targetType);

            // CHECK FOR COMPILED MATERIALIZER FIRST
            if (projection == null && CompiledMaterializerStore.TryGet(targetType, out var compiled))
            {
                return compiled;
            }

            var cacheKey = new MaterializerCacheKey(
                mapping.Type.GetHashCode(),
                targetType.GetHashCode(),
                projection != null ? ExpressionFingerprint.Compute(projection) : 0,
                mapping.TableName);

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

        // Optimized schema-aware materializer for JOIN scenarios
        /// <summary>
        /// Creates a materializer that is aware of the actual reader schema, enabling efficient JOIN projections.
        /// </summary>
        /// <param name="mapping">Mapping describing the table layout.</param>
        /// <param name="targetType">Type of object to materialize.</param>
        /// <param name="projection">Optional projection expression.</param>
        /// <returns>A delegate that materializes objects taking the reader schema into account.</returns>
        public Func<DbDataReader, CancellationToken, Task<object>> CreateSchemaAwareMaterializer(
            TableMapping mapping,
            Type targetType,
            LambdaExpression? projection = null)
        {
            ArgumentNullException.ThrowIfNull(mapping);
            ArgumentNullException.ThrowIfNull(targetType);

            // For simple cases without JOINs, use regular materializer
            if (projection == null && CompiledMaterializerStore.TryGet(targetType, out var compiled))
            {
                return compiled;
            }

            var cacheKey = new MaterializerCacheKey(
                mapping.Type.GetHashCode(),
                targetType.GetHashCode(),
                projection != null ? ExpressionFingerprint.Compute(projection) : 0,
                mapping.TableName);

            return _cache.GetOrAdd(cacheKey, _ =>
            {
                var baseMaterializer = CreateMaterializerInternal(mapping, targetType, projection);

                // For most cases, use base materializer with minimal overhead
                if (projection == null)
                {
                    return (reader, ct) =>
                    {
                        ct.ThrowIfCancellationRequested();
                        return Task.FromResult(baseMaterializer(reader));
                    };
                }

                // Only use schema-aware logic for complex projections
                OrdinalMapping? cachedMapping = null;
                var mappingComputed = false;

                return (DbDataReader reader, CancellationToken ct) =>
                {
                    ct.ThrowIfCancellationRequested();

                    // Always try base materializer first - it's fastest
                    try
                    {
                        return Task.FromResult(baseMaterializer(reader));
                    }
                    catch (FormatException)
                    {
                        // Only on format exception, compute schema mapping once
                        if (!mappingComputed)
                        {
                            cachedMapping = CreateOrdinalMapping(reader, mapping);
                            mappingComputed = true;
                        }

                        if (cachedMapping?.IsValid == true)
                        {
                            using var shimReader = new OptimizedOrdinalShimReader(reader, cachedMapping.Value);
                            return Task.FromResult(baseMaterializer(shimReader));
                        }

                        // If mapping failed, rethrow original exception
                        throw;
                    }
                };
            });
        }

        private static OrdinalMapping CreateOrdinalMapping(DbDataReader reader, TableMapping mapping)
        {
            var fieldCount = reader.FieldCount;
            var ordinals = new int[mapping.Columns.Length];
            var isValid = true;

            // Build quick lookup for field names (case-insensitive)
            var nameToOrdinal = new Dictionary<string, int>(fieldCount, StringComparer.OrdinalIgnoreCase);
            var fieldTypes = new Type[fieldCount];

            for (int i = 0; i < fieldCount; i++)
            {
                var name = reader.GetName(i);
                nameToOrdinal.TryAdd(name, i); // First occurrence wins
                fieldTypes[i] = reader.GetFieldType(i);
            }

            // Map each column to its ordinal
            for (int i = 0; i < mapping.Columns.Length; i++)
            {
                var column = mapping.Columns[i];
                var propName = column.PropName;
                var expectedType = column.Prop.PropertyType;
                var foundOrdinal = -1;

                // Try direct property name match first
                if (nameToOrdinal.TryGetValue(propName, out foundOrdinal))
                {
                    if (IsTypeCompatible(fieldTypes[foundOrdinal], expectedType))
                    {
                        ordinals[i] = foundOrdinal;
                        continue;
                    }
                    // Type mismatch, continue searching
                    foundOrdinal = -1;
                }

                // Try table-qualified names for JOIN scenarios
                var tableName = mapping.TableName;
                if (!string.IsNullOrEmpty(tableName))
                {
                    var qualifiedNames = new[]
                    {
                        $"{tableName}.{propName}",
                        $"{tableName}_{propName}",
                        $"[{tableName}].[{propName}]",
                        $"`{tableName}`.`{propName}`"
                    };

                    foreach (var qualifiedName in qualifiedNames)
                    {
                        if (nameToOrdinal.TryGetValue(qualifiedName, out foundOrdinal))
                        {
                            if (IsTypeCompatible(fieldTypes[foundOrdinal], expectedType))
                            {
                                ordinals[i] = foundOrdinal;
                                break;
                            }
                            foundOrdinal = -1;
                        }
                    }
                }

                // Try escaped column name
                if (foundOrdinal == -1 && !string.IsNullOrEmpty(column.EscCol))
                {
                    var normalizedEscCol = column.EscCol.Trim('[', ']', '`', '"', '\'');
                    if (nameToOrdinal.TryGetValue(normalizedEscCol, out foundOrdinal))
                    {
                        if (!IsTypeCompatible(fieldTypes[foundOrdinal], expectedType))
                        {
                            foundOrdinal = -1;
                        }
                    }
                }

                if (foundOrdinal == -1)
                {
                    // Column not found or type incompatible - this mapping may not be suitable for this schema
                    ordinals[i] = -1;
                    isValid = false;
                }
                else
                {
                    ordinals[i] = foundOrdinal;
                }
            }

            return new OrdinalMapping(ordinals, isValid);
        }

        private static bool IsTypeCompatible(Type fieldType, Type expectedType)
        {
            if (fieldType == expectedType)
                return true;

            var underlyingExpected = Nullable.GetUnderlyingType(expectedType) ?? expectedType;
            var underlyingField = Nullable.GetUnderlyingType(fieldType) ?? fieldType;

            if (underlyingField == underlyingExpected)
                return true;

            // Allow numeric conversions
            if (IsNumericType(underlyingField) && IsNumericType(underlyingExpected))
                return true;

            // Allow string to string only (don't allow string to numeric - causes format errors)
            if (underlyingField == typeof(string) && underlyingExpected == typeof(string))
                return true;

            // Allow object type (can be converted)
            if (underlyingField == typeof(object))
                return true;

            // Allow specific safe conversions from string to non-string types
            if (underlyingField == typeof(string))
            {
                // Only allow string to these specific types that can handle string conversion safely
                return underlyingExpected == typeof(Guid) ||
                       underlyingExpected == typeof(DateTime) ||
                       underlyingExpected == typeof(DateTimeOffset) ||
                       underlyingExpected == typeof(TimeSpan) ||
                       underlyingExpected == typeof(bool); // SQLite stores bools as strings sometimes
            }

            return false;
        }

        private static bool IsNumericType(Type type)
        {
            return type == typeof(byte) || type == typeof(sbyte) ||
                   type == typeof(short) || type == typeof(ushort) ||
                   type == typeof(int) || type == typeof(uint) ||
                   type == typeof(long) || type == typeof(ulong) ||
                   type == typeof(float) || type == typeof(double) ||
                   type == typeof(decimal);
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
                        var ctor = _parameterlessCtorDelegates.GetOrAdd(dmap.Type, t =>
                        {
                            var newExpr = Expression.New(t);
                            var body = Expression.Convert(newExpr, typeof(object));
                            return Expression.Lambda<Func<object>>(body).Compile();
                        });
                        return (Func<DbDataReader, object>)(reader =>
                        {
                            var entity = ctor();
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
                var defaultFactory = _parameterlessCtorDelegates.GetOrAdd(targetType, t =>
                {
                    var body = Expression.Convert(Expression.New(t), typeof(object));
                    return Expression.Lambda<Func<object>>(body).Compile();
                });

                return reader =>
                {
                    if (reader.IsDBNull(0))
                        return targetType.IsValueType ? defaultFactory() : null!;
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

                var properties = _propertiesCache.GetOrAdd(targetType, t => t.GetProperties(BindingFlags.Instance | BindingFlags.Public));
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
                var args = new object?[ctorParams.Length];
                for (int i = 0; i < ctorParams.Length; i++)
                {
                    if (reader.IsDBNull(i))
                    {
                        var paramType = ctorParams[i].ParameterType;
                        args[i] = paramType.IsValueType ? Activator.CreateInstance(paramType) : default;
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
            try
            {
                materializer(reader);
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Materializer validation failed for type {targetType.Name}: {ex.Message}", ex);
            }
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
            /// <summary>
            /// Always reports the field as <c>DBNull</c> for validation purposes.
            /// </summary>
            /// <param name="ordinal">The zero-based column ordinal.</param>
            /// <param name="cancellationToken">Token used to cancel the operation.</param>
            /// <returns>A completed task returning <c>true</c>.</returns>
            public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => Task.FromResult(true);
            public override object GetValue(int ordinal) => DBNull.Value;

            public override int GetValues(object[] values)
            {
                Array.Fill(values, DBNull.Value);
                return Math.Min(values.Length, _fieldCount);
            }

            public override string GetName(int ordinal) => $"Field_{ordinal}";
            public override int GetOrdinal(string name) => int.Parse(name.Replace("Field_", ""));
            public override string GetDataTypeName(int ordinal) => nameof(Object);
            public override Type GetFieldType(int ordinal) => typeof(object);
            public override bool HasRows => false;
            public override bool IsClosed => false;
            public override int RecordsAffected => 0;
            public override object this[int ordinal] => DBNull.Value;
            public override object this[string name] => DBNull.Value;
            public override IEnumerator GetEnumerator() => Array.Empty<object>().GetEnumerator();
            public override bool Read() => false;
            /// <summary>
            /// Always returns <c>false</c> because this reader has no rows.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the read operation.</param>
            /// <returns>A completed task returning <c>false</c>.</returns>
            public override Task<bool> ReadAsync(CancellationToken cancellationToken) => Task.FromResult(false);
            public override bool NextResult() => false;
            /// <summary>
            /// Always returns <c>false</c> because there are no additional result sets.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the operation.</param>
            /// <returns>A completed task returning <c>false</c>.</returns>
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
                nameof(Decimal) => Expression.Call(reader, Methods.GetDecimal, Expression.Constant(index)),
                nameof(Int64) => Expression.Call(reader, Methods.GetInt64, Expression.Constant(index)),
                nameof(Double) => Expression.Call(reader, Methods.GetDouble, Expression.Constant(index)),
                nameof(Single) => Expression.Call(reader, Methods.GetFloat, Expression.Constant(index)),
                nameof(Guid) => Expression.Call(reader, Methods.GetGuid, Expression.Constant(index)),
                _ => Expression.Convert(
                        Expression.Call(reader, Methods.GetValue, Expression.Constant(index)),
                        underlyingType)
            };

            if (call.Type != propertyType)
                call = Expression.Convert(call, propertyType);

            return call;
        }

        private static Action<object, DbDataReader>[] GetOptimizedSetters(Type type)
        {
            return _setterCache.GetOrAdd(type, t =>
            {
                var properties = _propertiesCache.GetOrAdd(t, tt => tt.GetProperties(BindingFlags.Instance | BindingFlags.Public));
                var setters = new Action<object, DbDataReader>[properties.Length];

                for (int i = 0; i < properties.Length; i++)
                {
                    var prop = properties[i];
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

        // Optimized ordinal shim reader with minimal overhead
        private sealed class OptimizedOrdinalShimReader : DbDataReader
        {
            private readonly DbDataReader _inner;
            private readonly OrdinalMapping _mapping;

            public OptimizedOrdinalShimReader(DbDataReader inner, OrdinalMapping mapping)
            {
                _inner = inner ?? throw new ArgumentNullException(nameof(inner));
                _mapping = mapping;
            }

            private int MapOrdinal(int ordinal)
            {
                if ((uint)ordinal >= (uint)_mapping.Ordinals.Length) return -1;
                return _mapping.Ordinals[ordinal];
            }

            public override object GetValue(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetValue(mapped) : DBNull.Value;
            }

            public override bool IsDBNull(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.IsDBNull(mapped) : true;
            }

            public override string GetName(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetName(mapped) : $"Unmapped_{ordinal}";
            }

            public override int FieldCount => Math.Max(_inner.FieldCount, _mapping.Ordinals.Length);
            public override int GetOrdinal(string name) => _inner.GetOrdinal(name);
            public override Type GetFieldType(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetFieldType(mapped) : typeof(object);
            }

            // Delegate most properties and methods to inner reader
            public override int Depth => _inner.Depth;
            public override bool HasRows => _inner.HasRows;
            public override bool IsClosed => _inner.IsClosed;
            public override int RecordsAffected => _inner.RecordsAffected;
            public override bool Read() => _inner.Read();
            /// <summary>
            /// Asynchronously reads the next row, delegating to the inner reader while applying ordinal mapping.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the read operation.</param>
            /// <returns>A task that resolves to <c>true</c> if a row was read.</returns>
            public override Task<bool> ReadAsync(CancellationToken cancellationToken) => _inner.ReadAsync(cancellationToken);
            public override bool NextResult() => _inner.NextResult();
            /// <summary>
            /// Advances to the next result set asynchronously.
            /// </summary>
            /// <param name="cancellationToken">Token used to cancel the operation.</param>
            /// <returns>A task that resolves to <c>true</c> if another result set is available.</returns>
            public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => _inner.NextResultAsync(cancellationToken);
            public override int GetValues(object[] values) => _inner.GetValues(values);
            public override object this[int ordinal] => GetValue(ordinal);
            public override object this[string name] => _inner[name];
            public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(MapOrdinal(ordinal));
            public override IEnumerator GetEnumerator() => ((IEnumerable)_inner).GetEnumerator();

            // Typed getters with ordinal mapping
            public override bool GetBoolean(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetBoolean(mapped) : default;
            }

            public override byte GetByte(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetByte(mapped) : default;
            }

            public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
                => _inner.GetBytes(MapOrdinal(ordinal), dataOffset, buffer, bufferOffset, length);

            public override char GetChar(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetChar(mapped) : default;
            }

            public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
                => _inner.GetChars(MapOrdinal(ordinal), dataOffset, buffer, bufferOffset, length);

            public override Guid GetGuid(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetGuid(mapped) : default;
            }

            public override short GetInt16(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetInt16(mapped) : default;
            }

            public override int GetInt32(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetInt32(mapped) : default;
            }

            public override long GetInt64(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetInt64(mapped) : default;
            }

            public override DateTime GetDateTime(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetDateTime(mapped) : default;
            }

            public override string GetString(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetString(mapped) : string.Empty;
            }

            public override decimal GetDecimal(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                if (mapped < 0) return default;

                // Additional safety check - if the field is a string, try to parse it safely
                try
                {
                    if (_inner.GetFieldType(mapped) == typeof(string) && !_inner.IsDBNull(mapped))
                    {
                        var stringValue = _inner.GetString(mapped);
                        if (decimal.TryParse(stringValue, out var result))
                            return result;
                        return default; // Return default instead of throwing
                    }
                    return _inner.GetDecimal(mapped);
                }
                catch (FormatException)
                {
                    // If conversion fails, return default value instead of throwing
                    return default;
                }
            }

            public override double GetDouble(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetDouble(mapped) : default;
            }

            public override float GetFloat(int ordinal)
            {
                var mapped = MapOrdinal(ordinal);
                return mapped >= 0 ? _inner.GetFloat(mapped) : default;
            }

            public override int VisibleFieldCount => Math.Max(_inner.VisibleFieldCount, _mapping.Ordinals.Length);
        }

        // Value types for better cache performance
        private readonly struct MaterializerCacheKey : IEquatable<MaterializerCacheKey>
        {
            public readonly int MappingTypeHash;
            public readonly int TargetTypeHash;
            public readonly int ProjectionHash;
            public readonly string TableName;

            public MaterializerCacheKey(int mappingTypeHash, int targetTypeHash, int projectionHash, string tableName)
            {
                MappingTypeHash = mappingTypeHash;
                TargetTypeHash = targetTypeHash;
                ProjectionHash = projectionHash;
                TableName = tableName ?? string.Empty;
            }

            public bool Equals(MaterializerCacheKey other) =>
                MappingTypeHash == other.MappingTypeHash &&
                TargetTypeHash == other.TargetTypeHash &&
                ProjectionHash == other.ProjectionHash &&
                string.Equals(TableName, other.TableName, StringComparison.Ordinal);

            public override bool Equals(object? obj) => obj is MaterializerCacheKey other && Equals(other);

            public override int GetHashCode() => HashCode.Combine(MappingTypeHash, TargetTypeHash, ProjectionHash, TableName);
        }

        private readonly struct SchemaCacheKey : IEquatable<SchemaCacheKey>
        {
            public readonly string[] FieldNames;
            public readonly Type[] FieldTypes;
            public readonly string TableName;
            private readonly int _hashCode;

            public SchemaCacheKey(string[] fieldNames, Type[] fieldTypes, string tableName)
            {
                FieldNames = fieldNames;
                FieldTypes = fieldTypes;
                TableName = tableName;

                // Pre-compute hash code
                var hash = new HashCode();
                hash.Add(TableName);
                foreach (var name in fieldNames)
                    hash.Add(name);
                foreach (var type in fieldTypes)
                    hash.Add(type);
                _hashCode = hash.ToHashCode();
            }

            public bool Equals(SchemaCacheKey other)
            {
                if (!string.Equals(TableName, other.TableName, StringComparison.Ordinal))
                    return false;

                if (FieldNames.Length != other.FieldNames.Length || FieldTypes.Length != other.FieldTypes.Length)
                    return false;

                for (int i = 0; i < FieldNames.Length; i++)
                {
                    if (!string.Equals(FieldNames[i], other.FieldNames[i], StringComparison.Ordinal))
                        return false;
                }

                for (int i = 0; i < FieldTypes.Length; i++)
                {
                    if (FieldTypes[i] != other.FieldTypes[i])
                        return false;
                }

                return true;
            }

            public override bool Equals(object? obj) => obj is SchemaCacheKey other && Equals(other);
            public override int GetHashCode() => _hashCode;
        }

        private readonly struct OrdinalMapping
        {
            public readonly int[] Ordinals;
            public readonly bool IsValid;

            public OrdinalMapping(int[] ordinals, bool isValid)
            {
                Ordinals = ordinals;
                IsValid = isValid;
            }
        }
    }
}