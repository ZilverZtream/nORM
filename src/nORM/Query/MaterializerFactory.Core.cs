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
using System.Globalization;
using nORM.Core;

namespace nORM.Query
{
    internal sealed partial class MaterializerFactory
    {
        /// <summary>
        /// Coerces a TPH discriminator value to the discriminator column's declared CLR
        /// type so model-side keys (e.g. boxed int from a DiscriminatorValue attribute) and
        /// provider-widened reader values (e.g. Int64 from SQLite INTEGER) compare equal.
        /// Falls back to the raw value when no safe conversion exists.
        /// </summary>
        private static object NormalizeDiscriminator(object value, Type discType)
        {
            if (value is DBNull) return value;
            var target = Nullable.GetUnderlyingType(discType) ?? discType;
            if (value.GetType() == target) return value;
            try
            {
                if (target.IsEnum) return Enum.ToObject(target, value);
                return Convert.ChangeType(value, target, System.Globalization.CultureInfo.InvariantCulture);
            }
            catch
            {
                return value;
            }
        }

        private Func<DbDataReader, object> CreateMaterializerInternal(TableMapping mapping, Type targetType, LambdaExpression? projection = null, bool ignoreTph = false, int startOffset = 0, IReadOnlyDictionary<string, nORM.Mapping.IValueConverter>? projectionSubqueryConverters = null)
        {
            if (!ignoreTph && mapping.DiscriminatorColumn != null && mapping.TphMappings.Count > 0 && projection == null)
            {
                var discIndex = startOffset + Array.IndexOf(mapping.Columns, mapping.DiscriminatorColumn);
                var baseMat = CreateMaterializerInternal(mapping, targetType, null, true, startOffset);
                // Normalize discriminator keys and the runtime value to the discriminator
                // column's declared CLR type. Providers widen storage types (SQLite returns
                // Int64 for INTEGER), so a boxed (long)1 from the reader would never match a
                // key boxed as (int)1 from the model — every derived row then silently
                // materialized as the base type. Coercing both sides to one type fixes it.
                var discType = Nullable.GetUnderlyingType(mapping.DiscriminatorColumn.Prop.PropertyType)
                    ?? mapping.DiscriminatorColumn.Prop.PropertyType;
                var derivedMats = mapping.TphMappings.ToDictionary(
                    kvp => NormalizeDiscriminator(kvp.Key, discType),
                    kvp =>
                    {
                        var dmap = kvp.Value;
                        var indices = dmap.Columns.Select(c =>
                        {
                            if (mapping.ColumnsByName.TryGetValue(c.PropName, out var baseCol))
                                return startOffset + Array.IndexOf(mapping.Columns, baseCol);
                            return UnmappedOrdinal; // column not found in base mapping
                        }).ToArray();
                        var getters = dmap.Columns.Select((c, i) => CreateReaderGetter(c.Prop.PropertyType, indices[i], 0)).ToArray();
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
                    // reader.GetValue returns DBNull.Value (not null) for SQL NULL columns
                    if (disc is not null and not DBNull
                        && derivedMats.TryGetValue(NormalizeDiscriminator(disc, discType), out var mat))
                        return mat(reader);
                    return baseMat(reader);
                };
            }

            // Handle simple scalar types directly
            if (IsSimpleType(targetType))
            {
                var scalarConverter = projection != null
                    ? TryResolveProjectedColumnConverter(mapping, projection.Body)
                    : null;
                var getter = CreateReaderGetter(targetType, 0, startOffset, scalarConverter);
                // Only build a default factory for value types (used when DB returns NULL for non-nullable value type)
                // Reference types (e.g., string) use null! directly.
                Func<object>? defaultFactory = null;
                if (targetType.IsValueType)
                {
                    defaultFactory = _parameterlessCtorDelegates.GetOrAdd(targetType, t =>
                    {
                        var body = Expression.Convert(Expression.New(t), typeof(object));
                        return Expression.Lambda<Func<object>>(body).Compile();
                    });
                }

                return reader =>
                {
                    if (reader.IsDBNull(startOffset))
                        return targetType.IsValueType ? defaultFactory!() : null!;
                    return getter(reader)!;
                };
            }

            // Bare-scalar projection of a non-simple value type:
            // `Select(p => p.A - p.B)` returning TimeSpan, or any computed
            // single value (BinaryExpression / member on a subtraction / etc.).
            // ExtractColumnsFromProjection only knows how to expand NewExpression
            // and MemberInit; everything else falls back to mapping.Columns
            // which materializes the entity rather than the scalar. Route
            // through ConvertDbValue (which already handles TimeSpan from REAL
            // seconds, DateTime from string, etc.) so the single SELECT
            // expression round-trips correctly.
            if (projection != null
                && (projection.Body is BinaryExpression
                    || projection.Body is ConditionalExpression
                    || (projection.Body is MemberExpression mb
                        && mb.Expression is BinaryExpression mbBin
                        && mbBin.NodeType == ExpressionType.Subtract))
                && targetType != mapping.Type)
            {
                var convertTarget = targetType;
                return reader =>
                {
                    if (reader.IsDBNull(startOffset))
                    {
                        if (Nullable.GetUnderlyingType(convertTarget) != null) return null!;
                        if (convertTarget.IsValueType)
                            return Activator.CreateInstance(convertTarget)!;
                        return null!;
                    }
                    var raw = reader.GetValue(startOffset);
                    return ConvertDbValue(raw, convertTarget)!;
                };
            }

            // Composite GROUP BY key projected as a NESTED anonymous type:
            //   `.Select(g => new { Key = g.Key, Count = g.Count() })`
            // where g.Key is itself an anonymous type built from the composite key
            // members. BuildGroupBySelectClause emits one flat column per nested key
            // member; here we reconstruct the nested anonymous type from those flat
            // columns before invoking the outer constructor.
            if (projection?.Body is NewExpression projectionNewNested
                && projectionNewNested.Type == targetType
                && projectionNewNested.Constructor is { } projectionCtorNested
                && HasNestedAnonymousProjectionArg(projectionNewNested))
            {
                return CreateNestedAnonymousProjectionMaterializer(projectionNewNested, projectionCtorNested, startOffset);
            }

            var columns = projection == null
                ? mapping.Columns
                : ExtractColumnsFromProjection(mapping, projection, projectionSubqueryConverters);

            if (projection?.Body is NewExpression projectionNew
                && projectionNew.Type == targetType
                && projectionNew.Constructor is { } projectionCtor
                && projectionCtor.GetParameters().Length == projectionNew.Arguments.Count)
            {
                // Shaped/bare navigation-collection args are populated by the split-query pipeline and carry no
                // projection column (ExtractColumnsFromProjection excludes them); the materializer injects an
                // empty mutable list for each. Every OTHER arg maps 1:1 to a column, so the column count must
                // equal the non-collection arg count. A projection with no collection member counts every arg,
                // so this stays byte-identical (Arguments.Count == columns.Length) for the common case.
                var nonCollectionArgCount = projectionNew.Arguments.Count(a => !IsShapedOrBareNavigationCollection(a, mapping));
                if (columns.Length == nonCollectionArgCount)
                {
                    var projectionCtorParams = projectionCtor.GetParameters();
                    return CreateProjectionConstructorMaterializer(projectionCtor, projectionCtorParams, projectionNew.Arguments, columns, startOffset, mapping);
                }
            }

            var parameterlessCtor = _parameterlessCtorCache.GetOrAdd(targetType, t => t.GetConstructor(Type.EmptyTypes));
            bool hasConverters = columns.Any(c => c.Converter != null);

            if (parameterlessCtor != null && !hasConverters && columns.All(c => c.Prop.DeclaringType == targetType && c.Prop.GetSetMethod() != null))
            {
                return CreateOptimizedMaterializer(columns, targetType, startOffset);
            }

            if (parameterlessCtor != null)
            {
                var parameterlessCtorDelegate = _parameterlessCtorDelegates.GetOrAdd(targetType, t =>
                {
                    var newExpr = Expression.New(t);
                    var body = Expression.Convert(newExpr, typeof(object));
                    return Expression.Lambda<Func<object>>(body).Compile();
                });

                if (hasConverters)
                {
                    // Converter-aware path: use ConvertDbValue + apply converter per column
                    var colsSnapshot = columns;
                    return reader =>
                    {
                        var entity = parameterlessCtorDelegate();
                        for (int i = 0; i < colsSnapshot.Length; i++)
                        {
                            var col = colsSnapshot[i];
                            int ordinal;
                            try { ordinal = reader.GetOrdinal(col.Name); }
                            catch (IndexOutOfRangeException) { continue; } // column not found (ADO.NET standard)
                            catch (ArgumentOutOfRangeException) { continue; } // column not found (Microsoft.Data.Sqlite variant)
                            if (reader.IsDBNull(ordinal)) continue;
                            var rawValue = reader.GetValue(ordinal);
                            try
                            {
                                object? value;
                                if (col.Converter != null)
                                    value = col.Converter.ConvertFromProvider(rawValue);
                                else
                                    value = ConvertDbValue(rawValue, col.Prop.PropertyType);
                                col.Setter(entity, value);
                            }
                            catch (InvalidCastException)
                            {
                                // skip columns that cannot be converted (e.g., no converter configured
                                // but DB type doesn't match property type)
                            }
                            catch (FormatException)
                            {
                                // skip columns with format mismatches during conversion
                            }
                        }
                        return entity!;
                    };
                }

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
                    var setters = GetOptimizedSetters(targetType, startOffset);
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

                var fallbackCols = columns;
                var getters = fallbackCols.Select((c, i) => CreateReaderGetter(c.Prop.PropertyType, i, startOffset, c.Converter)).ToArray();
                return reader =>
                {
                    var entity = parameterlessCtorDelegate();
                    for (int i = 0; i < fallbackCols.Length; i++)
                    {
                        var col = fallbackCols[i];
                        if (col.IsShadow)
                        {
                            // Shadow columns use name-based lookup to handle the case where the
                            // column doesn't exist in the result set (e.g., SELECT * without the column).
                            int ord;
                            try { ord = reader.GetOrdinal(col.Name); }
                            catch (IndexOutOfRangeException) { continue; } // column not found (ADO.NET standard)
                            catch (ArgumentOutOfRangeException) { continue; } // column not found (Microsoft.Data.Sqlite variant)
                            if (reader.IsDBNull(ord)) continue;
                            var rawVal = reader.GetValue(ord);
                            try { col.Setter(entity, ConvertDbValue(rawVal, col.Prop.PropertyType)); }
                            catch (InvalidCastException) { /* shadow column type mismatch - skip silently */ }
                            catch (FormatException) { /* shadow column format mismatch - skip silently */ }
                        }
                        else
                        {
                            if (reader.IsDBNull(i + startOffset)) continue;
                            var value = getters[i](reader);
                            col.Setter(entity, value);
                        }
                    }
                    return entity!;
                };
            }

            // Constructor with parameters (record types, anonymous types, etc.)
            var ctor = GetCachedConstructor(targetType, columns);
            var ctorParams = ctor.GetParameters();
            var ctorDelegate = _constructorDelegates.GetOrAdd(targetType, _ => CreateConstructorDelegate(ctor));
            var paramGetters = ctorParams.Select((p, i) =>
                CreateReaderGetter(p.ParameterType, i, startOffset, i < columns.Length ? columns[i].Converter : null)).ToArray();

            return reader =>
            {
                var args = new object?[ctorParams.Length];
                for (int i = 0; i < ctorParams.Length; i++)
                {
                    if (reader.IsDBNull(i + startOffset))
                    {
                        var paramType = ctorParams[i].ParameterType;
                        var underlyingNullable = Nullable.GetUnderlyingType(paramType);
                        if (paramType.IsValueType && underlyingNullable == null)
                        {
                            // MAP-4: Fail fast for non-nullable value type constructor parameters
                            throw new InvalidOperationException(
                                $"DB column returned NULL for non-nullable constructor parameter '{ctorParams[i].Name}' " +
                                $"of type '{paramType.Name}'. Mark the parameter type as nullable or fix the data source.");
                        }
                        try
                        {
                            args[i] = paramType.IsValueType ? Activator.CreateInstance(paramType) : default;
                        }
                        catch (MissingMethodException)
                        {
                            throw new InvalidOperationException(
                                $"Cannot create default value for constructor parameter '{ctorParams[i].Name}' " +
                                $"of type '{paramType.Name}': type has no parameterless constructor. " +
                                $"Mark the parameter as nullable or provide a default value.");
                        }
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
                var ctors = t.GetConstructors();

                // Anonymous types (compiler-generated) use positional matching by parameter count
                // since their constructor parameter names (camelCase) don't match column prop names
                if (t.Namespace == null && t.Name.Contains("AnonymousType", StringComparison.Ordinal))
                {
                    return ctors.FirstOrDefault(c => c.GetParameters().Length == columns.Length)
                        ?? throw new InvalidOperationException($"No suitable constructor for {t}");
                }

                return ctors
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
                    }) ?? throw new InvalidOperationException(
                        $"No suitable constructor for {t.FullName ?? t.Name}. " +
                        $"Expected constructor signature: ({string.Join(", ", columns.Select(c => $"{c.Prop.PropertyType.Name} {c.Prop.Name}"))}).");
            });
        }
    }
}
