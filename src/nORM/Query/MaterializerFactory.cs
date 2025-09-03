using System;
using System.Collections.Generic;
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
        private static readonly ConcurrentLruCache<(Type MappingType, Type TargetType, string? ProjectionKey), Func<DbDataReader, CancellationToken, Task<object>>> _cache = new(maxSize: 1000);

        public Func<DbDataReader, CancellationToken, Task<object>> CreateMaterializer(TableMapping mapping, Type targetType, LambdaExpression? projection = null)
        {
            var projectionKey = projection?.ToString();
            var cacheKey = (mapping.Type, targetType, projectionKey);

            if (projection == null && CompiledMaterializerStore.TryGet(targetType, out var precompiled))
            {
                _cache.GetOrAdd(cacheKey, _ => precompiled);
                return precompiled;
            }

            return _cache.GetOrAdd(cacheKey, _ =>
            {
                var sync = CreateMaterializerInternal(mapping, targetType, projection);
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
            var ctor = targetType.GetConstructors()
                .OrderByDescending(c => c.GetParameters().Length)
                .FirstOrDefault(c =>
                {
                    var ps = c.GetParameters();
                    if (ps.Length != columns.Length) return false;
                    for (int i = 0; i < ps.Length; i++)
                    {
                        if (!string.Equals(ps[i].Name, columns[i].Prop.Name, StringComparison.OrdinalIgnoreCase))
                            return false;
                    }
                    return true;
                }) ?? throw new InvalidOperationException($"Type {targetType} has no suitable constructor");

            return reader =>
            {
                var args = new object?[columns.Length];
                var parameters = ctor.GetParameters();
                for (int i = 0; i < columns.Length; i++)
                {
                    if (reader.IsDBNull(i))
                    {
                        args[i] = parameters[i].ParameterType.IsValueType ? Activator.CreateInstance(parameters[i].ParameterType) : null;
                        continue;
                    }
                    var paramType = parameters[i].ParameterType;
                    var readerMethod = Methods.GetReaderMethod(paramType);
                    var value = readerMethod.Invoke(reader, new object[] { i });
                    if (readerMethod == Methods.GetValue)
                        value = Convert.ChangeType(value!, paramType);
                    args[i] = value;
                }
                return ctor.Invoke(args)!;
            };
        }

        private static Column[] ExtractColumnsFromProjection(TableMapping mapping, LambdaExpression projection)
        {
            if (projection.Body is NewExpression newExpr)
            {
                return newExpr.Arguments
                    .OfType<MemberExpression>()
                    .Select(m => mapping.Columns.First(c => c.Prop.Name == m.Member.Name))
                    .ToArray();
            }
            return mapping.Columns;
        }

    }
}
