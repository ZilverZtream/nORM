using System;
using System.Collections.Generic;
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

            var dm = new DynamicMethod("mat_" + Guid.NewGuid().ToString("N"), typeof(object), new[] { typeof(DbDataReader) }, targetType.Module, true);
            var il = dm.GetILGenerator();

            if (targetType.IsPrimitive || targetType == typeof(decimal) || targetType == typeof(string))
            {
                var read = Methods.GetReaderMethod(targetType);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4_0);
                il.EmitCall(OpCodes.Callvirt, read, null);
                if (read.ReturnType == typeof(object))
                    il.Emit(OpCodes.Unbox_Any, targetType);
                else if (read.ReturnType != targetType)
                    il.Emit(OpCodes.Box, read.ReturnType);
                if (targetType.IsValueType)
                    il.Emit(OpCodes.Box, targetType);
                il.Emit(OpCodes.Ret);
                return (Func<DbDataReader, object>)dm.CreateDelegate(typeof(Func<DbDataReader, object>));
            }

            var ctor = targetType.GetConstructor(Type.EmptyTypes) ?? throw new InvalidOperationException($"Type {targetType} must have a parameterless constructor");
            il.Emit(OpCodes.Newobj, ctor);
            var entity = il.DeclareLocal(targetType);
            il.Emit(OpCodes.Stloc, entity);

            var columns = projection == null
                ? mapping.Columns
                : ExtractColumnsFromProjection(mapping, projection);

            for (int i = 0; i < columns.Length; i++)
            {
                var col = columns[i];
                var propType = col.Prop.PropertyType;
                var read = Methods.GetReaderMethod(propType);
                il.Emit(OpCodes.Ldloc, entity);
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, i);
                il.EmitCall(OpCodes.Callvirt, read, null);
                if (read.ReturnType == typeof(object))
                {
                    il.Emit(OpCodes.Unbox_Any, propType);
                }
                else if (read.ReturnType != propType)
                {
                    il.Emit(OpCodes.Box, read.ReturnType);
                    il.Emit(OpCodes.Unbox_Any, propType);
                }
                il.Emit(OpCodes.Callvirt, col.Prop.SetMethod!);
            }

            il.Emit(OpCodes.Ldloc, entity);
            if (targetType.IsValueType)
                il.Emit(OpCodes.Box, targetType);
            il.Emit(OpCodes.Ret);
            return (Func<DbDataReader, object>)dm.CreateDelegate(typeof(Func<DbDataReader, object>));
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

        private static bool IsScalarType(Type type) =>
            type.IsPrimitive || type == typeof(string) || type == typeof(decimal);
    }
}
