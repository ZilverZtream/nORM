using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using nORM.Configuration;
using nORM.Providers;

#nullable enable

namespace nORM.Mapping
{
    public static class ColumnMappingCache
    {
        private static readonly ConcurrentDictionary<Type, CachedTypeInfo> _typeCache = new();
        private static readonly ConcurrentDictionary<(Type EntityType, Type ProviderType, IEntityTypeConfiguration? Config), Column[]> _columnCache = new();

        /// <summary>
        /// Retrieves column metadata for the specified entity type from a cache,
        /// generating the mapping if it has not been cached previously.
        /// </summary>
        /// <param name="entityType">The CLR type of the entity.</param>
        /// <param name="provider">Database provider used for escaping identifiers.</param>
        /// <param name="config">Optional entity type configuration.</param>
        /// <returns>An array of <see cref="Column"/> objects describing the entity's columns.</returns>
        public static Column[] GetCachedColumns(Type entityType, DatabaseProvider provider, IEntityTypeConfiguration? config)
        {
            var key = (entityType, provider.GetType(), config);

            return _columnCache.GetOrAdd(key, _ =>
            {
                var typeInfo = GetCachedTypeInfo(entityType);
                var columns = new List<Column>(typeInfo.MappableProperties.Count);

                foreach (var propInfo in typeInfo.MappableProperties)
                {
                    OwnedNavigation? ownedNav = null;
                    config?.OwnedNavigations.TryGetValue(propInfo.Property, out ownedNav);
                    if (ownedNav != null || propInfo.OwnedTypeInfo != null)
                    {
                        var ownedTypeInfo = ownedNav != null ? GetCachedTypeInfo(ownedNav.OwnedType) : propInfo.OwnedTypeInfo!;
                        var ownedConfig = ownedNav?.Configuration;
                        foreach (var ownedProp in ownedTypeInfo.MappableProperties)
                        {
                            var ownedColumn = CreateOwnedColumn(propInfo, ownedProp, provider, ownedConfig);
                            columns.Add(ownedColumn);
                        }
                    }
                    else
                    {
                        var column = new Column(propInfo, provider, config);
                        columns.Add(column);
                    }
                }

                return columns.ToArray();
            });
        }

        private static CachedTypeInfo GetCachedTypeInfo(Type type)
        {
            return _typeCache.GetOrAdd(type, static t =>
            {
                var properties = t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                    .Where(p => p.CanRead && p.CanWrite)
                    .ToArray();

                var mappableProperties = new List<CachedPropertyInfo>(properties.Length);

                foreach (var prop in properties)
                {
                    var attributes = prop.GetCustomAttributes().ToArray();

                    if (attributes.OfType<NotMappedAttribute>().Any())
                        continue;

                    var propType = prop.PropertyType;

                    // Skip collection navigation properties
                    if (propType != typeof(string) && propType != typeof(byte[]) && typeof(IEnumerable).IsAssignableFrom(propType))
                        continue;

                    var ownedAttribute = attributes.OfType<OwnedAttribute>().FirstOrDefault();

                    // Skip reference navigation properties (entities)
                    if (!propType.IsValueType && propType != typeof(string) && propType != typeof(byte[]) &&
                        ownedAttribute == null && propType.GetCustomAttribute<OwnedAttribute>() == null)
                    {
                        var entityProps = propType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
                        var looksLikeEntity = entityProps.Any(p => p.GetCustomAttribute<KeyAttribute>() != null ||
                            string.Equals(p.Name, "Id", StringComparison.OrdinalIgnoreCase));
                        if (looksLikeEntity)
                            continue;
                    }

                    var propertyInfo = new CachedPropertyInfo
                    {
                        Property = prop,
                        Attributes = attributes,
                        IsKey = attributes.OfType<KeyAttribute>().Any(),
                        IsTimestamp = attributes.OfType<TimestampAttribute>().Any(),
                        IsDbGenerated = attributes.OfType<DatabaseGeneratedAttribute>()
                            .Any(attr => attr.DatabaseGeneratedOption == DatabaseGeneratedOption.Identity),
                        ColumnName = attributes.OfType<ColumnAttribute>().FirstOrDefault()?.Name,
                        ForeignKeyName = attributes.OfType<ForeignKeyAttribute>().FirstOrDefault()?.Name,
                        Getter = CreateOptimizedGetter(prop),
                        Setter = CreateOptimizedSetter(prop)
                    };

                    if (ownedAttribute != null || propType.GetCustomAttribute<OwnedAttribute>() != null)
                    {
                        propertyInfo.OwnedTypeInfo = GetCachedTypeInfo(prop.PropertyType);
                    }

                    mappableProperties.Add(propertyInfo);
                }

                return new CachedTypeInfo
                {
                    Type = t,
                    MappableProperties = mappableProperties,
                    KeyProperties = mappableProperties.Where(p => p.IsKey).ToArray(),
                    TimestampProperty = mappableProperties.FirstOrDefault(p => p.IsTimestamp),
                    PropertyLookup = mappableProperties.ToDictionary(p => p.Property.Name, p => p)
                };
            });
        }

        private static Column CreateOwnedColumn(CachedPropertyInfo ownerProp, CachedPropertyInfo ownedProp, DatabaseProvider provider, IEntityTypeConfiguration? config)
        {
            var getter = CreateOwnedGetter(ownerProp.Property, ownedProp.Property);
            var setter = CreateOwnedSetter(ownerProp.Property, ownedProp.Property, out var setterMethod);
            return new Column(ownedProp, provider, config, ownerProp.Property.Name, getter, setter, setterMethod);
        }

        private static Func<object, object?> CreateOptimizedGetter(PropertyInfo property)
        {
            var instanceParam = Expression.Parameter(typeof(object), "instance");
            var castInstance = Expression.Convert(instanceParam, property.DeclaringType!);
            var getProperty = Expression.Property(castInstance, property);
            var convertResult = Expression.Convert(getProperty, typeof(object));

            return Expression.Lambda<Func<object, object?>>(convertResult, instanceParam).Compile();
        }

        private static Action<object, object?> CreateOptimizedSetter(PropertyInfo property)
        {
            var instanceParam = Expression.Parameter(typeof(object), "instance");
            var valueParam = Expression.Parameter(typeof(object), "value");

            var castInstance = Expression.Convert(instanceParam, property.DeclaringType!);
            var castValue = Expression.Convert(valueParam, property.PropertyType);
            var setProperty = Expression.Call(castInstance, property.GetSetMethod()!, castValue);

            return Expression.Lambda<Action<object, object?>>(setProperty, instanceParam, valueParam).Compile();
        }

        private static Func<object, object?> CreateOwnedGetter(PropertyInfo owner, PropertyInfo owned)
        {
            var entityParam = Expression.Parameter(typeof(object), "e");
            var castEntity = Expression.Convert(entityParam, owner.DeclaringType!);
            var ownerAccess = Expression.Property(castEntity, owner);
            var nullCheck = Expression.Equal(ownerAccess, Expression.Constant(null, owner.PropertyType));
            var ownedAccess = Expression.Property(ownerAccess, owned);
            Expression body = Expression.Condition(
                nullCheck,
                Expression.Constant(null, typeof(object)),
                Expression.Convert(ownedAccess, typeof(object)));
            return Expression.Lambda<Func<object, object?>>(body, entityParam).Compile();
        }

        private static Action<object, object?> CreateOwnedSetter(PropertyInfo owner, PropertyInfo owned, out MethodInfo methodInfo)
        {
            var dm = new DynamicMethod($"set_{owner.Name}_{owned.Name}", typeof(void), new[] { typeof(object), typeof(object) }, owner.DeclaringType!.Module, true);
            var il = dm.GetILGenerator();

            // Cast entity to owner type
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, owner.DeclaringType!);
            il.Emit(OpCodes.Dup);
            il.Emit(OpCodes.Callvirt, owner.GetGetMethod()!);
            var ownedVar = il.DeclareLocal(owner.PropertyType);
            il.Emit(OpCodes.Stloc, ownedVar);
            il.Emit(OpCodes.Pop); // remove duplicated entity

            // Initialize owned object if null
            il.Emit(OpCodes.Ldloc, ownedVar);
            var hasValue = il.DefineLabel();
            il.Emit(OpCodes.Brtrue_S, hasValue);
            il.Emit(OpCodes.Ldarg_0);
            il.Emit(OpCodes.Castclass, owner.DeclaringType!);
            il.Emit(OpCodes.Newobj, owner.PropertyType.GetConstructor(Type.EmptyTypes)!);
            il.Emit(OpCodes.Dup);
            il.Emit(OpCodes.Stloc, ownedVar);
            il.Emit(OpCodes.Callvirt, owner.GetSetMethod()!);
            il.MarkLabel(hasValue);

            // Assign value
            il.Emit(OpCodes.Ldloc, ownedVar);
            il.Emit(OpCodes.Ldarg_1);
            if (owned.PropertyType.IsValueType)
                il.Emit(OpCodes.Unbox_Any, owned.PropertyType);
            else
                il.Emit(OpCodes.Castclass, owned.PropertyType);
            il.Emit(OpCodes.Callvirt, owned.GetSetMethod()!);
            il.Emit(OpCodes.Ret);

            methodInfo = dm;
            return (Action<object, object?>)dm.CreateDelegate(typeof(Action<object, object?>));
        }

        internal class CachedTypeInfo
        {
            public Type Type { get; set; } = null!;
            public IReadOnlyList<CachedPropertyInfo> MappableProperties { get; set; } = null!;
            public CachedPropertyInfo[] KeyProperties { get; set; } = null!;
            public CachedPropertyInfo? TimestampProperty { get; set; }
            public Dictionary<string, CachedPropertyInfo> PropertyLookup { get; set; } = null!;
        }

        internal class CachedPropertyInfo
        {
            public PropertyInfo Property { get; set; } = null!;
            public Attribute[] Attributes { get; set; } = null!;
            public bool IsKey { get; set; }
            public bool IsTimestamp { get; set; }
            public bool IsDbGenerated { get; set; }
            public string? ColumnName { get; set; }
            public string? ForeignKeyName { get; set; }
            public Func<object, object?> Getter { get; set; } = null!;
            public Action<object, object?> Setter { get; set; } = null!;
            internal CachedTypeInfo? OwnedTypeInfo { get; set; }
        }
    }
}
