using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using System.Data;
using nORM.Core;
using nORM.Mapping;
using nORM.Internal;

#nullable enable

namespace nORM.Navigation
{
    /// <summary>
    /// Advanced navigation property system with lazy loading, change tracking, and performance optimization
    /// Provides EF Core-like experience without sacrificing nORM's performance characteristics
    /// </summary>
    public static class NavigationPropertyExtensions
    {
        internal static readonly ConditionalWeakTable<object, NavigationContext> _navigationContexts = new();
        private static readonly ConcurrentLruCache<Type, List<NavigationPropertyInfo>> _navigationPropertyCache = new(maxSize: 1000);
        private static readonly ConditionalWeakTable<DbContext, BatchedNavigationLoader> _navigationLoaders = new();

        /// <summary>
        /// Enables lazy loading for an entity instance
        /// </summary>
        public static T EnableLazyLoading<T>(this T entity, DbContext context) where T : class
        {
            if (entity == null) return null!;
            
            var navContext = _navigationContexts.GetValue(entity, _ => new NavigationContext(context, typeof(T)));
            
            // Initialize navigation properties with lazy loading proxies
            InitializeNavigationProperties(entity, navContext);
            
            return entity;
        }

        /// <summary>
        /// Removes navigation context tracking for an entity and cleans up lazy proxies.
        /// </summary>
        public static void CleanupNavigationContext<T>(T entity) where T : class
        {
            if (entity != null && _navigationContexts.TryGetValue(entity, out _))
            {
                _navigationContexts.Remove(entity);
            }
        }

        /// <summary>
        /// Loads a navigation property explicitly
        /// </summary>
        public static async Task LoadAsync<T, TProperty>(this T entity, 
            System.Linq.Expressions.Expression<Func<T, TProperty?>> navigationProperty, 
            CancellationToken ct = default) 
            where T : class
            where TProperty : class
        {
            if (entity == null || navigationProperty == null) return;
            
            if (!_navigationContexts.TryGetValue(entity, out var navContext))
                throw new InvalidOperationException("Entity must be loaded from a DbContext or have lazy loading enabled to use LoadAsync");
            
            var propertyInfo = GetPropertyInfo(navigationProperty);
            await LoadNavigationPropertyAsync(entity, propertyInfo, navContext, ct);
        }

        /// <summary>
        /// Loads a collection navigation property explicitly
        /// </summary>
        public static async Task LoadAsync<T, TProperty>(this T entity, 
            System.Linq.Expressions.Expression<Func<T, ICollection<TProperty>?>> navigationProperty, 
            CancellationToken ct = default) 
            where T : class
            where TProperty : class
        {
            if (entity == null || navigationProperty == null) return;
            
            if (!_navigationContexts.TryGetValue(entity, out var navContext))
                throw new InvalidOperationException("Entity must be loaded from a DbContext or have lazy loading enabled to use LoadAsync");
            
            var propertyInfo = GetPropertyInfo(navigationProperty);
            await LoadNavigationPropertyAsync(entity, propertyInfo, navContext, ct);
        }

        /// <summary>
        /// Checks if a navigation property is loaded
        /// </summary>
        public static bool IsLoaded<T, TProperty>(this T entity, 
            System.Linq.Expressions.Expression<Func<T, TProperty?>> navigationProperty) 
            where T : class
        {
            if (entity == null || navigationProperty == null) return false;
            
            if (!_navigationContexts.TryGetValue(entity, out var navContext))
                return false;
            
            var propertyInfo = GetPropertyInfo(navigationProperty);
            return navContext.IsLoaded(propertyInfo.Name);
        }

        internal static async Task LoadNavigationPropertyAsync(object entity, PropertyInfo property, NavigationContext context, CancellationToken ct)
        {
            if (context.IsLoaded(property.Name))
                return;
                
            var entityMapping = context.DbContext.GetMapping(context.EntityType);
            
            // Check if this property has a relationship defined
            if (entityMapping.Relations.TryGetValue(property.Name, out var relation))
            {
                await LoadRelationshipAsync(entity, property, relation, context, ct);
            }
            else
            {
                // Try to infer the relationship
                await LoadInferredRelationshipAsync(entity, property, context, ct);
            }
            
            context.MarkAsLoaded(property.Name);
        }

        private static void InitializeNavigationProperties(object entity, NavigationContext context)
        {
            var entityType = entity.GetType();
            var navigationProperties = GetNavigationProperties(entityType);
            
            foreach (var navProp in navigationProperties)
            {
                // Only initialize lazy loading proxies when the navigation property is null
                if (navProp.Property.GetValue(entity) == null)
                {
                    if (navProp.IsCollection)
                    {
                        // Create lazy collection
                        var collectionType = typeof(LazyNavigationCollection<>).MakeGenericType(navProp.TargetType);
                        var collection = Activator.CreateInstance(collectionType, entity, navProp.Property, context);
                        navProp.Property.SetValue(entity, collection);
                    }
                    else
                    {
                        // Create lazy reference
                        var referenceType = typeof(LazyNavigationReference<>).MakeGenericType(navProp.TargetType);
                        var reference = Activator.CreateInstance(referenceType, entity, navProp.Property, context);
                        navProp.Property.SetValue(entity, reference);
                        context.MarkAsUnloaded(navProp.Property.Name);
                    }
                }
                else
                {
                    // Property already has a value; mark it as loaded so it won't be reloaded
                    context.MarkAsLoaded(navProp.Property.Name);
                }
            }
        }

        private static List<NavigationPropertyInfo> GetNavigationProperties(Type entityType)
        {
            return _navigationPropertyCache.GetOrAdd(entityType, static t =>
            {
                var properties = new List<NavigationPropertyInfo>();

                foreach (var prop in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (prop.GetCustomAttribute<NotMappedAttribute>() != null)
                        continue;

                    // Check if it's a navigation property
                    if (IsNavigationProperty(prop))
                    {
                        var isCollection = typeof(IEnumerable).IsAssignableFrom(prop.PropertyType) &&
                                         prop.PropertyType != typeof(string) &&
                                         prop.PropertyType.IsGenericType;

                        Type targetType;
                        if (isCollection)
                        {
                            targetType = prop.PropertyType.GetGenericArguments()[0];
                        }
                        else if (prop.PropertyType.IsGenericType &&
                                 prop.PropertyType.GetGenericTypeDefinition() == typeof(LazyNavigationReference<>))
                        {
                            targetType = prop.PropertyType.GetGenericArguments()[0];
                        }
                        else
                        {
                            targetType = prop.PropertyType;
                        }

                        properties.Add(new NavigationPropertyInfo(prop, targetType, isCollection));
                    }
                }

                return properties;
            });
        }

        private static bool IsNavigationProperty(PropertyInfo property)
        {
            // A navigation property is:
            // 1. A reference to another entity type (class)
            // 2. A collection of entity types
            // 3. Not a primitive type or string
            // 4. Not explicitly marked as NotMapped
            
            if (property.PropertyType.IsPrimitive ||
                property.PropertyType == typeof(string) ||
                property.PropertyType == typeof(DateTime) ||
                property.PropertyType == typeof(decimal) ||
                property.PropertyType == typeof(Guid))
                return false;

            if (property.PropertyType.IsValueType) // Enums, structs, etc.
                return false;

            if (property.PropertyType.GetCustomAttribute<OwnedAttribute>() != null)
                return false;
                
            // Check if it's a collection
            if (typeof(IEnumerable).IsAssignableFrom(property.PropertyType) && 
                property.PropertyType != typeof(string))
            {
                if (property.PropertyType.IsGenericType)
                {
                    var elementType = property.PropertyType.GetGenericArguments()[0];
                    return elementType.IsClass && !elementType.IsPrimitive && elementType != typeof(string);
                }
                return false;
            }
            
            // It's a reference navigation property if it's a class (excluding string)
            return property.PropertyType.IsClass;
        }

        private static PropertyInfo GetPropertyInfo<T, TProperty>(System.Linq.Expressions.Expression<Func<T, TProperty>> expression)
        {
            if (expression.Body is System.Linq.Expressions.MemberExpression memberExpression)
            {
                return (PropertyInfo)memberExpression.Member;
            }
            
            throw new ArgumentException("Expression must be a property access", nameof(expression));
        }

        private static async Task LoadRelationshipAsync(object entity, PropertyInfo property, TableMapping.Relation relation, NavigationContext context, CancellationToken ct)
        {
            var principalKeyValue = relation.PrincipalKey.Getter(entity);
            if (principalKeyValue == null) return;
            
            var dependentMapping = context.DbContext.GetMapping(relation.DependentType);
            
            if (property.PropertyType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(property.PropertyType))
            {
                // Collection navigation property
                var loader = _navigationLoaders.GetValue(context.DbContext, ctx => new BatchedNavigationLoader(ctx));
                var results = await loader.LoadNavigationAsync(entity, property.Name, ct);

                var collectionType = typeof(List<>).MakeGenericType(relation.DependentType);
                var collection = (IList)Activator.CreateInstance(collectionType)!;
                foreach (var item in results)
                {
                    collection.Add(item);
                }

                property.SetValue(entity, collection);
            }
            else
            {
                // Reference navigation property (one-to-one)
                var result = await ExecuteSingleQueryAsync(context.DbContext, dependentMapping, relation.ForeignKey, principalKeyValue, relation.DependentType, ct);

                if (property.PropertyType.IsGenericType &&
                    property.PropertyType.GetGenericTypeDefinition() == typeof(LazyNavigationReference<>))
                {
                    var reference = property.GetValue(entity);
                    reference?.GetType().GetMethod("SetValue")?.Invoke(reference, new object?[] { result });
                }
                else
                {
                    property.SetValue(entity, result);
                }
            }
        }

        private static async Task LoadInferredRelationshipAsync(object entity, PropertyInfo property, NavigationContext context, CancellationToken ct)
        {
            Type targetType;
            if (property.PropertyType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(property.PropertyType))
            {
                targetType = property.PropertyType.GetGenericArguments()[0];
            }
            else if (property.PropertyType.IsGenericType &&
                     property.PropertyType.GetGenericTypeDefinition() == typeof(LazyNavigationReference<>))
            {
                targetType = property.PropertyType.GetGenericArguments()[0];
            }
            else
            {
                targetType = property.PropertyType;
            }
                
            var targetMapping = context.DbContext.GetMapping(targetType);
            var sourceMapping = context.DbContext.GetMapping(context.EntityType);
            
            // Try to find foreign key relationship
            var sourcePrimaryKey = sourceMapping.KeyColumns.FirstOrDefault();
            if (sourcePrimaryKey == null) return;
            
            var sourcePrimaryKeyValue = sourcePrimaryKey.Getter(entity);
            if (sourcePrimaryKeyValue == null) return;
            
            // Look for foreign key in target type
            var foreignKeyProperty = $"{context.EntityType.Name}Id";
            var foreignKeyColumn = targetMapping.Columns.FirstOrDefault(c => 
                string.Equals(c.PropName, foreignKeyProperty, StringComparison.OrdinalIgnoreCase) ||
                string.Equals(c.PropName, $"{context.EntityType.Name}_{sourcePrimaryKey.PropName}", StringComparison.OrdinalIgnoreCase));
                
            if (foreignKeyColumn != null)
            {
                sourceMapping.Relations[property.Name] = new TableMapping.Relation(property, targetType, sourcePrimaryKey, foreignKeyColumn);
                var loader = _navigationLoaders.GetValue(context.DbContext, ctx => new BatchedNavigationLoader(ctx));
                if (property.PropertyType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(property.PropertyType))
                {
                    // Collection
                    var results = await loader.LoadNavigationAsync(entity, property.Name, ct);

                    var collectionType = typeof(List<>).MakeGenericType(targetType);
                    var collection = (IList)Activator.CreateInstance(collectionType)!;
                    foreach (var item in results)
                    {
                        collection.Add(item);
                    }

                    property.SetValue(entity, collection);
                }
                else
                {
                    // Reference
                    var list = await loader.LoadNavigationAsync(entity, property.Name, ct);
                    var result = list.FirstOrDefault();

                    if (property.PropertyType.IsGenericType &&
                        property.PropertyType.GetGenericTypeDefinition() == typeof(LazyNavigationReference<>))
                    {
                        var reference = property.GetValue(entity);
                        reference?.GetType()?.GetMethod("SetValue")?.Invoke(reference, new object?[] { result });
                    }
                    else
                    {
                        property.SetValue(entity, result);
                    }
                }
            }
        }

        private static async Task<object?> ExecuteSingleQueryAsync(DbContext context, TableMapping mapping, Column foreignKey, object keyValue, Type entityType, CancellationToken ct)
        {
            await context.EnsureConnectionAsync(ct);
            using var cmd = context.Connection.CreateCommand();
            cmd.CommandTimeout = (int)context.Options.TimeoutConfiguration.BaseTimeout.TotalSeconds;
            
            var paramName = context.Provider.ParamPrefix + "fk";
            cmd.CommandText = $"SELECT * FROM {mapping.EscTable} WHERE {foreignKey.EscCol} = {paramName}";
            cmd.AddParam(paramName, keyValue);

            // Apply LIMIT 1 for single result
            var sql = new System.Text.StringBuilder(cmd.CommandText);
            var limitParam = context.Provider.ParamPrefix + "p_limit";
            context.Provider.ApplyPaging(sql, 1, null, limitParam, null);
            cmd.CommandText = sql.ToString();
            cmd.AddParam(limitParam, 1);

            var materializer = Query.QueryTranslator.Rent(context).CreateMaterializer(mapping, entityType);

            using var reader = await cmd.ExecuteReaderWithInterceptionAsync(context, CommandBehavior.Default, ct);
            if (await reader.ReadAsync(ct))
            {
                var entity = await materializer(reader, ct);
                // Enable lazy loading for the loaded entity
                _navigationContexts.GetValue(entity, _ => new NavigationContext(context, entityType));
                context.ChangeTracker.Track(entity, EntityState.Unchanged, mapping);
                return entity;
            }
            
            return null;
        }
    }

    /// <summary>
    /// Holds navigation context for an entity instance
    /// </summary>
    public sealed class NavigationContext
    {
        private readonly ConcurrentDictionary<string, byte> _loadedProperties = new();
        
        public DbContext DbContext { get; }
        public Type EntityType { get; }
        
        public NavigationContext(DbContext dbContext, Type entityType)
        {
            DbContext = dbContext;
            EntityType = entityType;
        }
        
        public bool IsLoaded(string propertyName) => _loadedProperties.ContainsKey(propertyName);
        public void MarkAsLoaded(string propertyName) => _loadedProperties[propertyName] = 0;
        public void MarkAsUnloaded(string propertyName) => _loadedProperties.TryRemove(propertyName, out _);
    }

    /// <summary>
    /// Information about a navigation property
    /// </summary>
    public sealed record NavigationPropertyInfo(PropertyInfo Property, Type TargetType, bool IsCollection);

    /// <summary>
    /// Lazy loading collection that loads data on first enumeration
    /// </summary>
    public sealed class LazyNavigationCollection<T> : ICollection<T>, IList<T>, IAsyncEnumerable<T> where T : class
    {
        private readonly object _parent;
        private readonly PropertyInfo _property;
        private readonly NavigationContext _context;

        public LazyNavigationCollection(object parent, PropertyInfo property, NavigationContext context)
        {
            _parent = parent;
            _property = property;
            _context = context;
        }

        private ICollection<T> GetOrLoadCollection()
        {
            if (!_context.IsLoaded(_property.Name))
            {
                NavigationPropertyExtensions
                    .LoadNavigationPropertyAsync(_parent, _property, _context, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();
            }

            return (ICollection<T>)(_property.GetValue(_parent) ?? throw new InvalidOperationException("The collection could not be loaded."));
        }

        private IList<T> GetOrLoadList() => (IList<T>)GetOrLoadCollection();

        /// <summary>
        /// Returns an enumerator for the collection, loading it if necessary.
        /// </summary>
        public IEnumerator<T> GetEnumerator() => GetOrLoadCollection().GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public async IAsyncEnumerator<T> GetAsyncEnumerator(CancellationToken ct = default)
        {
            if (!_context.IsLoaded(_property.Name))
                await NavigationPropertyExtensions.LoadNavigationPropertyAsync(_parent, _property, _context, ct).ConfigureAwait(false);

            var collection = (IEnumerable<T>)(_property.GetValue(_parent) ?? Array.Empty<T>());
            foreach (var item in collection)
                yield return item;
        }

        /// <summary>
        /// Adds an item to the collection, loading it if necessary.
        /// </summary>
        public void Add(T item) => GetOrLoadCollection().Add(item);

        /// <summary>
        /// Clears the collection, loading it if necessary.
        /// </summary>
        public void Clear() => GetOrLoadCollection().Clear();

        /// <summary>
        /// Determines whether the collection contains the specified item, loading it if necessary.
        /// </summary>
        public bool Contains(T item) => GetOrLoadCollection().Contains(item);

        /// <summary>
        /// Copies the elements of the collection to an array, loading it if necessary.
        /// </summary>
        public void CopyTo(T[] array, int arrayIndex) => GetOrLoadCollection().CopyTo(array, arrayIndex);

        /// <summary>
        /// Removes the first occurrence of a specific object from the collection, loading it if necessary.
        /// </summary>
        public bool Remove(T item) => GetOrLoadCollection().Remove(item);

        /// <summary>
        /// Gets the number of elements in the collection, loading it if necessary.
        /// </summary>
        public int Count => GetOrLoadCollection().Count;

        public bool IsReadOnly => GetOrLoadCollection().IsReadOnly;

        /// <summary>
        /// Searches for the specified object and returns the zero-based index of the first occurrence within the list, loading it if necessary.
        /// </summary>
        public int IndexOf(T item) => GetOrLoadList().IndexOf(item);

        /// <summary>
        /// Inserts an item to the list at the specified index, loading it if necessary.
        /// </summary>
        public void Insert(int index, T item) => GetOrLoadList().Insert(index, item);

        /// <summary>
        /// Removes the list item at the specified index, loading the collection if necessary.
        /// </summary>
        public void RemoveAt(int index) => GetOrLoadList().RemoveAt(index);

        public T this[int index]
        {
            get => GetOrLoadList()[index];
            set => GetOrLoadList()[index] = value;
        }
    }

    /// <summary>
    /// Lazy loading reference that loads data on first access
    /// </summary>
    public sealed class LazyNavigationReference<T> where T : class
    {
        private readonly object _parent;
        private readonly PropertyInfo _property;
        private readonly NavigationContext _context;
        private T? _value;
        private volatile bool _isLoaded;
        private readonly object _loadLock = new();

        public LazyNavigationReference(object parent, PropertyInfo property, NavigationContext context)
        {
            _parent = parent;
            _property = property;
            _context = context;
            _isLoaded = false;
        }

        public async Task<T?> GetValueAsync(CancellationToken ct = default)
        {
            if (!_isLoaded)
                await NavigationPropertyExtensions.LoadNavigationPropertyAsync(_parent, _property, _context, ct).ConfigureAwait(false);

            return _value;
        }

        public void SetValue(T? value)
        {
            lock (_loadLock)
            {
                _value = value;
                _isLoaded = true;
                _context.MarkAsLoaded(_property.Name);
            }
        }

        public static implicit operator Task<T?>(LazyNavigationReference<T> reference) => reference.GetValueAsync();
    }
}
