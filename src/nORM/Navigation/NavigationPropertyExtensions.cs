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
using nORM.Query;
using nORM.Core;
using nORM.Mapping;
using nORM.Internal;
using nORM.Execution;

#nullable enable

namespace nORM.Navigation
{
    /// <summary>
    /// Provides extension methods for lazy loading and explicit loading of navigation properties.
    /// Delivers an EF Core-like experience without sacrificing nORM's performance characteristics.
    /// </summary>
    public static class NavigationPropertyExtensions
    {
        internal static readonly ConditionalWeakTable<object, NavigationContext> _navigationContexts = new();

        /// <summary>Maximum number of entity types cached for navigation property discovery.</summary>
        private const int NavigationPropertyCacheMaxSize = 1000;

        private static readonly ConcurrentLruCache<Type, List<NavigationPropertyInfo>> _navigationPropertyCache = new(maxSize: NavigationPropertyCacheMaxSize);
        private static readonly ConditionalWeakTable<DbContext, BatchedNavigationLoader> _navigationLoaders = new();
        private static readonly ConcurrentDictionary<BatchedNavigationLoader, byte> _activeLoaders = new();

        /// <summary>
        /// Registers a <see cref="BatchedNavigationLoader"/> so that pending navigation requests
        /// can be tracked and cleaned up when entities are disposed.
        /// </summary>
        /// <param name="loader">The loader instance to register.</param>
        internal static void RegisterLoader(BatchedNavigationLoader loader) => _activeLoaders[loader] = 0;
        internal static void UnregisterLoader(BatchedNavigationLoader loader) => _activeLoaders.TryRemove(loader, out _);

        /// <summary>
        /// Enables lazy loading for an entity instance by attaching lazy-loading proxies to
        /// all eligible navigation properties.
        /// </summary>
        /// <exception cref="ArgumentNullException"><paramref name="entity"/> or <paramref name="context"/> is <c>null</c>.</exception>
        public static T EnableLazyLoading<T>(this T entity, DbContext context) where T : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (context == null) throw new ArgumentNullException(nameof(context));

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
            if (entity != null && _navigationContexts.TryGetValue(entity, out var context))
            {
                context?.Dispose();
                _navigationContexts.Remove(entity);
                CleanupFromBatchedLoaders(entity);
            }
        }

        private static void CleanupFromBatchedLoaders(object entity)
        {
            foreach (var loader in _activeLoaders.Keys)
            {
                loader.RemovePendingLoadsForEntity(entity);
            }
        }

        /// <summary>
        /// Explicitly loads a reference navigation property from the database.
        /// </summary>
        public static async Task LoadAsync<T, TProperty>(this T entity,
            System.Linq.Expressions.Expression<Func<T, TProperty?>> navigationProperty,
            CancellationToken ct = default)
            where T : class
            where TProperty : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (navigationProperty == null) throw new ArgumentNullException(nameof(navigationProperty));

            if (!_navigationContexts.TryGetValue(entity, out var navContext))
                throw new InvalidOperationException("Entity must be loaded from a DbContext or have lazy loading enabled to use LoadAsync");

            var propertyInfo = GetPropertyInfo(navigationProperty);
            await LoadNavigationPropertyAsync(entity, propertyInfo, navContext, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Explicitly loads a collection navigation property from the database.
        /// </summary>
        public static async Task LoadAsync<T, TProperty>(this T entity,
            System.Linq.Expressions.Expression<Func<T, ICollection<TProperty>?>> navigationProperty,
            CancellationToken ct = default)
            where T : class
            where TProperty : class
        {
            if (entity == null) throw new ArgumentNullException(nameof(entity));
            if (navigationProperty == null) throw new ArgumentNullException(nameof(navigationProperty));
            
            if (!_navigationContexts.TryGetValue(entity, out var navContext))
                throw new InvalidOperationException("Entity must be loaded from a DbContext or have lazy loading enabled to use LoadAsync");
            
            var propertyInfo = GetPropertyInfo(navigationProperty);
            await LoadNavigationPropertyAsync(entity, propertyInfo, navContext, ct).ConfigureAwait(false);
        }

        /// <summary>
        /// Checks whether a navigation property has already been loaded for the given entity.
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
                await LoadRelationshipAsync(entity, property, relation, context, ct).ConfigureAwait(false);
            }
            else
            {
                // Try to infer the relationship
                await LoadInferredRelationshipAsync(entity, property, context, ct).ConfigureAwait(false);
            }
            
            context.MarkAsLoaded(property.Name);
        }

        internal static void LoadNavigationProperty(object entity, PropertyInfo property, NavigationContext context, CancellationToken ct)
        {
            // Known limitation: sync-over-async is required here because this method is called from
            // synchronous lazy-loading entry points (e.g., ICollection<T> accessors on LazyNavigationCollection,
            // IEnumerator.GetEnumerator). The translator/materializer context is inherently synchronous,
            // and there is no way to propagate async through the ICollection/IList interface contracts.
            LoadNavigationPropertyAsync(entity, property, context, ct)
                .ConfigureAwait(false)
                .GetAwaiter()
                .GetResult();
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
                        // Create lazy collection only if the property type can accept it
                        var collectionType = typeof(LazyNavigationCollection<>).MakeGenericType(navProp.TargetType);
                        if (navProp.Property.PropertyType.IsAssignableFrom(collectionType))
                        {
                            var collection = Activator.CreateInstance(collectionType, entity, navProp.Property, context);
                            navProp.Property.SetValue(entity, collection);
                        }
                        // If property type is List<T> or similar concrete type, skip lazy proxy;
                        // the property stays null and won't interfere with owned collection loading.
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
                            var args = prop.PropertyType.GetGenericArguments();
                            if (args.Length == 0) continue;
                            targetType = args[0];
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
            // A navigation property is one of:
            // 1. A collection of entity types
            // 2. A LazyNavigationReference<T> for reference navigation

            if (property.PropertyType.IsPrimitive ||
                property.PropertyType == typeof(string) ||
                property.PropertyType == typeof(DateTime) ||
                property.PropertyType == typeof(decimal) ||
                property.PropertyType == typeof(Guid))
                return false;

            if (property.PropertyType.IsValueType)
                return false;

            if (property.PropertyType.GetCustomAttribute<OwnedAttribute>() != null)
                return false;

            // Collection navigation property
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

            // Reference navigation property must be LazyNavigationReference<T>
            return property.PropertyType.IsGenericType &&
                   property.PropertyType.GetGenericTypeDefinition() == typeof(LazyNavigationReference<>);
        }

        private static PropertyInfo GetPropertyInfo<T, TProperty>(System.Linq.Expressions.Expression<Func<T, TProperty>> expression)
        {
            var body = expression.Body;

            // Unwrap Convert/ConvertChecked nodes that the compiler may insert for
            // covariant / interface property access expressions.
            if (body is System.Linq.Expressions.UnaryExpression unary &&
                (unary.NodeType == System.Linq.Expressions.ExpressionType.Convert ||
                 unary.NodeType == System.Linq.Expressions.ExpressionType.ConvertChecked))
            {
                body = unary.Operand;
            }

            if (body is System.Linq.Expressions.MemberExpression memberExpression &&
                memberExpression.Member is PropertyInfo propertyInfo)
            {
                return propertyInfo;
            }

            throw new ArgumentException("Expression must be a property access", nameof(expression));
        }

        private static async Task LoadRelationshipAsync(object entity, PropertyInfo property, TableMapping.Relation relation, NavigationContext context, CancellationToken ct)
        {
            if (relation.PrincipalKey == null) return;
            var principalKeyValue = relation.PrincipalKey.Getter(entity);
            if (principalKeyValue == null) return;

            if (property.PropertyType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(property.PropertyType))
            {
                // Collection navigation property
                var loader = _navigationLoaders.GetValue(context.DbContext, ctx => new BatchedNavigationLoader(ctx));
                var results = await loader.LoadNavigationAsync(entity, property.Name, ct).ConfigureAwait(false);

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
                var dependentMapping = context.DbContext.GetMapping(relation.DependentType);
                var result = await ExecuteSingleQueryAsync(context.DbContext, dependentMapping, relation.ForeignKey, principalKeyValue, relation.DependentType, ct).ConfigureAwait(false);

                if (property.PropertyType.IsGenericType &&
                    property.PropertyType.GetGenericTypeDefinition() == typeof(LazyNavigationReference<>))
                {
                    try
                    {
                        var reference = property.GetValue(entity);
                        reference?.GetType().GetMethod("SetValue")?.Invoke(reference, new object?[] { result });
                    }
                    catch (TargetInvocationException ex)
                    {
                        throw new InvalidOperationException(
                            $"Failed to set navigation property '{property.Name}' on entity type '{entity.GetType().Name}'.",
                            ex.InnerException ?? ex);
                    }
                    catch (Exception ex) when (ex is not InvalidOperationException)
                    {
                        throw new InvalidOperationException(
                            $"Failed to set navigation property '{property.Name}' on entity type '{entity.GetType().Name}'.",
                            ex);
                    }
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
                if (!sourceMapping.Relations.ContainsKey(property.Name))
                    sourceMapping.Relations[property.Name] = new TableMapping.Relation(property, targetType, sourcePrimaryKey, foreignKeyColumn);
                var loader = _navigationLoaders.GetValue(context.DbContext, ctx => new BatchedNavigationLoader(ctx));
                if (property.PropertyType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(property.PropertyType))
                {
                    // Collection
                    var results = await loader.LoadNavigationAsync(entity, property.Name, ct).ConfigureAwait(false);

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
                    var list = await loader.LoadNavigationAsync(entity, property.Name, ct).ConfigureAwait(false);
                    var result = list.FirstOrDefault();

                    if (property.PropertyType.IsGenericType &&
                        property.PropertyType.GetGenericTypeDefinition() == typeof(LazyNavigationReference<>))
                    {
                        try
                        {
                            var reference = property.GetValue(entity);
                            reference?.GetType()?.GetMethod("SetValue")?.Invoke(reference, new object?[] { result });
                        }
                        catch (TargetInvocationException ex)
                        {
                            throw new InvalidOperationException(
                                $"Failed to set navigation property '{property.Name}' on entity type '{entity.GetType().Name}'.",
                                ex.InnerException ?? ex);
                        }
                        catch (Exception ex) when (ex is not InvalidOperationException)
                        {
                            throw new InvalidOperationException(
                                $"Failed to set navigation property '{property.Name}' on entity type '{entity.GetType().Name}'.",
                                ex);
                        }
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
            await context.EnsureConnectionAsync(ct).ConfigureAwait(false);
            using var cmd = context.CreateCommand();

            var paramName = context.Provider.ParamPrefix + "fk";
            cmd.CommandText = $"SELECT * FROM {mapping.EscTable} WHERE {foreignKey.EscCol} = {paramName}";
            cmd.CommandTimeout = ToSecondsClamped(context.GetAdaptiveTimeout(AdaptiveTimeoutManager.OperationType.SimpleSelect, cmd.CommandText));
            cmd.AddParam(paramName, keyValue);

            // Apply LIMIT 1 for single result
            using (var sb = new OptimizedSqlBuilder(cmd.CommandText.Length + 20))
            {
                sb.Append(cmd.CommandText);
                var limitParam = context.Provider.ParamPrefix + "p_limit";
                context.Provider.ApplyPaging(sb, 1, null, limitParam, null);
                cmd.CommandText = sb.ToSqlString();
                cmd.AddParam(limitParam, 1);
            }

            using var translator = Query.QueryTranslator.Rent(context);
            var materializer = translator.CreateMaterializer(mapping, entityType);

            using var reader = await cmd.ExecuteReaderWithInterceptionAsync(context, CommandBehavior.Default, ct).ConfigureAwait(false);
            if (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var entity = await materializer(reader, ct).ConfigureAwait(false);
                var entry = context.ChangeTracker.Track(entity, EntityState.Unchanged, mapping);
                entity = entry.Entity!;
                // Enable lazy loading for the loaded entity
                _navigationContexts.GetValue(entity, _ => new NavigationContext(context, entityType));
                return entity;
            }
            
            return null;
        }

        /// <summary>
        /// Converts a <see cref="TimeSpan"/> to whole seconds suitable for
        /// <see cref="System.Data.Common.DbCommand.CommandTimeout"/>, clamping to a minimum of 1
        /// and guarding against overflow. Mirrors the helper used by <see cref="DbContext"/>.
        /// </summary>
        private static int ToSecondsClamped(TimeSpan t)
        {
            if (t.TotalSeconds > int.MaxValue)
                return int.MaxValue;

            return Math.Max(1, (int)Math.Ceiling(t.TotalSeconds));
        }
    }

    /// <summary>
    /// Tracks which navigation properties have been loaded for a particular entity instance,
    /// along with the owning <see cref="DbContext"/> and entity type.
    /// </summary>
    public sealed class NavigationContext : IDisposable
    {
        private readonly ConcurrentDictionary<string, byte> _loadedProperties = new();

        /// <summary>
        /// Gets the owning <see cref="DbContext"/> used to load navigation properties.
        /// </summary>
        public DbContext DbContext { get; }

        /// <summary>
        /// Gets the <see cref="Type"/> of the entity associated with this context.
        /// </summary>
        public Type EntityType { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="NavigationContext"/> class.
        /// </summary>
        /// <param name="dbContext">The associated context.</param>
        /// <param name="entityType">The entity type this context relates to.</param>
        /// <exception cref="ArgumentNullException"><paramref name="dbContext"/> or <paramref name="entityType"/> is <c>null</c>.</exception>
        public NavigationContext(DbContext dbContext, Type entityType)
        {
            DbContext = dbContext ?? throw new ArgumentNullException(nameof(dbContext));
            EntityType = entityType ?? throw new ArgumentNullException(nameof(entityType));
        }
        
        /// <summary>
        /// Determines whether the specified navigation property has already been loaded for the entity.
        /// </summary>
        /// <param name="propertyName">The name of the navigation property.</param>
        /// <returns><c>true</c> if the property is considered loaded; otherwise, <c>false</c>.</returns>
        public bool IsLoaded(string propertyName) => _loadedProperties.ContainsKey(propertyName);

        /// <summary>
        /// Marks the specified navigation property as loaded within this context.
        /// </summary>
        /// <param name="propertyName">The name of the navigation property.</param>
        public void MarkAsLoaded(string propertyName) => _loadedProperties[propertyName] = 0;

        /// <summary>
        /// Marks the specified navigation property as unloaded so it will be reloaded on next access.
        /// </summary>
        /// <param name="propertyName">The name of the navigation property.</param>
        public void MarkAsUnloaded(string propertyName) => _loadedProperties.TryRemove(propertyName, out _);

        /// <summary>
        /// Clears the loaded-property tracking state. Safe to call multiple times.
        /// </summary>
        public void Dispose() => _loadedProperties.Clear();
    }

    /// <summary>
    /// Metadata describing a discovered navigation property: its reflection handle, the related
    /// entity type, and whether it represents a collection or reference navigation.
    /// </summary>
    public sealed record NavigationPropertyInfo(PropertyInfo Property, Type TargetType, bool IsCollection);

    /// <summary>
    /// Lazy loading collection that defers database access until first enumeration or mutation.
    /// </summary>
    public sealed class LazyNavigationCollection<T> : ICollection<T>, IList<T>, IAsyncEnumerable<T> where T : class
    {
        private readonly object _parent;
        private readonly PropertyInfo _property;
        private readonly NavigationContext _context;

        /// <summary>
        /// Initializes a new lazy collection wrapper for the specified navigation property.
        /// </summary>
        /// <param name="parent">Entity that owns the navigation property.</param>
        /// <param name="property">Reflection metadata describing the navigation.</param>
        /// <param name="context">Navigation loading context.</param>
        /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
        public LazyNavigationCollection(object parent, PropertyInfo property, NavigationContext context)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            _property = property ?? throw new ArgumentNullException(nameof(property));
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        private ICollection<T> GetOrLoadCollection()
        {
            if (!_context.IsLoaded(_property.Name))
            {
                NavigationPropertyExtensions.LoadNavigationProperty(
                    _parent,
                    _property,
                    _context,
                    CancellationToken.None);
            }

            return (ICollection<T>)(_property.GetValue(_parent) ?? throw new InvalidOperationException("The collection could not be loaded."));
        }

        private IList<T> GetOrLoadList() => (IList<T>)GetOrLoadCollection();

        /// <summary>
        /// Returns an enumerator for the collection, loading it if necessary.
        /// </summary>
        public IEnumerator<T> GetEnumerator() => GetOrLoadCollection().GetEnumerator();

        /// <summary>
        /// Returns a non-generic enumerator for the collection.
        /// </summary>
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>
        /// Asynchronously enumerates the items in the collection, ensuring the navigation is loaded.
        /// </summary>
        /// <param name="ct">A cancellation token used to cancel the asynchronous iteration.</param>
        /// <returns>An asynchronous enumerator over the collection.</returns>
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

        /// <summary>
        /// Gets a value indicating whether the underlying collection is read-only.
        /// Accessing this property triggers lazy loading if the collection has not yet
        /// been loaded.
        /// </summary>
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

        /// <summary>
        /// Gets or sets the element at the specified index, loading the collection if
        /// necessary.
        /// </summary>
        public T this[int index]
        {
            get => GetOrLoadList()[index];
            set => GetOrLoadList()[index] = value;
        }
    }

    /// <summary>
    /// Lazy loading reference that defers database access until the value is first requested.
    /// </summary>
    public sealed class LazyNavigationReference<T> where T : class
    {
        private readonly object _parent;
        private readonly PropertyInfo _property;
        private readonly NavigationContext _context;
        private T? _value;
        private volatile bool _isLoaded;
        private readonly object _loadLock = new();

        /// <summary>
        /// Initializes a new lazy reference wrapper for the specified navigation property.
        /// </summary>
        /// <param name="parent">Entity that owns the navigation property.</param>
        /// <param name="property">Reflection metadata describing the navigation.</param>
        /// <param name="context">Navigation loading context.</param>
        /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
        public LazyNavigationReference(object parent, PropertyInfo property, NavigationContext context)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            _property = property ?? throw new ArgumentNullException(nameof(property));
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _isLoaded = false;
        }

        /// <summary>
        /// Gets the referenced entity value, loading it from the database if it has not been loaded yet.
        /// </summary>
        /// <param name="ct">A cancellation token that can be used to cancel the asynchronous operation.</param>
        /// <returns>The referenced entity, or <c>null</c> if no related entity exists.</returns>
        public async Task<T?> GetValueAsync(CancellationToken ct = default)
        {
            if (!_isLoaded)
                await NavigationPropertyExtensions.LoadNavigationPropertyAsync(_parent, _property, _context, ct).ConfigureAwait(false);

            return _value;
        }

        /// <summary>
        /// Sets the referenced entity and marks the navigation as loaded.
        /// </summary>
        /// <param name="value">The entity to assign to the navigation property.</param>
        public void SetValue(T? value)
        {
            lock (_loadLock)
            {
                _value = value;
                _isLoaded = true;
                _context.MarkAsLoaded(_property.Name);
            }
        }

        /// <summary>
        /// Implicitly converts the reference to a task that retrieves the value.
        /// </summary>
        /// <exception cref="ArgumentNullException"><paramref name="reference"/> is <c>null</c>.</exception>
        public static implicit operator Task<T?>(LazyNavigationReference<T> reference)
        {
            if (reference == null) throw new ArgumentNullException(nameof(reference));
            return reference.GetValueAsync();
        }
    }
}
