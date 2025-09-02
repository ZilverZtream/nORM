using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
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
        private static readonly ConditionalWeakTable<object, NavigationContext> _navigationContexts = new();
        private static readonly Dictionary<Type, List<NavigationPropertyInfo>> _navigationPropertyCache = new();
        private static readonly object _cacheLock = new object();

        /// <summary>
        /// Enables lazy loading for an entity instance
        /// </summary>
        public static T EnableLazyLoading<T>(this T entity, DbContext context) where T : class
        {
            if (entity == null) return null!;
            
            var navContext = new NavigationContext(context, typeof(T));
            _navigationContexts.AddOrUpdate(entity, navContext);
            
            // Initialize navigation properties with lazy loading proxies
            InitializeNavigationProperties(entity, navContext);
            
            return entity;
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
                if (navProp.IsCollection)
                {
                    // Create lazy collection
                    var collectionType = typeof(LazyNavigationCollection<>).MakeGenericType(navProp.TargetType);
                    var collection = Activator.CreateInstance(collectionType, entity, navProp.Property, context);
                    navProp.Property.SetValue(entity, collection);
                }
                else
                {
                    // For reference properties, we'll load on first access through a proxy
                    // Initially set to null, will be loaded when accessed
                    if (navProp.Property.GetValue(entity) == null)
                    {
                        context.MarkAsUnloaded(navProp.Property.Name);
                    }
                }
            }
        }

        private static List<NavigationPropertyInfo> GetNavigationProperties(Type entityType)
        {
            lock (_cacheLock)
            {
                if (_navigationPropertyCache.TryGetValue(entityType, out var cached))
                    return cached;

                var properties = new List<NavigationPropertyInfo>();
                
                foreach (var prop in entityType.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    if (prop.GetCustomAttribute<NotMappedAttribute>() != null)
                        continue;
                        
                    // Check if it's a navigation property
                    if (IsNavigationProperty(prop))
                    {
                        var isCollection = typeof(IEnumerable).IsAssignableFrom(prop.PropertyType) && 
                                         prop.PropertyType != typeof(string) &&
                                         prop.PropertyType.IsGenericType;
                        
                        var targetType = isCollection 
                            ? prop.PropertyType.GetGenericArguments()[0]
                            : prop.PropertyType;
                            
                        properties.Add(new NavigationPropertyInfo(prop, targetType, isCollection));
                    }
                }
                
                _navigationPropertyCache[entityType] = properties;
                return properties;
            }
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
                var results = await ExecuteCollectionQueryAsync(context.DbContext, dependentMapping, relation.ForeignKey, principalKeyValue, relation.DependentType, ct);
                
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
                property.SetValue(entity, result);
            }
        }

        private static async Task LoadInferredRelationshipAsync(object entity, PropertyInfo property, NavigationContext context, CancellationToken ct)
        {
            var targetType = property.PropertyType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(property.PropertyType)
                ? property.PropertyType.GetGenericArguments()[0]
                : property.PropertyType;
                
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
                if (property.PropertyType.IsGenericType && typeof(IEnumerable).IsAssignableFrom(property.PropertyType))
                {
                    // Collection
                    var results = await ExecuteCollectionQueryAsync(context.DbContext, targetMapping, foreignKeyColumn, sourcePrimaryKeyValue, targetType, ct);
                    
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
                    var result = await ExecuteSingleQueryAsync(context.DbContext, targetMapping, foreignKeyColumn, sourcePrimaryKeyValue, targetType, ct);
                    property.SetValue(entity, result);
                }
            }
        }

        private static async Task<List<object>> ExecuteCollectionQueryAsync(DbContext context, TableMapping mapping, Column foreignKey, object keyValue, Type entityType, CancellationToken ct)
        {
            using var cmd = context.Connection.CreateCommand();
            cmd.CommandTimeout = (int)context.Options.CommandTimeout.TotalSeconds;
            
            var paramName = context.Provider.ParamPrefix + "fk";
            cmd.CommandText = $"SELECT * FROM {mapping.EscTable} WHERE {foreignKey.EscCol} = {paramName}";
            cmd.AddParam(paramName, keyValue);

            var materializer = new Query.QueryTranslator(context).CreateMaterializer(mapping, entityType);
            var results = new List<object>();
            
            using var reader = await cmd.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                var entity = materializer(reader);
                // Enable lazy loading for the loaded entity
                _navigationContexts.AddOrUpdate(entity, new NavigationContext(context, entityType));
                results.Add(entity);
            }
            
            return results;
        }

        private static async Task<object?> ExecuteSingleQueryAsync(DbContext context, TableMapping mapping, Column foreignKey, object keyValue, Type entityType, CancellationToken ct)
        {
            using var cmd = context.Connection.CreateCommand();
            cmd.CommandTimeout = (int)context.Options.CommandTimeout.TotalSeconds;
            
            var paramName = context.Provider.ParamPrefix + "fk";
            cmd.CommandText = $"SELECT * FROM {mapping.EscTable} WHERE {foreignKey.EscCol} = {paramName}";
            cmd.AddParam(paramName, keyValue);
            
            // Apply LIMIT 1 for single result
            var sql = new System.Text.StringBuilder(cmd.CommandText);
            context.Provider.ApplyPaging(sql, 1, null);
            cmd.CommandText = sql.ToString();

            var materializer = new Query.QueryTranslator(context).CreateMaterializer(mapping, entityType);
            
            using var reader = await cmd.ExecuteReaderAsync(ct);
            if (await reader.ReadAsync(ct))
            {
                var entity = materializer(reader);
                // Enable lazy loading for the loaded entity
                _navigationContexts.AddOrUpdate(entity, new NavigationContext(context, entityType));
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
        private readonly HashSet<string> _loadedProperties = new();
        
        public DbContext DbContext { get; }
        public Type EntityType { get; }
        
        public NavigationContext(DbContext dbContext, Type entityType)
        {
            DbContext = dbContext;
            EntityType = entityType;
        }
        
        public bool IsLoaded(string propertyName) => _loadedProperties.Contains(propertyName);
        public void MarkAsLoaded(string propertyName) => _loadedProperties.Add(propertyName);
        public void MarkAsUnloaded(string propertyName) => _loadedProperties.Remove(propertyName);
    }

    /// <summary>
    /// Information about a navigation property
    /// </summary>
    public sealed record NavigationPropertyInfo(PropertyInfo Property, Type TargetType, bool IsCollection);

    /// <summary>
    /// Lazy loading collection that loads data on first enumeration
    /// </summary>
    public sealed class LazyNavigationCollection<T> : ICollection<T>, IList<T> where T : class
    {
        private readonly object _parent;
        private readonly PropertyInfo _property;
        private readonly NavigationContext _context;
        private List<T>? _data;
        private bool _isLoaded;

        public LazyNavigationCollection(object parent, PropertyInfo property, NavigationContext context)
        {
            _parent = parent;
            _property = property;
            _context = context;
            _isLoaded = false;
        }

        private async Task EnsureLoadedAsync()
        {
            if (_isLoaded) return;
            
            await NavigationPropertyExtensions.LoadNavigationPropertyAsync(_parent, _property, _context, CancellationToken.None);
            _data = (List<T>?)_property.GetValue(_parent) ?? new List<T>();
            _isLoaded = true;
        }

        public IEnumerator<T> GetEnumerator()
        {
            EnsureLoadedAsync().GetAwaiter().GetResult();
            return _data!.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public void Add(T item)
        {
            EnsureLoadedAsync().GetAwaiter().GetResult();
            _data!.Add(item);
        }

        public void Clear()
        {
            EnsureLoadedAsync().GetAwaiter().GetResult();
            _data!.Clear();
        }

        public bool Contains(T item)
        {
            EnsureLoadedAsync().GetAwaiter().GetResult();
            return _data!.Contains(item);
        }

        public void CopyTo(T[] array, int arrayIndex)
        {
            EnsureLoadedAsync().GetAwaiter().GetResult();
            _data!.CopyTo(array, arrayIndex);
        }

        public bool Remove(T item)
        {
            EnsureLoadedAsync().GetAwaiter().GetResult();
            return _data!.Remove(item);
        }

        public int Count
        {
            get
            {
                EnsureLoadedAsync().GetAwaiter().GetResult();
                return _data!.Count;
            }
        }

        public bool IsReadOnly => false;

        public int IndexOf(T item)
        {
            EnsureLoadedAsync().GetAwaiter().GetResult();
            return _data!.IndexOf(item);
        }

        public void Insert(int index, T item)
        {
            EnsureLoadedAsync().GetAwaiter().GetResult();
            _data!.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            EnsureLoadedAsync().GetAwaiter().GetResult();
            _data!.RemoveAt(index);
        }

        public T this[int index]
        {
            get
            {
                EnsureLoadedAsync().GetAwaiter().GetResult();
                return _data![index];
            }
            set
            {
                EnsureLoadedAsync().GetAwaiter().GetResult();
                _data![index] = value;
            }
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
        private bool _isLoaded;

        public LazyNavigationReference(object parent, PropertyInfo property, NavigationContext context)
        {
            _parent = parent;
            _property = property;
            _context = context;
            _isLoaded = false;
        }

        public T? Value
        {
            get
            {
                if (!_isLoaded)
                {
                    NavigationPropertyExtensions.LoadNavigationPropertyAsync(_parent, _property, _context, CancellationToken.None)
                        .GetAwaiter().GetResult();
                    _value = (T?)_property.GetValue(_parent);
                    _isLoaded = true;
                }
                return _value;
            }
            set
            {
                _value = value;
                _property.SetValue(_parent, value);
                _isLoaded = true;
                _context.MarkAsLoaded(_property.Name);
            }
        }
    }
}
