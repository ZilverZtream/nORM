using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using System.Linq.Expressions;

#nullable enable

namespace nORM.Mapping
{
    /// <summary>
    /// Describes how a CLR type maps to a database table including column and relationship metadata.
    /// </summary>
    public sealed class TableMapping
    {
        /// <summary>Gets the CLR type represented by this mapping.</summary>
        public readonly Type Type;

        /// <summary>Gets the escaped table name used in SQL statements.</summary>
        public string EscTable;

        /// <summary>Gets all mapped columns.</summary>
        public readonly Column[] Columns;

        /// <summary>Gets a lookup of columns by property name.</summary>
        public readonly Dictionary<string, Column> ColumnsByName;

        /// <summary>Gets the columns that form the primary key.</summary>
        public readonly Column[] KeyColumns;

        /// <summary>Gets the timestamp column used for concurrency, if any.</summary>
        public readonly Column? TimestampColumn;

        /// <summary>Gets the tenant discriminator column, if multi-tenancy is enabled.</summary>
        public readonly Column? TenantColumn;

        /// <summary>Gets the set of columns included in insert statements.</summary>
        public readonly Column[] InsertColumns;

        /// <summary>Gets the set of columns included in update statements.</summary>
        public readonly Column[] UpdateColumns;

        /// <summary>Gets the unescaped table name.</summary>
        public string TableName { get; }

        /// <summary>Gets the relationships to dependent entities.</summary>
        public readonly Dictionary<string, Relation> Relations = new();

        /// <summary>Gets the database provider associated with this mapping.</summary>
        public readonly DatabaseProvider Provider;

        /// <summary>Gets the discriminator column used for TPH inheritance, if any.</summary>
        public readonly Column? DiscriminatorColumn = null;

        /// <summary>Gets mappings for derived types in TPH inheritance scenarios.</summary>
        public readonly Dictionary<object, TableMapping> TphMappings = new();

        /// <summary>Gets a fingerprint hash of all converter configurations for cache differentiation.</summary>
        public int ConverterFingerprint { get; private set; }

        /// <summary>Gets a fingerprint hash of shadow property columns for cache differentiation.</summary>
        public int ShadowFingerprint { get; private set; }

        /// <summary>Gets the owned collection mappings for this entity type.</summary>
        public readonly List<OwnedCollectionMapping> OwnedCollections = new();

        /// <summary>Gets the many-to-many join table mappings for this entity type.</summary>
        public readonly List<JoinTableMapping> ManyToManyJoins = new();

        private readonly IEntityTypeConfiguration? _fluentConfig;

        // Cache derived type discovery to avoid an expensive AppDomain.GetAssemblies() scan on every mapping
        // (scanning all assemblies can take 100–1000 ms+ in large applications).
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type, List<Type>> _derivedTypesCache = new();

        /// <summary>
        /// Creates a new <see cref="TableMapping"/> for the specified type and provider.
        /// </summary>
        /// <param name="t">CLR type being mapped.</param>
        /// <param name="p">Database provider used for identifier escaping.</param>
        /// <param name="ctx">Context used to resolve related mappings.</param>
        /// <param name="fluentConfig">Optional configuration overrides.</param>
        public TableMapping(Type t, DatabaseProvider p, DbContext ctx, IEntityTypeConfiguration? fluentConfig)
        {
            Type = t;
            Provider = p;
            _fluentConfig = fluentConfig;

            var splitAttr = t.GetCustomAttribute<TableSplitAttribute>();
            var splitType = fluentConfig?.TableSplitWith ?? splitAttr?.PrincipalType;
            if (splitType != null)
            {
                var principal = ctx.GetMapping(splitType);
                EscTable = principal.EscTable;
                TableName = principal.TableName;
            }
            else
            {
                var tableName = fluentConfig?.TableName ?? t.GetCustomAttribute<TableAttribute>()?.Name ?? t.Name;
                EscTable = p.Escape(tableName);
                TableName = tableName;
            }

            var cols = ColumnMappingCache.GetCachedColumns(t, p, fluentConfig).ToList();

            if (fluentConfig?.ShadowProperties.Count > 0)
            {
                foreach (var sp in fluentConfig.ShadowProperties)
                {
                    cols.Add(new Column(sp.Key, sp.Value.ClrType, t, p, sp.Value.ColumnName));
                }
            }

            var discriminatorAttr = t.GetCustomAttribute<DiscriminatorColumnAttribute>();
            if (discriminatorAttr != null)
            {
                DiscriminatorColumn = cols.FirstOrDefault(c => string.Equals(c.Prop.Name, discriminatorAttr.PropertyName, StringComparison.OrdinalIgnoreCase));

                // Use cached derived type discovery instead of scanning all assemblies on each call.
                var derivedTypes = _derivedTypesCache.GetOrAdd(t, baseType =>
                {
                    // Only scan assemblies once per base type, then cache forever
                    var types = new List<Type>();
                    foreach (var assembly in AppDomain.CurrentDomain.GetAssemblies())
                    {
                        try
                        {
                            // Filter types efficiently: only those with DiscriminatorValueAttribute and matching base
                            var assemblyTypes = assembly.GetTypes();
                            foreach (var tp in assemblyTypes)
                            {
                                if (tp.BaseType == baseType && tp.GetCustomAttribute<DiscriminatorValueAttribute>() != null)
                                {
                                    types.Add(tp);
                                }
                            }
                        }
                        catch (ReflectionTypeLoadException)
                        {
                            // Skip assemblies that can't be fully loaded (e.g., missing dependencies)
                            // This is safer than crashing on dynamic assemblies or partially loaded modules
                        }
                    }
                    return types;
                });

                foreach (var dt in derivedTypes)
                {
                    var value = dt.GetCustomAttribute<DiscriminatorValueAttribute>()!.Value;
                    var map = ctx.GetMapping(dt);
                    TphMappings[value] = map;
                    foreach (var dc in map.Columns)
                    {
                        if (!cols.Any(c => string.Equals(c.Prop.Name, dc.Prop.Name, StringComparison.Ordinal)))
                            cols.Add(dc);
                    }
                }
            }

            Columns = cols.ToArray();
            ColumnsByName = Columns.ToDictionary(c => c.Prop.Name, StringComparer.Ordinal);

            KeyColumns = Columns.Where(c => c.IsKey).ToArray();
            TimestampColumn = Columns.FirstOrDefault(c => c.IsTimestamp);
            TenantColumn = Columns.FirstOrDefault(c => c.PropName == ctx.Options.TenantColumnName);
            InsertColumns = Columns.Where(c => !c.IsDbGenerated).ToArray();
            UpdateColumns = Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToArray();

            // Compute converter fingerprint for materializer cache differentiation
            int fp = 0;
            foreach (var col in Columns)
                if (col.Converter != null) fp = HashCode.Combine(fp, col.Name, col.Converter.GetType());
            ConverterFingerprint = fp;

            // Compute shadow fingerprint so that different shadow-property configurations for
            // the same entity type produce distinct materializer cache entries.
            int sfp = Columns.Length; // include total column count
            foreach (var col in Columns)
                if (col.IsShadow) sfp = HashCode.Combine(sfp, col.Name);
            ShadowFingerprint = sfp;

            DiscoverRelations(ctx);
            BuildOwnedCollections(fluentConfig, p);
            BuildManyToManyJoins(fluentConfig, p, ctx);
        }

        /// <summary>
        /// Inspects the entity type and associated configuration to build the collection of
        /// relationships that describe how this entity links to dependents. Both explicitly
        /// configured relationships and convention-based matches are considered.
        /// </summary>
        /// <param name="ctx">The <see cref="DbContext"/> used to resolve related entity mappings.</param>
        private void DiscoverRelations(DbContext ctx)
        {
            if (_fluentConfig?.Relationships.Count > 0)
            {
                foreach (var rel in _fluentConfig.Relationships)
                {
                    var dependentMap = ctx.GetMapping(rel.DependentType);
                    Column principalKey;
                    if (rel.PrincipalKey != null)
                    {
                        principalKey = Columns.FirstOrDefault(c => c.Prop == rel.PrincipalKey)
                            ?? throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration, $"Principal key '{rel.PrincipalKey.Name}' not found on entity {Type.Name}"));
                    }
                    else if (KeyColumns.Length == 1)
                    {
                        principalKey = KeyColumns[0];
                    }
                    else
                    {
                        throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration, $"Principal key must be specified for relationship '{rel.PrincipalNavigation.Name}' on entity {Type.Name}"));
                    }
                    var foreignKey = dependentMap.Columns.FirstOrDefault(c => c.Prop == rel.ForeignKey)
                        ?? throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration, $"Foreign key '{rel.ForeignKey.Name}' not found on entity {dependentMap.Type.Name}"));
                    Relations[rel.PrincipalNavigation.Name] = new Relation(rel.PrincipalNavigation, rel.DependentType, principalKey, foreignKey, rel.CascadeDelete);
                }
            }

            foreach (var prop in Type.GetProperties().Where(pr => pr.GetCustomAttribute<NotMappedAttribute>() == null))
            {
                if (Relations.ContainsKey(prop.Name))
                    continue;
                if (typeof(IEnumerable).IsAssignableFrom(prop.PropertyType) && prop.PropertyType.IsGenericType)
                {
                    var dependentType = prop.PropertyType.GetGenericArguments()[0];
                    var dependentMap = ctx.GetMapping(dependentType);

                    Column? foreignKeyProp = dependentMap.Columns
                        .FirstOrDefault(c => string.Equals(c.ForeignKeyPrincipalTypeName, Type.Name, StringComparison.OrdinalIgnoreCase));

                    // Determine the principal key column: explicit key or convention-based "Id"
                    var principalKey = KeyColumns.Length == 1 ? KeyColumns[0]
                        : Columns.FirstOrDefault(c => string.Equals(c.PropName, "Id", StringComparison.OrdinalIgnoreCase));

                    if (foreignKeyProp == null && principalKey != null)
                    {
                        var fkName = $"{Type.Name}Id";
                        var fkComposite = $"{Type.Name}_{principalKey.PropName}";
                        foreignKeyProp = dependentMap.Columns.FirstOrDefault(c =>
                            string.Equals(c.PropName, fkName, StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(c.PropName, fkComposite, StringComparison.OrdinalIgnoreCase));
                    }

                    if (foreignKeyProp != null && principalKey != null)
                    {
                        Relations[prop.Name] = new Relation(prop, dependentType, principalKey, foreignKeyProp);
                    }
                }
            }
        }

        /// <summary>
        /// Sets the value of the primary key on the provided entity for key columns that are
        /// generated by the database (e.g., identity columns).
        /// </summary>
        /// <param name="entity">The entity instance whose key should be set.</param>
        /// <param name="value">The key value produced by the database.</param>
        public void SetPrimaryKey(object entity, object value)
        {
            if (entity is null) throw new ArgumentNullException(nameof(entity));
            if (value is null) throw new ArgumentNullException(nameof(value));

            var keyCol = KeyColumns.FirstOrDefault(k => k.IsDbGenerated);
            if (keyCol != null)
            {
                // Use the underlying type for nullable value types (e.g., int?, long?) because
                // Convert.ChangeType does not support Nullable<T> as a target type and would throw
                // InvalidCastException. The setter's compiled lambda handles the implicit boxing.
                var targetType = Nullable.GetUnderlyingType(keyCol.Prop.PropertyType) ?? keyCol.Prop.PropertyType;
                var convertedValue = Convert.ChangeType(value, targetType);
                keyCol.Setter(entity, convertedValue);
            }
        }

        private void BuildOwnedCollections(IEntityTypeConfiguration? fluentConfig, DatabaseProvider p)
        {
            if (fluentConfig?.OwnedCollectionNavigations == null) return;
            foreach (var kvp in fluentConfig.OwnedCollectionNavigations)
            {
                var navProp = kvp.Key;
                var nav = kvp.Value;

                var ownedType = nav.OwnedType;

                // Build columns for the owned type (excluding the FK column itself)
                var ownedCols = ColumnMappingCache.GetCachedColumns(ownedType, p, nav.Configuration);
                var keyColumns = ownedCols.Where(c => c.IsKey).ToArray();

                OwnedCollections.Add(new OwnedCollectionMapping(
                    navProp, ownedType, nav.TableName, nav.ForeignKeyName, ownedCols, keyColumns, p));
            }
        }

        private void BuildManyToManyJoins(IEntityTypeConfiguration? fluentConfig, DatabaseProvider p, DbContext ctx)
        {
            if (fluentConfig?.ManyToManyRelationships == null || fluentConfig.ManyToManyRelationships.Count == 0)
                return;

            foreach (var m2m in fluentConfig.ManyToManyRelationships)
            {
                // Resolve the left PK column (this entity)
                if (KeyColumns.Length == 0)
                    continue;
                var leftPkCol = KeyColumns[0];

                // Resolve the right entity mapping and PK column
                var rightMapping = ctx.GetMapping(m2m.RelatedType);
                if (rightMapping.KeyColumns.Length == 0)
                    continue;
                var rightPkCol = rightMapping.KeyColumns[0];

                // Resolve nav properties
                var leftNavProp = Type.GetProperty(m2m.NavPropertyName)
                    ?? throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration,
                        $"Navigation property '{m2m.NavPropertyName}' not found on type '{Type.Name}'"));

                System.Reflection.PropertyInfo? rightNavProp = null;
                if (m2m.RelatedNavPropertyName != null)
                    rightNavProp = m2m.RelatedType.GetProperty(m2m.RelatedNavPropertyName);

                ManyToManyJoins.Add(new JoinTableMapping(
                    m2m.JoinTableName,
                    m2m.LeftFkColumn,
                    m2m.RightFkColumn,
                    Type,
                    m2m.RelatedType,
                    m2m.NavPropertyName,
                    m2m.RelatedNavPropertyName,
                    leftPkCol,
                    rightPkCol,
                    leftNavProp,
                    rightNavProp,
                    p));
            }
        }

        /// <summary>
        /// Represents the mapping of a relationship from the principal entity to a dependent entity type.
        /// </summary>
        /// <param name="NavProp">The navigation property on the principal entity that exposes the related data.</param>
        /// <param name="DependentType">The CLR type of the dependent entity.</param>
        /// <param name="PrincipalKey">The key column on the principal entity used as the relationship principal.</param>
        /// <param name="ForeignKey">The foreign key column on the dependent entity referencing the principal key.</param>
        /// <param name="CascadeDelete">Specifies whether deletes on the principal entity cascade to dependents.</param>
        public record Relation(PropertyInfo NavProp, Type DependentType, Column PrincipalKey, Column ForeignKey, bool CascadeDelete = true);
    }

    /// <summary>
    /// Describes an owned collection stored in a child table with a foreign key back to the owner.
    /// </summary>
    public sealed class OwnedCollectionMapping
    {
        /// <summary>The navigation property on the owner entity that holds the collection.</summary>
        public readonly PropertyInfo NavigationProperty;

        /// <summary>CLR element type of the owned collection items.</summary>
        public readonly Type OwnedType;

        /// <summary>Escaped name of the child table that stores owned items.</summary>
        public readonly string EscTable;

        /// <summary>Plain name of the child table.</summary>
        public readonly string TableName;

        /// <summary>Column name in the child table that holds the FK to the owner's PK.</summary>
        public readonly string ForeignKeyColumn;

        /// <summary>Escaped FK column.</summary>
        public readonly string EscForeignKeyColumn;

        /// <summary>All columns of the owned item (excluding the FK column itself).</summary>
        public readonly Column[] Columns;

        /// <summary>PK columns of the owned item (used for UPDATE/DELETE targeting).</summary>
        public readonly Column[] KeyColumns;

        /// <summary>Getter that reads the collection from an owner instance.</summary>
        public readonly Func<object, object?> CollectionGetter;

        /// <summary>Setter that assigns the collection on an owner instance.</summary>
        public readonly Action<object, object?> CollectionSetter;

        internal OwnedCollectionMapping(
            PropertyInfo navProp,
            Type ownedType,
            string tableName,
            string foreignKeyColumn,
            Column[] columns,
            Column[] keyColumns,
            DatabaseProvider provider)
        {
            NavigationProperty = navProp;
            OwnedType = ownedType;
            TableName = tableName;
            EscTable = provider.Escape(tableName);
            ForeignKeyColumn = foreignKeyColumn;
            EscForeignKeyColumn = provider.Escape(foreignKeyColumn);
            Columns = columns;
            KeyColumns = keyColumns;

            var entityParam = Expression.Parameter(typeof(object), "e");
            var cast = Expression.Convert(entityParam, navProp.DeclaringType!);
            var getProp = Expression.Property(cast, navProp);
            CollectionGetter = Expression.Lambda<Func<object, object?>>(
                Expression.Convert(getProp, typeof(object)), entityParam).Compile();

            var valueParam = Expression.Parameter(typeof(object), "v");
            var castV = Expression.Convert(valueParam, navProp.PropertyType);
            var setProp = Expression.Call(cast, navProp.GetSetMethod()!, castV);
            CollectionSetter = Expression.Lambda<Action<object, object?>>(
                setProp, entityParam, valueParam).Compile();
        }
    }
}