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

        private readonly IEntityTypeConfiguration? _fluentConfig;

        // PERFORMANCE FIX: Cache derived type discovery to avoid expensive AppDomain.GetAssemblies() scan on every mapping
        // Scanning all assemblies and types can take 100-1000ms+ in large applications with many assemblies
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

                // PERFORMANCE FIX: Use cached derived type discovery instead of scanning all assemblies
                // This avoids 100-1000ms+ delay from AppDomain.GetAssemblies().SelectMany(a => a.GetTypes())
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

            DiscoverRelations(ctx);
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

                    if (foreignKeyProp == null && KeyColumns.Length == 1)
                    {
                        var fkName = $"{Type.Name}Id";
                        var fkComposite = $"{Type.Name}_{KeyColumns[0].PropName}";
                        foreignKeyProp = dependentMap.Columns.FirstOrDefault(c =>
                            string.Equals(c.PropName, fkName, StringComparison.OrdinalIgnoreCase) ||
                            string.Equals(c.PropName, fkComposite, StringComparison.OrdinalIgnoreCase));
                    }

                    if (foreignKeyProp != null && KeyColumns.Length == 1)
                    {
                        Relations[prop.Name] = new Relation(prop, dependentType, KeyColumns[0], foreignKeyProp);
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
            var keyCol = KeyColumns.FirstOrDefault(k => k.IsDbGenerated);
            if (keyCol != null)
            {
                var convertedValue = Convert.ChangeType(value, keyCol.Prop.PropertyType);
                keyCol.Setter(entity, convertedValue);
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
}