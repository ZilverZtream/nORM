using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Core;
using nORM.Mapping;

#nullable enable

namespace nORM.Configuration
{
    public partial class EntityTypeBuilder<TEntity> where TEntity : class
    {
        internal class MappingConfiguration : IEntityTypeConfiguration
        {
            public string? TableName { get; private set; }
            public string? SchemaName { get; private set; }
            public bool IsReadOnly { get; private set; }
            public bool IsKeyless { get; private set; }
            public List<PropertyInfo> KeyProperties { get; } = new();
            public string? PrimaryKeyConstraintName { get; private set; }
            public Dictionary<PropertyInfo, string> ColumnNames { get; } = new();
            public Dictionary<PropertyInfo, string> DefaultValues { get; } = new();
            public Dictionary<PropertyInfo, string> DefaultValueConstraintNameValues { get; } = new();
            public Dictionary<PropertyInfo, IdentityOptionsConfiguration> IdentityOptionValues { get; } = new();
            public Dictionary<PropertyInfo, int> MaxLengthValues { get; } = new();
            public Dictionary<PropertyInfo, bool> RequiredValues { get; } = new();
            public Dictionary<PropertyInfo, System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption> ValueGeneratedValues { get; } = new();
            public HashSet<PropertyInfo> RowVersionValues { get; } = new();
            public Dictionary<PropertyInfo, string> ColumnTypeValues { get; } = new();
            public Dictionary<PropertyInfo, bool> UnicodeValues { get; } = new();
            public Dictionary<PropertyInfo, bool> FixedLengthValues { get; } = new();
            public Dictionary<PropertyInfo, PrecisionConfiguration> PrecisionValues { get; } = new();
            public Dictionary<PropertyInfo, string> CollationValues { get; } = new();
            public List<CheckConstraintConfiguration> CheckConstraintList { get; } = new();
            public Dictionary<PropertyInfo, ComputedColumnConfiguration> ComputedColumns { get; } = new();
            public List<ExpressionIndexConfiguration> ExpressionIndexList { get; } = new();
            public Type? TableSplitWith { get; private set; }
            public Dictionary<PropertyInfo, OwnedNavigation> OwnedNavigations { get; } = new();
            public Dictionary<string, ShadowPropertyConfiguration> ShadowProperties { get; } = new();
            public List<RelationshipConfiguration> Relationships { get; } = new();
            public List<ConverterConfiguration> ConverterList { get; } = new();

            // Explicit interface implementations for read-only surface of IEntityTypeConfiguration.
            IReadOnlyList<PropertyInfo> IEntityTypeConfiguration.KeyProperties => KeyProperties;
            string? IEntityTypeConfiguration.PrimaryKeyConstraintName => PrimaryKeyConstraintName;
            bool IEntityTypeConfiguration.IsReadOnly => IsReadOnly;
            bool IEntityTypeConfiguration.IsKeyless => IsKeyless;
            IReadOnlyDictionary<PropertyInfo, string> IEntityTypeConfiguration.ColumnNames => ColumnNames;
            IReadOnlyDictionary<PropertyInfo, string> IEntityTypeConfiguration.DefaultValueSql => DefaultValues;
            IReadOnlyDictionary<PropertyInfo, string> IEntityTypeConfiguration.DefaultValueConstraintNames => DefaultValueConstraintNameValues;
            IReadOnlyDictionary<PropertyInfo, IdentityOptionsConfiguration> IEntityTypeConfiguration.IdentityOptions => IdentityOptionValues;
            IReadOnlyDictionary<PropertyInfo, int> IEntityTypeConfiguration.MaxLengths => MaxLengthValues;
            IReadOnlyDictionary<PropertyInfo, bool> IEntityTypeConfiguration.RequiredSettings => RequiredValues;
            IReadOnlyDictionary<PropertyInfo, System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption> IEntityTypeConfiguration.ValueGeneratedSettings => ValueGeneratedValues;
            IReadOnlyCollection<PropertyInfo> IEntityTypeConfiguration.RowVersionSettings => RowVersionValues;
            IReadOnlyDictionary<PropertyInfo, string> IEntityTypeConfiguration.ColumnTypes => ColumnTypeValues;
            IReadOnlyDictionary<PropertyInfo, bool> IEntityTypeConfiguration.UnicodeSettings => UnicodeValues;
            IReadOnlyDictionary<PropertyInfo, bool> IEntityTypeConfiguration.FixedLengthSettings => FixedLengthValues;
            IReadOnlyDictionary<PropertyInfo, PrecisionConfiguration> IEntityTypeConfiguration.Precisions => PrecisionValues;
            IReadOnlyDictionary<PropertyInfo, string> IEntityTypeConfiguration.Collations => CollationValues;
            IReadOnlyList<CheckConstraintConfiguration> IEntityTypeConfiguration.CheckConstraints => CheckConstraintList;
            IReadOnlyDictionary<PropertyInfo, ComputedColumnConfiguration> IEntityTypeConfiguration.ComputedColumnSql => ComputedColumns;
            IReadOnlyList<ExpressionIndexConfiguration> IEntityTypeConfiguration.ExpressionIndexes => ExpressionIndexList;
            IReadOnlyDictionary<PropertyInfo, OwnedNavigation> IEntityTypeConfiguration.OwnedNavigations => OwnedNavigations;
            IReadOnlyDictionary<string, ShadowPropertyConfiguration> IEntityTypeConfiguration.ShadowProperties => ShadowProperties;
            IReadOnlyList<RelationshipConfiguration> IEntityTypeConfiguration.Relationships => Relationships;
            IReadOnlyList<ConverterConfiguration> IEntityTypeConfiguration.Converters => ConverterList;

            public Dictionary<PropertyInfo, OwnedCollectionNavigation> OwnedCollectionNavigations { get; } = new();
            IReadOnlyDictionary<PropertyInfo, OwnedCollectionNavigation> IEntityTypeConfiguration.OwnedCollectionNavigations => OwnedCollectionNavigations;

            public List<ManyToManyConfiguration> ManyToManyList { get; } = new();
            IReadOnlyList<ManyToManyConfiguration> IEntityTypeConfiguration.ManyToManyRelationships => ManyToManyList;

            /// <summary>
            /// Sets the database table name that <typeparamref name="TEntity"/> maps to.
            /// </summary>
            /// <param name="name">The unescaped table name.</param>
            /// <param name="schema">Optional unescaped schema containing the table.</param>
            /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
            public void SetTableName(string name, string? schema = null)
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentException("Table name cannot be null or whitespace.", nameof(name));
                if (schema is not null && string.IsNullOrWhiteSpace(schema))
                    throw new ArgumentException("Table schema cannot be whitespace.", nameof(schema));

                TableName = name;
                SchemaName = schema;
            }

            /// <summary>
            /// Registers a property as part of the entity's primary key.
            /// </summary>
            /// <param name="prop">The property to mark as a key member.</param>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="prop"/> is null.</exception>
            public void AddKey(PropertyInfo prop)
            {
                ArgumentNullException.ThrowIfNull(prop);
                if (!KeyProperties.Contains(prop))
                    KeyProperties.Add(prop);
            }

            /// <summary>
            /// Sets the database primary-key constraint name.
            /// </summary>
            /// <param name="constraintName">The primary-key constraint name.</param>
            public void SetPrimaryKeyConstraintName(string? constraintName)
            {
                PrimaryKeyConstraintName = NormalizeConstraintName(constraintName);
            }

            /// <summary>
            /// Marks the entity as query-only so generated write operations are
            /// rejected before SQL is generated.
            /// </summary>
            public void SetReadOnly()
            {
                IsReadOnly = true;
            }

            /// <summary>
            /// Marks the entity as keyless (a query type): it is never tracked and cannot be saved,
            /// suitable for database views and read models.
            /// </summary>
            public void SetKeyless()
            {
                IsKeyless = true;
            }

            /// <summary>
            /// Overrides the column name used for the specified property.
            /// </summary>
            /// <param name="prop">The property whose column name to configure.</param>
            /// <param name="name">The column name to map the property to.</param>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="prop"/> is null.</exception>
            /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
            public void SetColumnName(PropertyInfo prop, string name)
            {
                ArgumentNullException.ThrowIfNull(prop);
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentException("Column name cannot be null or whitespace.", nameof(name));
                ColumnNames[prop] = name;
            }

            /// <summary>
            /// Sets SQL default metadata for the specified property. The value is used by
            /// migration snapshots and generators; it does not mark the property as
            /// database-generated for runtime writes.
            /// </summary>
            /// <param name="prop">The property whose column default to configure.</param>
            /// <param name="sql">A SQL literal or no-argument function accepted by the migration default validator.</param>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="prop"/> is null.</exception>
            /// <exception cref="ArgumentException">Thrown when <paramref name="sql"/> is null or whitespace.</exception>
            public void SetDefaultValueSql(PropertyInfo prop, string sql)
                => SetDefaultValueSql(prop, sql, constraintName: null);

            /// <summary>
            /// Sets SQL default metadata and an optional provider default-constraint name for the specified property.
            /// </summary>
            public void SetDefaultValueSql(PropertyInfo prop, string sql, string? constraintName)
            {
                ArgumentNullException.ThrowIfNull(prop);
                if (string.IsNullOrWhiteSpace(sql))
                    throw new ArgumentException("Default SQL cannot be null or whitespace.", nameof(sql));
                DefaultValues[prop] = sql.Trim();
                if (string.IsNullOrWhiteSpace(constraintName))
                    DefaultValueConstraintNameValues.Remove(prop);
                else
                    DefaultValueConstraintNameValues[prop] = NormalizeConstraintName(constraintName)!;
            }

            /// <summary>
            /// Sets provider identity seed/increment metadata for the specified property.
            /// </summary>
            public void SetIdentityOptions(PropertyInfo prop, long seed, long increment)
            {
                ArgumentNullException.ThrowIfNull(prop);
                if (increment == 0)
                    throw new ArgumentException("Identity increment cannot be zero.", nameof(increment));
                IdentityOptionValues[prop] = new IdentityOptionsConfiguration(seed, increment);
            }

            /// <summary>
            /// Sets maximum length metadata for string or byte array columns in migration snapshots.
            /// </summary>
            public void SetMaxLength(PropertyInfo prop, int length)
            {
                ArgumentNullException.ThrowIfNull(prop);
                if (length <= 0)
                    throw new ArgumentOutOfRangeException(nameof(length), "Max length must be greater than zero.");

                var clrType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                if (clrType != typeof(string) && clrType != typeof(byte[]))
                    throw new ArgumentException("Max length can only be configured for string or byte[] properties.", nameof(prop));

                MaxLengthValues[prop] = length;
            }

            /// <summary>
            /// Sets whether the column is required (non-nullable) in migration snapshots. Overrides the
            /// CLR/attribute-derived nullability when the schema is generated (EnsureCreated / migrations).
            /// </summary>
            public void SetRequired(PropertyInfo prop, bool required)
            {
                ArgumentNullException.ThrowIfNull(prop);
                RequiredValues[prop] = required;
            }

            /// <summary>
            /// Sets the store value-generation strategy for a property (EF Core's ValueGeneratedOnAdd /
            /// Never / OnAddOrUpdate). Identity/Computed mark the column database-generated so it is omitted
            /// from INSERT (and UPDATE) statements; None clears the flag even if an attribute set it.
            /// </summary>
            public void SetValueGenerated(PropertyInfo prop, System.ComponentModel.DataAnnotations.Schema.DatabaseGeneratedOption option)
            {
                ArgumentNullException.ThrowIfNull(prop);
                ValueGeneratedValues[prop] = option;
            }

            /// <summary>
            /// Marks a property as the entity's row-version / optimistic-concurrency token (EF Core's
            /// IsRowVersion) — the fluent equivalent of [Timestamp]. nORM manages the token and adds it to the
            /// UPDATE concurrency check.
            /// </summary>
            public void SetRowVersion(PropertyInfo prop)
            {
                ArgumentNullException.ThrowIfNull(prop);
                RowVersionValues.Add(prop);
            }

            /// <summary>
            /// Sets an explicit provider store type for the column (EF Core's HasColumnType), emitted verbatim
            /// by the migration SQL generators instead of the CLR-derived type.
            /// </summary>
            public void SetColumnType(PropertyInfo prop, string columnType)
            {
                ArgumentNullException.ThrowIfNull(prop);
                if (string.IsNullOrWhiteSpace(columnType))
                    throw new ArgumentException("Column type cannot be null or whitespace.", nameof(columnType));
                ColumnTypeValues[prop] = columnType.Trim();
            }

            /// <summary>
            /// Sets Unicode storage metadata for the specified string column in migration snapshots.
            /// </summary>
            public void SetUnicode(PropertyInfo prop, bool unicode)
            {
                ArgumentNullException.ThrowIfNull(prop);
                var clrType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                if (clrType != typeof(string))
                    throw new ArgumentException("Unicode can only be configured for string properties.", nameof(prop));

                UnicodeValues[prop] = unicode;
            }

            /// <summary>
            /// Sets fixed-length storage metadata for string or byte array columns in migration snapshots.
            /// </summary>
            public void SetFixedLength(PropertyInfo prop, bool fixedLength)
            {
                ArgumentNullException.ThrowIfNull(prop);
                var clrType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                if (clrType != typeof(string) && clrType != typeof(byte[]))
                    throw new ArgumentException("Fixed length can only be configured for string or byte[] properties.", nameof(prop));

                FixedLengthValues[prop] = fixedLength;
            }

            /// <summary>
            /// Sets precision/scale metadata for decimal columns in migration snapshots.
            /// </summary>
            public void SetPrecision(PropertyInfo prop, int precision, int? scale)
            {
                ArgumentNullException.ThrowIfNull(prop);
                if (precision <= 0)
                    throw new ArgumentOutOfRangeException(nameof(precision), "Precision must be greater than zero.");
                if (scale is < 0)
                    throw new ArgumentOutOfRangeException(nameof(scale), "Scale cannot be negative.");
                if (scale > precision)
                    throw new ArgumentOutOfRangeException(nameof(scale), "Scale cannot be greater than precision.");

                var clrType = Nullable.GetUnderlyingType(prop.PropertyType) ?? prop.PropertyType;
                if (clrType != typeof(decimal))
                    throw new ArgumentException("Precision can only be configured for decimal properties.", nameof(prop));

                PrecisionValues[prop] = new PrecisionConfiguration(precision, scale);
            }

            /// <summary>
            /// Sets database collation metadata for the specified property.
            /// </summary>
            public void SetCollation(PropertyInfo prop, string collation)
            {
                ArgumentNullException.ThrowIfNull(prop);
                if (string.IsNullOrWhiteSpace(collation))
                    throw new ArgumentException("Collation cannot be null or whitespace.", nameof(collation));
                CollationValues[prop] = collation.Trim();
            }

            /// <summary>
            /// Adds a table-level CHECK constraint for migration snapshot generation.
            /// </summary>
            /// <param name="name">Database constraint name.</param>
            /// <param name="sql">Provider SQL predicate inside the CHECK clause.</param>
            /// <exception cref="ArgumentException">Thrown when either argument is null or whitespace.</exception>
            public void AddCheckConstraint(string name, string sql)
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentException("Check constraint name cannot be null or whitespace.", nameof(name));
                if (string.IsNullOrWhiteSpace(sql))
                    throw new ArgumentException("Check constraint SQL cannot be null or whitespace.", nameof(sql));
                CheckConstraintList.RemoveAll(c => string.Equals(c.Name, name.Trim(), StringComparison.OrdinalIgnoreCase));
                CheckConstraintList.Add(new CheckConstraintConfiguration(name.Trim(), sql.Trim()));
            }

            /// <summary>
            /// Sets database-computed/generated column metadata for the specified property.
            /// </summary>
            public void SetComputedColumnSql(PropertyInfo prop, string sql, bool stored)
            {
                ArgumentNullException.ThrowIfNull(prop);
                if (string.IsNullOrWhiteSpace(sql))
                    throw new ArgumentException("Computed column SQL cannot be null or whitespace.", nameof(sql));
                ComputedColumns[prop] = new ComputedColumnConfiguration(sql.Trim(), stored);
            }

            /// <summary>
            /// Adds a provider-specific index over a SQL expression.
            /// </summary>
            public void AddExpressionIndex(string name, string expressionSql, bool isUnique, string? filterSql)
                => AddExpressionIndex(
                    name,
                    expressionSql,
                    isUnique,
                    filterSql,
                    includedColumnNames: null,
                    nullsNotDistinct: false,
                    nullSortOrder: IndexNullSortOrder.Default);

            /// <summary>
            /// Adds a provider-specific index over a SQL expression.
            /// </summary>
            public void AddExpressionIndex(
                string name,
                string expressionSql,
                bool isUnique,
                string? filterSql,
                IReadOnlyList<string>? includedColumnNames,
                bool nullsNotDistinct,
                IndexNullSortOrder nullSortOrder)
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentException("Expression index name cannot be null or whitespace.", nameof(name));
                if (string.IsNullOrWhiteSpace(expressionSql))
                    throw new ArgumentException("Expression index SQL cannot be null or whitespace.", nameof(expressionSql));
                if (!Enum.IsDefined(nullSortOrder))
                    throw new ArgumentOutOfRangeException(nameof(nullSortOrder), $"Unsupported null sort order '{nullSortOrder}'.");

                var included = NormalizeExpressionIndexIncludedColumns(includedColumnNames);
                ExpressionIndexList.RemoveAll(i => string.Equals(i.Name, name.Trim(), StringComparison.OrdinalIgnoreCase));
                ExpressionIndexList.Add(new ExpressionIndexConfiguration(name.Trim(), expressionSql.Trim(), isUnique, string.IsNullOrWhiteSpace(filterSql) ? null : filterSql.Trim())
                {
                    IncludedColumnNames = included,
                    NullsNotDistinct = nullsNotDistinct,
                    NullSortOrder = nullSortOrder
                });
            }

            private static string[] NormalizeExpressionIndexIncludedColumns(IReadOnlyList<string>? includedColumnNames)
            {
                if (includedColumnNames is null || includedColumnNames.Count == 0)
                    return Array.Empty<string>();

                var normalized = includedColumnNames
                    .Select(static column =>
                    {
                        if (string.IsNullOrWhiteSpace(column))
                            throw new ArgumentException("Expression index included column names cannot be null or whitespace.", nameof(includedColumnNames));
                        return column.Trim();
                    })
                    .ToArray();
                if (normalized.Distinct(StringComparer.OrdinalIgnoreCase).Count() != normalized.Length)
                    throw new ArgumentException("Expression index included column names must be unique.", nameof(includedColumnNames));

                return normalized;
            }

            /// <summary>
            /// Configures this entity to share its database table with the specified
            /// principal entity type.
            /// </summary>
            /// <param name="principal">The CLR type of the principal entity that owns the table.</param>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="principal"/> is null.</exception>
            public void SetTableSplit(Type principal)
            {
                ArgumentNullException.ThrowIfNull(principal);
                TableSplitWith = principal;
            }

            /// <summary>
            /// Registers an owned navigation property for the entity type. Owned entities
            /// are mapped to the same table as the owning entity.
            /// </summary>
            /// <param name="prop">The navigation property representing the owned entity.</param>
            /// <param name="config">Optional configuration for the owned entity type.</param>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="prop"/> is null.</exception>
            public void AddOwned(PropertyInfo prop, IEntityTypeConfiguration? config)
            {
                ArgumentNullException.ThrowIfNull(prop);
                OwnedNavigations[prop] = new OwnedNavigation(prop.PropertyType, config);
            }

            /// <summary>
            /// Adds a shadow property -- a property not defined on the CLR type -- to the entity configuration.
            /// </summary>
            /// <param name="name">The name of the shadow property.</param>
            /// <param name="clrType">The CLR type the shadow property should expose.</param>
            /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="clrType"/> is null.</exception>
            public void AddShadowProperty(string name, Type clrType)
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentException("Shadow property name cannot be null or whitespace.", nameof(name));
                ArgumentNullException.ThrowIfNull(clrType);
                ShadowProperties[name] = new ShadowPropertyConfiguration(clrType);
            }

            /// <summary>
            /// Sets the database column name for a previously defined shadow property.
            /// </summary>
            /// <param name="name">The name of the shadow property.</param>
            /// <param name="column">The column name to map the property to.</param>
            /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> or <paramref name="column"/> is null or whitespace.</exception>
            /// <exception cref="NormConfigurationException">Thrown when no shadow property with the given <paramref name="name"/> has been registered.</exception>
            public void SetShadowColumnName(string name, string column)
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentException("Shadow property name cannot be null or whitespace.", nameof(name));
                if (string.IsNullOrWhiteSpace(column))
                    throw new ArgumentException("Column name cannot be null or whitespace.", nameof(column));
                if (!ShadowProperties.TryGetValue(name, out var sp))
                    throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration, $"Shadow property '{name}' has not been registered on entity '{typeof(TEntity).Name}'. Call Property<T>(\"{name}\") before setting a column name."));
                ShadowProperties[name] = sp with { ColumnName = column };
            }

            /// <summary>
            /// Adds a configured relationship to this entity type.
            /// </summary>
            /// <param name="relationship">The relationship configuration to register.</param>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="relationship"/> is null.</exception>
            public void AddRelationship(RelationshipConfiguration relationship)
            {
                ArgumentNullException.ThrowIfNull(relationship);
                Relationships.Add(relationship);
            }

            /// <summary>
            /// Registers a value converter for the specified property.
            /// </summary>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="prop"/> or <paramref name="converter"/> is null.</exception>
            public void AddConverter(PropertyInfo prop, IValueConverter converter)
            {
                ArgumentNullException.ThrowIfNull(prop);
                ArgumentNullException.ThrowIfNull(converter);
                ConverterList.Add(new ConverterConfiguration(prop, converter));
            }

            /// <summary>
            /// Registers an owned collection navigation property stored in a separate child table.
            /// </summary>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="prop"/> or <paramref name="nav"/> is null.</exception>
            public void AddOwnedCollection(PropertyInfo prop, OwnedCollectionNavigation nav)
            {
                ArgumentNullException.ThrowIfNull(prop);
                ArgumentNullException.ThrowIfNull(nav);
                OwnedCollectionNavigations[prop] = nav;
            }

            /// <summary>
            /// Registers a many-to-many relationship configuration.
            /// </summary>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="config"/> is null.</exception>
            public void AddManyToMany(ManyToManyConfiguration config)
            {
                ArgumentNullException.ThrowIfNull(config);
                ManyToManyList.Add(config);
            }
        }
    }
}
