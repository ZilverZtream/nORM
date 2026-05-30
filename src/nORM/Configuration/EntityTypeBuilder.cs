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
    /// <summary>
    /// Provides a fluent API for configuring how a given entity type is mapped
    /// to database structures such as tables, columns and relationships.
    /// </summary>
    /// <typeparam name="TEntity">The CLR type being configured.</typeparam>
    public class EntityTypeBuilder<TEntity> where TEntity : class
    {
        internal class MappingConfiguration : IEntityTypeConfiguration
        {
            public string? TableName { get; private set; }
            public bool IsReadOnly { get; private set; }
            public List<PropertyInfo> KeyProperties { get; } = new();
            public Dictionary<PropertyInfo, string> ColumnNames { get; } = new();
            public Dictionary<PropertyInfo, string> DefaultValues { get; } = new();
            public Dictionary<PropertyInfo, IdentityOptionsConfiguration> IdentityOptionValues { get; } = new();
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
            bool IEntityTypeConfiguration.IsReadOnly => IsReadOnly;
            IReadOnlyDictionary<PropertyInfo, string> IEntityTypeConfiguration.ColumnNames => ColumnNames;
            IReadOnlyDictionary<PropertyInfo, string> IEntityTypeConfiguration.DefaultValueSql => DefaultValues;
            IReadOnlyDictionary<PropertyInfo, IdentityOptionsConfiguration> IEntityTypeConfiguration.IdentityOptions => IdentityOptionValues;
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
            /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
            public void SetTableName(string name)
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentException("Table name cannot be null or whitespace.", nameof(name));
                TableName = name;
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
            /// Marks the entity as query-only so generated write operations are
            /// rejected before SQL is generated.
            /// </summary>
            public void SetReadOnly()
            {
                IsReadOnly = true;
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
            {
                ArgumentNullException.ThrowIfNull(prop);
                if (string.IsNullOrWhiteSpace(sql))
                    throw new ArgumentException("Default SQL cannot be null or whitespace.", nameof(sql));
                DefaultValues[prop] = sql.Trim();
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
            {
                if (string.IsNullOrWhiteSpace(name))
                    throw new ArgumentException("Expression index name cannot be null or whitespace.", nameof(name));
                if (string.IsNullOrWhiteSpace(expressionSql))
                    throw new ArgumentException("Expression index SQL cannot be null or whitespace.", nameof(expressionSql));
                ExpressionIndexList.RemoveAll(i => string.Equals(i.Name, name.Trim(), StringComparison.OrdinalIgnoreCase));
                ExpressionIndexList.Add(new ExpressionIndexConfiguration(name.Trim(), expressionSql.Trim(), isUnique, string.IsNullOrWhiteSpace(filterSql) ? null : filterSql.Trim()));
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

        private readonly MappingConfiguration _config = new();
        internal IEntityTypeConfiguration Configuration => _config;

        /// <summary>
        /// Specifies the database table that <typeparamref name="TEntity"/> maps to.
        /// </summary>
        /// <param name="name">The unqualified table name.</param>
        /// <returns>The same <see cref="EntityTypeBuilder{TEntity}"/> instance for chaining.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
        public EntityTypeBuilder<TEntity> ToTable(string name)
        {
            _config.SetTableName(name);
            return this;
        }

        /// <summary>
        /// Maps <typeparamref name="TEntity"/> to the same table as another principal entity type.
        /// </summary>
        /// <typeparam name="TPrincipal">The principal entity that owns the table.</typeparam>
        /// <returns>The same builder instance for fluent chaining.</returns>
        public EntityTypeBuilder<TEntity> SharesTableWith<TPrincipal>()
        {
            _config.SetTableSplit(typeof(TPrincipal));
            return this;
        }

        /// <summary>
        /// Configures one or more properties to constitute the entity's primary key.
        /// </summary>
        /// <param name="keyExpression">Expression selecting the key property or properties.</param>
        /// <returns>The same builder instance for fluent chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="keyExpression"/> is null.</exception>
        /// <exception cref="ArgumentException">
        /// Thrown when the expression selects no properties, or contains a non-property argument.
        /// </exception>
        public EntityTypeBuilder<TEntity> HasKey(Expression<Func<TEntity, object>> keyExpression)
        {
            ArgumentNullException.ThrowIfNull(keyExpression);
            if (keyExpression.Body is NewExpression ne)
            {
                if (ne.Arguments.Count == 0)
                    throw new ArgumentException(
                        "HasKey expression must select at least one property. " +
                        "An empty anonymous object 'e => new { }' is not a valid key selector.",
                        nameof(keyExpression));
                foreach (var arg in ne.Arguments)
                {
                    PropertyInfo prop;
                    try { prop = GetProperty(arg); }
                    catch (ArgumentException inner)
                    {
                        throw new ArgumentException(
                            $"HasKey expression contains an invalid argument: {inner.Message}",
                            nameof(keyExpression), inner);
                    }
                    _config.AddKey(prop);
                }
            }
            else
            {
                var prop = GetProperty(keyExpression.Body);
                _config.AddKey(prop);
            }
            return this;
        }

        /// <summary>
        /// Marks this entity as query-only. nORM can materialize it from queries,
        /// but insert, update, delete, and tracked SaveChanges writes are rejected.
        /// </summary>
        public EntityTypeBuilder<TEntity> IsReadOnly()
        {
            _config.SetReadOnly();
            return this;
        }

        /// <summary>
        /// Configures a table-level CHECK constraint for migration snapshot generation.
        /// The SQL predicate is provider SQL and should not include the outer CHECK keyword.
        /// </summary>
        /// <param name="name">Database constraint name.</param>
        /// <param name="sql">Provider SQL predicate inside the CHECK clause.</param>
        /// <returns>The same builder instance for chaining.</returns>
        /// <exception cref="ArgumentException">Thrown when either argument is null or whitespace.</exception>
        public EntityTypeBuilder<TEntity> HasCheckConstraint(string name, string sql)
        {
            _config.AddCheckConstraint(name, sql);
            return this;
        }

        /// <summary>
        /// Configures a provider-specific index over a SQL expression rather than a mapped property.
        /// </summary>
        /// <param name="name">Database index name.</param>
        /// <param name="expressionSql">Provider SQL expression used as the index key.</param>
        /// <param name="isUnique">Whether the index enforces uniqueness.</param>
        /// <param name="filterSql">Optional provider SQL predicate for a filtered/partial expression index.</param>
        public EntityTypeBuilder<TEntity> HasExpressionIndex(string name, string expressionSql, bool isUnique = false, string? filterSql = null)
        {
            _config.AddExpressionIndex(name, expressionSql, isUnique, filterSql);
            return this;
        }

        /// <summary>
        /// Begins configuration for the specified property.
        /// </summary>
        /// <param name="propertyExpression">Expression selecting the property to configure.</param>
        /// <returns>A <see cref="PropertyBuilder"/> for further configuration of the property.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="propertyExpression"/> is null.</exception>
        public PropertyBuilder Property(Expression<Func<TEntity, object>> propertyExpression)
        {
            ArgumentNullException.ThrowIfNull(propertyExpression);
            var prop = GetProperty(propertyExpression);
            return new PropertyBuilder(this, prop);
        }

        /// <summary>
        /// Begins typed configuration for the specified property.
        /// </summary>
        /// <typeparam name="TProperty">The CLR type of the property.</typeparam>
        /// <param name="propertyExpression">Expression selecting the property to configure.</param>
        /// <returns>A <see cref="PropertyBuilder{TProperty}"/> for further configuration including value converters.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="propertyExpression"/> is null.</exception>
        public PropertyBuilder<TProperty> Property<TProperty>(Expression<Func<TEntity, TProperty>> propertyExpression)
        {
            ArgumentNullException.ThrowIfNull(propertyExpression);
            var prop = GetProperty(propertyExpression);
            return new PropertyBuilder<TProperty>(this, prop);
        }

        /// <summary>
        /// Creates a shadow property on the entity type configuration.
        /// </summary>
        /// <typeparam name="TProperty">CLR type of the shadow property.</typeparam>
        /// <param name="name">The name of the shadow property.</param>
        /// <returns>A <see cref="ShadowPropertyBuilder"/> for further configuration.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
        public ShadowPropertyBuilder Property<TProperty>(string name)
        {
            // Validation delegated to AddShadowProperty
            _config.AddShadowProperty(name, typeof(TProperty));
            return new ShadowPropertyBuilder(this, name);
        }

        /// <summary>
        /// Configures a reference navigation property as an owned entity type.
        /// Owned types share the same table as the owner.
        /// </summary>
        /// <typeparam name="TOwned">The CLR type of the owned entity.</typeparam>
        /// <param name="navigation">Expression selecting the navigation property.</param>
        /// <param name="buildAction">Optional configuration for the owned entity type.</param>
        /// <returns>The same builder instance.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="navigation"/> is null.</exception>
        public EntityTypeBuilder<TEntity> OwnsOne<TOwned>(Expression<Func<TEntity, TOwned>> navigation, Action<EntityTypeBuilder<TOwned>>? buildAction = null) where TOwned : class
        {
            ArgumentNullException.ThrowIfNull(navigation);
            var prop = GetProperty(navigation);
            var builder = new EntityTypeBuilder<TOwned>();
            buildAction?.Invoke(builder);
            _config.AddOwned(prop, builder.Configuration);
            return this;
        }

        /// <summary>
        /// Configures a collection navigation property as an owned collection stored in a separate child table.
        /// </summary>
        /// <typeparam name="TOwned">CLR element type of the owned collection.</typeparam>
        /// <param name="navigation">Expression selecting the collection navigation property.</param>
        /// <param name="tableName">
        /// Name of the child table. Defaults to the owned type name.
        /// Must be supplied explicitly when <typeparamref name="TOwned"/> is a generic type.
        /// </param>
        /// <param name="foreignKey">
        /// FK column name referencing owner's PK. Defaults to owner type name + "Id".
        /// Must be supplied explicitly when <typeparamref name="TEntity"/> is a generic type.
        /// </param>
        /// <param name="buildAction">Optional configuration for the owned element type.</param>
        /// <returns>The same builder instance for chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="navigation"/> is null.</exception>
        /// <exception cref="ArgumentException">
        /// Thrown when <paramref name="tableName"/> is omitted and the owned type is generic,
        /// or when <paramref name="foreignKey"/> is omitted and the owning type is generic.
        /// </exception>
        public EntityTypeBuilder<TEntity> OwnsMany<TOwned>(
            Expression<Func<TEntity, IEnumerable<TOwned>>> navigation,
            string? tableName = null,
            string? foreignKey = null,
            Action<EntityTypeBuilder<TOwned>>? buildAction = null) where TOwned : class
        {
            ArgumentNullException.ThrowIfNull(navigation);
            var prop = GetProperty(navigation);
            var ownedBuilder = new EntityTypeBuilder<TOwned>();
            buildAction?.Invoke(ownedBuilder);

            if (tableName == null && typeof(TOwned).Name.Contains('`'))
                throw new ArgumentException(
                    $"The owned type '{typeof(TOwned).Name}' is generic. " +
                    "Provide an explicit tableName when calling OwnsMany on a generic type.",
                    nameof(tableName));
            if (foreignKey == null && typeof(TEntity).Name.Contains('`'))
                throw new ArgumentException(
                    $"The owning type '{typeof(TEntity).Name}' is generic. " +
                    "Provide an explicit foreignKey when calling OwnsMany on a generic type.",
                    nameof(foreignKey));

            var childTable = tableName ?? typeof(TOwned).Name;
            var fkCol = foreignKey ?? typeof(TEntity).Name + "Id";
            _config.AddOwnedCollection(prop, new OwnedCollectionNavigation(typeof(TOwned), childTable, fkCol, ownedBuilder.Configuration));
            return this;
        }

        /// <summary>
        /// Begins configuration of a collection navigation property.
        /// </summary>
        /// <typeparam name="TProperty">The type of the dependent entity.</typeparam>
        /// <param name="navigation">Expression selecting the collection navigation.</param>
        /// <returns>A <see cref="CollectionNavigationBuilder{TProperty}"/> for relationship configuration.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="navigation"/> is null.</exception>
        public CollectionNavigationBuilder<TProperty> HasMany<TProperty>(Expression<Func<TEntity, IEnumerable<TProperty>>> navigation) where TProperty : class
        {
            ArgumentNullException.ThrowIfNull(navigation);
            var prop = GetProperty(navigation);
            return new CollectionNavigationBuilder<TProperty>(this, prop);
        }

        private PropertyInfo GetProperty(Expression expression)
        {
            if (expression is MemberExpression me && me.Member is PropertyInfo meProp)
            {
                if (me.Expression is not ParameterExpression)
                    throw new ArgumentException(
                        $"Expression must be a direct property access (e => e.Property). " +
                        $"Chained or nested access such as 'e => e.Navigation.Property' is not supported. " +
                        $"Received expression tree: {expression}",
                        nameof(expression));
                return meProp;
            }

            if (expression is UnaryExpression ue &&
                (ue.NodeType == System.Linq.Expressions.ExpressionType.Convert ||
                 ue.NodeType == System.Linq.Expressions.ExpressionType.ConvertChecked) &&
                ue.Operand is MemberExpression ume && ume.Member is PropertyInfo umeProp)
            {
                if (ume.Expression is not ParameterExpression)
                    throw new ArgumentException(
                        $"Expression must be a direct property access (e => e.Property). " +
                        $"Chained access is not supported. Received expression tree: {expression}",
                        nameof(expression));
                return umeProp;
            }

            throw new ArgumentException(
                "Expression must select a single property (not a field, method call, or complex expression). " +
                "Received: " + expression.NodeType,
                nameof(expression));
        }

        private PropertyInfo GetProperty(LambdaExpression expression)
        {
            if (expression is null) throw new ArgumentNullException(nameof(expression));
            return GetProperty(expression.Body);
        }

        private IReadOnlyList<PropertyInfo> GetProperties(LambdaExpression expression)
        {
            if (expression is null) throw new ArgumentNullException(nameof(expression));

            static Expression UnwrapConvert(Expression e)
                => e is UnaryExpression ue &&
                   (ue.NodeType == System.Linq.Expressions.ExpressionType.Convert ||
                    ue.NodeType == System.Linq.Expressions.ExpressionType.ConvertChecked)
                    ? ue.Operand
                    : e;

            var body = UnwrapConvert(expression.Body);
            if (body is NewExpression ne)
            {
                if (ne.Arguments.Count == 0)
                    throw new ArgumentException("Composite key expression must select at least one property.", nameof(expression));

                var props = new PropertyInfo[ne.Arguments.Count];
                for (var i = 0; i < ne.Arguments.Count; i++)
                    props[i] = GetProperty(UnwrapConvert(ne.Arguments[i]));
                return props;
            }

            return new[] { GetProperty(body) };
        }

        /// <summary>
        /// Provides configuration options for a specific property on the entity type.
        /// </summary>
        public class PropertyBuilder
        {
            /// <summary>The parent entity type builder that owns this property builder.</summary>
            protected readonly EntityTypeBuilder<TEntity> _parent;
            /// <summary>Reflection metadata for the property being configured.</summary>
            protected readonly PropertyInfo _property;

            internal PropertyBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo property)
            {
                _parent = parent;
                _property = property;
            }

            /// <summary>
            /// Sets the database column name for the configured property.
            /// </summary>
            /// <param name="name">The column name to map the property to.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
            public PropertyBuilder HasColumnName(string name)
            {
                // Validation delegated to SetColumnName
                _parent._config.SetColumnName(_property, name);
                return this;
            }

            /// <summary>
            /// Configures SQL default metadata for this property for migration snapshot
            /// generation. This does not mark the property as database-generated and does
            /// not cause nORM to omit the column from INSERT statements.
            /// </summary>
            /// <param name="sql">A SQL literal or no-argument function accepted by migration default validation.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="sql"/> is null or whitespace.</exception>
            public PropertyBuilder HasDefaultValueSql(string sql)
            {
                _parent._config.SetDefaultValueSql(_property, sql);
                return this;
            }

            /// <summary>
            /// Configures provider identity seed/increment metadata for migration
            /// generation. Providers without matching identity DDL may ignore or reject
            /// the metadata during migration SQL generation.
            /// </summary>
            public PropertyBuilder HasIdentityOptions(long seed, long increment)
            {
                _parent._config.SetIdentityOptions(_property, seed, increment);
                return this;
            }

            /// <summary>
            /// Configures the database collation for this property in migration snapshots.
            /// The value is a provider collation identifier such as <c>NOCASE</c>,
            /// <c>Latin1_General_100_CI_AS</c>, or <c>utf8mb4_0900_ai_ci</c>.
            /// </summary>
            /// <param name="collation">Provider collation identifier.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="collation"/> is null or whitespace.</exception>
            public PropertyBuilder HasCollation(string collation)
            {
                _parent._config.SetCollation(_property, collation);
                return this;
            }

            /// <summary>
            /// Configures this property as a database-computed/generated column for
            /// migration snapshot generation. The SQL expression is provider SQL and
            /// should not include the outer generated-column syntax.
            /// </summary>
            /// <param name="sql">Provider SQL expression used to compute the column.</param>
            /// <param name="stored">Whether the generated value should be physically stored when the provider supports that choice.</param>
            /// <returns>This <see cref="PropertyBuilder"/> instance for further chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="sql"/> is null or whitespace.</exception>
            public PropertyBuilder HasComputedColumnSql(string sql, bool stored = false)
            {
                _parent._config.SetComputedColumnSql(_property, sql, stored);
                return this;
            }

            /// <summary>
            /// Configures a value converter for this property using the untyped interface.
            /// </summary>
            /// <param name="converter">The value converter to apply.</param>
            /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
            /// <exception cref="ArgumentNullException">Thrown when <paramref name="converter"/> is null.</exception>
            public EntityTypeBuilder<TEntity> HasConversion(IValueConverter converter)
            {
                ArgumentNullException.ThrowIfNull(converter);
                _parent._config.AddConverter(_property, converter);
                return _parent;
            }

            /// <summary>
            /// Returns the parent <see cref="EntityTypeBuilder{TEntity}"/> to resume entity-level chaining.
            /// </summary>
            public EntityTypeBuilder<TEntity> Builder => _parent;
        }

        /// <summary>
        /// Strongly-typed property builder that enables type-safe value converter configuration.
        /// </summary>
        /// <typeparam name="TProperty">The CLR type of the property being configured.</typeparam>
        public class PropertyBuilder<TProperty> : PropertyBuilder
        {
            internal PropertyBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo property)
                : base(parent, property) { }

            /// <summary>
            /// Configures a strongly-typed value converter for this property.
            /// </summary>
            /// <typeparam name="TProvider">The provider/database storage type.</typeparam>
            /// <param name="converter">The strongly-typed value converter.</param>
            /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
            public EntityTypeBuilder<TEntity> HasConversion<TProvider>(ValueConverter<TProperty, TProvider> converter)
                => HasConversion((IValueConverter)converter);
        }

        /// <summary>
        /// Configures a shadow property that exists only in the model and database.
        /// </summary>
        public class ShadowPropertyBuilder
        {
            private readonly EntityTypeBuilder<TEntity> _parent;
            private readonly string _name;

            internal ShadowPropertyBuilder(EntityTypeBuilder<TEntity> parent, string name)
            {
                _parent = parent;
                _name = name;
            }

            /// <summary>
            /// Assigns a column name for the shadow property.
            /// </summary>
            /// <param name="columnName">The column name to use in the database.</param>
            /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="columnName"/> is null or whitespace.</exception>
            public EntityTypeBuilder<TEntity> HasColumnName(string columnName)
            {
                // Validation delegated to SetShadowColumnName
                _parent._config.SetShadowColumnName(_name, columnName);
                return _parent;
            }
        }

        /// <summary>
        /// Supports configuration of collection navigations to dependent entity types.
        /// </summary>
        /// <typeparam name="TDependent">The CLR type of the dependent entity.</typeparam>
        public class CollectionNavigationBuilder<TDependent> where TDependent : class
        {
            private readonly EntityTypeBuilder<TEntity> _parent;
            private readonly PropertyInfo _principalNavigation;

            internal CollectionNavigationBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo principalNavigation)
            {
                _parent = parent;
                _principalNavigation = principalNavigation;
            }

            /// <summary>
            /// Configures the relationship to include an optional reference navigation
            /// from the dependent entity back to the principal.
            /// </summary>
            /// <param name="navigation">Expression selecting the dependent's navigation property, if any.</param>
            /// <returns>A builder for configuring the relationship further.</returns>
            public ReferenceCollectionBuilder WithOne(Expression<Func<TDependent, TEntity?>>? navigation = null)
            {
                PropertyInfo? dependentNav = null;
                if (navigation != null)
                    dependentNav = _parent.GetProperty(navigation);
                return new ReferenceCollectionBuilder(_parent, _principalNavigation, dependentNav);
            }

            /// <summary>
            /// Begins configuration of a many-to-many relationship with an optional inverse navigation.
            /// </summary>
            /// <param name="inverseNavigation">Expression selecting the inverse collection property on <typeparamref name="TDependent"/>, if any.</param>
            /// <returns>A builder for specifying the join table details.</returns>
            public ManyToManyBuilder WithMany(Expression<Func<TDependent, IEnumerable<TEntity>>>? inverseNavigation = null)
            {
                string? inverseNavName = null;
                if (inverseNavigation != null)
                    inverseNavName = _parent.GetProperty(inverseNavigation).Name;
                return new ManyToManyBuilder(_parent, _principalNavigation, inverseNavName);
            }

            /// <summary>
            /// Supports fluent specification of join table details for a many-to-many relationship.
            /// </summary>
            public class ManyToManyBuilder
            {
                private readonly EntityTypeBuilder<TEntity> _parent;
                private readonly PropertyInfo _principalNavigation;
                private readonly string? _inverseNavName;

                internal ManyToManyBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo principalNavigation, string? inverseNavName)
                {
                    _parent = parent;
                    _principalNavigation = principalNavigation;
                    _inverseNavName = inverseNavName;
                }

                /// <summary>
                /// Specifies the join table, left FK column (this entity's PK) and right FK column (related entity's PK).
                /// </summary>
                /// <param name="joinTable">Name of the join table.</param>
                /// <param name="leftFk">Column referencing this entity's PK.</param>
                /// <param name="rightFk">Column referencing the related entity's PK.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                /// <exception cref="ArgumentException">
                /// Thrown when any parameter is null or whitespace, or when <paramref name="leftFk"/>
                /// and <paramref name="rightFk"/> are the same column name.
                /// </exception>
                public EntityTypeBuilder<TEntity> UsingTable(string joinTable, string leftFk, string rightFk)
                    => UsingTable(joinTable, leftFk, rightFk, schema: null);

                /// <summary>
                /// Specifies a join table with ordered composite FK columns for this entity and the related entity.
                /// </summary>
                /// <param name="joinTable">Name of the join table.</param>
                /// <param name="leftFkColumns">Ordered columns referencing this entity's key.</param>
                /// <param name="rightFkColumns">Ordered columns referencing the related entity's key.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                public EntityTypeBuilder<TEntity> UsingTable(string joinTable, IReadOnlyList<string> leftFkColumns, IReadOnlyList<string> rightFkColumns)
                    => UsingTable(joinTable, leftFkColumns, rightFkColumns, schema: null);

                /// <summary>
                /// Specifies the schema-qualified join table, left FK column (this entity's PK) and right FK column (related entity's PK).
                /// </summary>
                /// <param name="joinTable">Name of the join table without schema qualification.</param>
                /// <param name="leftFk">Column referencing this entity's PK.</param>
                /// <param name="rightFk">Column referencing the related entity's PK.</param>
                /// <param name="schema">Optional schema containing the join table.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                /// <exception cref="ArgumentException">
                /// Thrown when any required parameter is null or whitespace, when <paramref name="schema"/>
                /// is whitespace, or when <paramref name="leftFk"/> and <paramref name="rightFk"/> are the same column name.
                /// </exception>
                public EntityTypeBuilder<TEntity> UsingTable(string joinTable, string leftFk, string rightFk, string? schema)
                {
                    if (string.IsNullOrWhiteSpace(joinTable))
                        throw new ArgumentException("Join table name cannot be null or whitespace.", nameof(joinTable));
                    if (string.IsNullOrWhiteSpace(leftFk))
                        throw new ArgumentException("Left FK column name cannot be null or whitespace.", nameof(leftFk));
                    if (string.IsNullOrWhiteSpace(rightFk))
                        throw new ArgumentException("Right FK column name cannot be null or whitespace.", nameof(rightFk));
                    if (schema is not null && string.IsNullOrWhiteSpace(schema))
                        throw new ArgumentException("Join table schema cannot be whitespace.", nameof(schema));
                    if (string.Equals(leftFk, rightFk, StringComparison.OrdinalIgnoreCase))
                        throw new ArgumentException(
                            $"The left FK column and right FK column in a many-to-many join table must be different. " +
                            $"Both were specified as '{leftFk}'. Provide distinct column names.",
                            nameof(rightFk));
                    _parent._config.AddManyToMany(new ManyToManyConfiguration(
                        _principalNavigation.Name,
                        typeof(TDependent),
                        joinTable,
                        leftFk,
                        rightFk,
                        _inverseNavName)
                    {
                        JoinTableSchema = schema,
                        LeftFkColumns = new[] { leftFk },
                        RightFkColumns = new[] { rightFk }
                    });
                    return _parent;
                }

                /// <summary>
                /// Specifies a schema-qualified join table with ordered composite FK columns for both sides.
                /// </summary>
                /// <param name="joinTable">Name of the join table without schema qualification.</param>
                /// <param name="leftFkColumns">Ordered columns referencing this entity's key.</param>
                /// <param name="rightFkColumns">Ordered columns referencing the related entity's key.</param>
                /// <param name="schema">Optional schema containing the join table.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
                public EntityTypeBuilder<TEntity> UsingTable(string joinTable, IReadOnlyList<string> leftFkColumns, IReadOnlyList<string> rightFkColumns, string? schema)
                {
                    if (string.IsNullOrWhiteSpace(joinTable))
                        throw new ArgumentException("Join table name cannot be null or whitespace.", nameof(joinTable));
                    if (schema is not null && string.IsNullOrWhiteSpace(schema))
                        throw new ArgumentException("Join table schema cannot be whitespace.", nameof(schema));
                    ValidateCompositeFkColumns(leftFkColumns, nameof(leftFkColumns));
                    ValidateCompositeFkColumns(rightFkColumns, nameof(rightFkColumns));
                    _parent._config.AddManyToMany(new ManyToManyConfiguration(
                        _principalNavigation.Name,
                        typeof(TDependent),
                        joinTable,
                        leftFkColumns[0],
                        rightFkColumns[0],
                        _inverseNavName)
                    {
                        JoinTableSchema = schema,
                        LeftFkColumns = leftFkColumns.ToArray(),
                        RightFkColumns = rightFkColumns.ToArray()
                    });
                    return _parent;
                }

                private static void ValidateCompositeFkColumns(IReadOnlyList<string>? columns, string paramName)
                {
                    if (columns is null || columns.Count == 0)
                        throw new ArgumentException("At least one FK column must be supplied.", paramName);
                    if (columns.Any(string.IsNullOrWhiteSpace))
                        throw new ArgumentException("FK column names cannot be null or whitespace.", paramName);
                    if (columns.Distinct(StringComparer.OrdinalIgnoreCase).Count() != columns.Count)
                        throw new ArgumentException("FK column names must be unique within each side of a many-to-many join table.", paramName);
                }
            }

            /// <summary>
            /// Enables configuration of relationship details between the principal
            /// and dependent types for a collection navigation.
            /// </summary>
            public class ReferenceCollectionBuilder
            {
                private readonly EntityTypeBuilder<TEntity> _parent;
                private readonly PropertyInfo _principalNavigation;
                private readonly PropertyInfo? _dependentNavigation;

                internal ReferenceCollectionBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo principalNavigation, PropertyInfo? dependentNavigation)
                {
                    _parent = parent;
                    _principalNavigation = principalNavigation;
                    _dependentNavigation = dependentNavigation;
                }

                /// <summary>
                /// Defines the foreign key used by the dependent entity in this relationship.
                /// </summary>
                /// <param name="foreignKeyExpression">Expression selecting the foreign key property on the dependent.</param>
                /// <param name="principalKeyExpression">Optional expression selecting the referenced principal key property.</param>
                /// <param name="cascadeDelete">Whether nORM should cascade deletes through the tracked object graph.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for further configuration.</returns>
                /// <exception cref="ArgumentNullException">Thrown when <paramref name="foreignKeyExpression"/> is null.</exception>
                /// <exception cref="NormConfigurationException">Thrown when the principal key cannot be inferred.</exception>
                public EntityTypeBuilder<TEntity> HasForeignKey(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression = null,
                    bool cascadeDelete = true)
                {
                    var (principalKeys, fkProps) = ResolveForeignKeyProperties(foreignKeyExpression, principalKeyExpression);
                    _parent._config.AddRelationship(new RelationshipConfiguration(
                        _principalNavigation,
                        typeof(TDependent),
                        _dependentNavigation,
                        principalKeys,
                        fkProps,
                        cascadeDelete));
                    return _parent;
                }

                /// <summary>
                /// Defines the foreign key used by the dependent entity and the database
                /// referential actions emitted for migration snapshots and provider DDL.
                /// </summary>
                /// <param name="foreignKeyExpression">Expression selecting the foreign key property on the dependent.</param>
                /// <param name="principalKeyExpression">Optional expression selecting the referenced principal key property.</param>
                /// <param name="onDelete">Database action for principal deletes.</param>
                /// <param name="onUpdate">Database action for principal key updates.</param>
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for further configuration.</returns>
                /// <exception cref="ArgumentNullException">Thrown when <paramref name="foreignKeyExpression"/> is null.</exception>
                /// <exception cref="NormConfigurationException">Thrown when the principal key cannot be inferred.</exception>
                public EntityTypeBuilder<TEntity> HasForeignKey(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression,
                    ReferentialAction onDelete,
                    ReferentialAction onUpdate)
                {
                    var (principalKeys, fkProps) = ResolveForeignKeyProperties(foreignKeyExpression, principalKeyExpression);
                    _parent._config.AddRelationship(new RelationshipConfiguration(
                        _principalNavigation,
                        typeof(TDependent),
                        _dependentNavigation,
                        principalKeys,
                        fkProps,
                        onDelete,
                        onUpdate));
                    return _parent;
                }

                private (IReadOnlyList<PropertyInfo> PrincipalKeys, IReadOnlyList<PropertyInfo> ForeignKeys) ResolveForeignKeyProperties(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression)
                {
                    ArgumentNullException.ThrowIfNull(foreignKeyExpression);
                    var fkProps = _parent.GetProperties(foreignKeyExpression);
                    IReadOnlyList<PropertyInfo> principalKeys;
                    if (principalKeyExpression != null)
                    {
                        principalKeys = _parent.GetProperties(principalKeyExpression);
                    }
                    else if (_parent._config.KeyProperties.Count == fkProps.Count)
                    {
                        principalKeys = _parent._config.KeyProperties.ToArray();
                    }
                    else
                    {
                        throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration, $"Principal key must be specified for relationship '{_principalNavigation.Name}' on entity {typeof(TEntity).Name}"));
                    }

                    if (principalKeys.Count != fkProps.Count)
                        throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration,
                            $"Relationship '{_principalNavigation.Name}' on entity {typeof(TEntity).Name} has {principalKeys.Count} principal key properties but {fkProps.Count} foreign key properties."));

                    return (principalKeys, fkProps);
                }
            }
        }
    }
}
