using System;
using System.Collections.Generic;
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
            public List<PropertyInfo> KeyProperties { get; } = new();
            public Dictionary<PropertyInfo, string> ColumnNames { get; } = new();
            public Type? TableSplitWith { get; private set; }
            public Dictionary<PropertyInfo, OwnedNavigation> OwnedNavigations { get; } = new();
            public Dictionary<string, ShadowPropertyConfiguration> ShadowProperties { get; } = new();
            public List<RelationshipConfiguration> Relationships { get; } = new();
            public List<ConverterConfiguration> ConverterList { get; } = new();
            IReadOnlyList<ConverterConfiguration> IEntityTypeConfiguration.Converters => ConverterList;

            public Dictionary<PropertyInfo, OwnedCollectionNavigation> OwnedCollectionNavigations { get; } = new();
            Dictionary<PropertyInfo, OwnedCollectionNavigation> IEntityTypeConfiguration.OwnedCollectionNavigations => OwnedCollectionNavigations;

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
        public EntityTypeBuilder<TEntity> HasKey(Expression<Func<TEntity, object>> keyExpression)
        {
            ArgumentNullException.ThrowIfNull(keyExpression);
            if (keyExpression.Body is NewExpression ne)
            {
                foreach (var arg in ne.Arguments)
                {
                    var prop = GetProperty(arg);
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
        /// <param name="tableName">Name of the child table. Defaults to the owned type name.</param>
        /// <param name="foreignKey">FK column name referencing owner's PK. Defaults to owner type name + "Id".</param>
        /// <param name="buildAction">Optional configuration for the owned element type.</param>
        /// <returns>The same builder instance for chaining.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="navigation"/> is null.</exception>
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
                return meProp;
            if (expression is UnaryExpression ue && ue.Operand is MemberExpression ume && ume.Member is PropertyInfo umeProp)
                return umeProp;
            throw new ArgumentException("Expression must select a property (not a field or method). Received: " + expression.NodeType, nameof(expression));
        }

        private PropertyInfo GetProperty(LambdaExpression expression) => GetProperty(expression.Body);

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
            /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for chaining.</returns>
            /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or whitespace.</exception>
            public EntityTypeBuilder<TEntity> HasColumnName(string name)
            {
                // Validation delegated to SetColumnName
                _parent._config.SetColumnName(_property, name);
                return _parent;
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
            internal readonly PropertyInfo PrincipalNavigation;

            internal CollectionNavigationBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo principalNavigation)
            {
                _parent = parent;
                PrincipalNavigation = principalNavigation;
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
                return new ReferenceCollectionBuilder(_parent, PrincipalNavigation, dependentNav);
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
                return new ManyToManyBuilder(_parent, PrincipalNavigation, inverseNavName);
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
                /// <exception cref="ArgumentException">Thrown when any parameter is null or whitespace.</exception>
                public EntityTypeBuilder<TEntity> UsingTable(string joinTable, string leftFk, string rightFk)
                {
                    if (string.IsNullOrWhiteSpace(joinTable))
                        throw new ArgumentException("Join table name cannot be null or whitespace.", nameof(joinTable));
                    if (string.IsNullOrWhiteSpace(leftFk))
                        throw new ArgumentException("Left FK column name cannot be null or whitespace.", nameof(leftFk));
                    if (string.IsNullOrWhiteSpace(rightFk))
                        throw new ArgumentException("Right FK column name cannot be null or whitespace.", nameof(rightFk));
                    _parent._config.AddManyToMany(new ManyToManyConfiguration(
                        _principalNavigation.Name,
                        typeof(TDependent),
                        joinTable,
                        leftFk,
                        rightFk,
                        _inverseNavName));
                    return _parent;
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
                /// <returns>The parent <see cref="EntityTypeBuilder{TEntity}"/> for further configuration.</returns>
                /// <exception cref="ArgumentNullException">Thrown when <paramref name="foreignKeyExpression"/> is null.</exception>
                /// <exception cref="NormConfigurationException">Thrown when the principal key cannot be inferred.</exception>
                public EntityTypeBuilder<TEntity> HasForeignKey(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression = null)
                {
                    ArgumentNullException.ThrowIfNull(foreignKeyExpression);
                    var fkProp = _parent.GetProperty(foreignKeyExpression);
                    PropertyInfo? principalKey = null;
                    if (principalKeyExpression != null)
                    {
                        principalKey = _parent.GetProperty(principalKeyExpression);
                    }
                    else if (_parent._config.KeyProperties.Count == 1)
                    {
                        principalKey = _parent._config.KeyProperties[0];
                    }
                    else
                    {
                        throw new NormConfigurationException(string.Format(ErrorMessages.InvalidConfiguration, $"Principal key must be specified for relationship '{_principalNavigation.Name}' on entity {typeof(TEntity).Name}"));
                    }
                    _parent._config.AddRelationship(new RelationshipConfiguration(_principalNavigation, typeof(TDependent), _dependentNavigation, principalKey, fkProp));
                    return _parent;
                }
            }
        }
    }
}