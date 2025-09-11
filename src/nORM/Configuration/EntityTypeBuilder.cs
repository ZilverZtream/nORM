using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Core;

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

            /// <summary>
            /// Sets the database table name that <typeparamref name="TEntity"/> maps to.
            /// </summary>
            /// <param name="name">The unescaped table name.</param>
            public void SetTableName(string name) => TableName = name;

            /// <summary>
            /// Registers a property as part of the entity's primary key.
            /// </summary>
            /// <param name="prop">The property to mark as a key member.</param>
            public void AddKey(PropertyInfo prop)
            {
                if (!KeyProperties.Contains(prop))
                    KeyProperties.Add(prop);
            }

            /// <summary>
            /// Overrides the column name used for the specified property.
            /// </summary>
            /// <param name="prop">The property whose column name to configure.</param>
            /// <param name="name">The column name to map the property to.</param>
            public void SetColumnName(PropertyInfo prop, string name) => ColumnNames[prop] = name;
            /// <summary>
            /// Configures this entity to share its database table with the specified
            /// principal entity type.
            /// </summary>
            /// <param name="principal">The CLR type of the principal entity that owns the table.</param>
            public void SetTableSplit(Type principal) => TableSplitWith = principal;

            /// <summary>
            /// Registers an owned navigation property for the entity type. Owned entities
            /// are mapped to the same table as the owning entity.
            /// </summary>
            /// <param name="prop">The navigation property representing the owned entity.</param>
            /// <param name="config">Optional configuration for the owned entity type.</param>
            public void AddOwned(PropertyInfo prop, IEntityTypeConfiguration? config) => OwnedNavigations[prop] = new OwnedNavigation(prop.PropertyType, config);

            /// <summary>
            /// Adds a shadow property – a property not defined on the CLR type – to the entity configuration.
            /// </summary>
            /// <param name="name">The name of the shadow property.</param>
            /// <param name="clrType">The CLR type the shadow property should expose.</param>
            public void AddShadowProperty(string name, Type clrType) => ShadowProperties[name] = new ShadowPropertyConfiguration(clrType);

            /// <summary>
            /// Sets the database column name for a previously defined shadow property.
            /// </summary>
            /// <param name="name">The name of the shadow property.</param>
            /// <param name="column">The column name to map the property to.</param>
            public void SetShadowColumnName(string name, string column)
            {
                if (ShadowProperties.TryGetValue(name, out var sp)) ShadowProperties[name] = sp with { ColumnName = column };
            }

            /// <summary>
            /// Adds a configured relationship to this entity type.
            /// </summary>
            /// <param name="relationship">The relationship configuration to register.</param>
            public void AddRelationship(RelationshipConfiguration relationship) => Relationships.Add(relationship);
        }

        private readonly MappingConfiguration _config = new();
        internal IEntityTypeConfiguration Configuration => _config;

        /// <summary>
        /// Specifies the database table that <typeparamref name="TEntity"/> maps to.
        /// </summary>
        /// <param name="name">The unqualified table name.</param>
        /// <returns>The same <see cref="EntityTypeBuilder{TEntity}"/> instance for chaining.</returns>
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
        public EntityTypeBuilder<TEntity> HasKey(Expression<Func<TEntity, object>> keyExpression)
        {
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
        public PropertyBuilder Property(Expression<Func<TEntity, object>> propertyExpression)
        {
            var prop = GetProperty(propertyExpression);
            return new PropertyBuilder(this, prop);
        }

        /// <summary>
        /// Creates a shadow property on the entity type configuration.
        /// </summary>
        /// <typeparam name="TProperty">CLR type of the shadow property.</typeparam>
        /// <param name="name">The name of the shadow property.</param>
        /// <returns>A <see cref="ShadowPropertyBuilder"/> for further configuration.</returns>
        public ShadowPropertyBuilder Property<TProperty>(string name)
        {
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
        public EntityTypeBuilder<TEntity> OwnsOne<TOwned>(Expression<Func<TEntity, TOwned>> navigation, Action<EntityTypeBuilder<TOwned>>? buildAction = null) where TOwned : class
        {
            var prop = GetProperty(navigation);
            var builder = new EntityTypeBuilder<TOwned>();
            buildAction?.Invoke(builder);
            _config.AddOwned(prop, builder.Configuration);
            return this;
        }

        /// <summary>
        /// Begins configuration of a collection navigation property.
        /// </summary>
        /// <typeparam name="TProperty">The type of the dependent entity.</typeparam>
        /// <param name="navigation">Expression selecting the collection navigation.</param>
        /// <returns>A <see cref="CollectionNavigationBuilder{TProperty}"/> for relationship configuration.</returns>
        public CollectionNavigationBuilder<TProperty> HasMany<TProperty>(Expression<Func<TEntity, IEnumerable<TProperty>>> navigation) where TProperty : class
        {
            var prop = GetProperty(navigation);
            return new CollectionNavigationBuilder<TProperty>(this, prop);
        }

        private PropertyInfo GetProperty(Expression expression)
        {
            if (expression is MemberExpression me)
                return (PropertyInfo)me.Member;
            if (expression is UnaryExpression ue && ue.Operand is MemberExpression ume)
                return (PropertyInfo)ume.Member;
            throw new ArgumentException("Expression is not a valid property selector.", nameof(expression));
        }

        private PropertyInfo GetProperty(LambdaExpression expression) => GetProperty(expression.Body);

        /// <summary>
        /// Provides configuration options for a specific property on the entity type.
        /// </summary>
        public class PropertyBuilder
        {
            private readonly EntityTypeBuilder<TEntity> _parent;
            private readonly PropertyInfo _property;

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
            public EntityTypeBuilder<TEntity> HasColumnName(string name)
            {
                _parent._config.SetColumnName(_property, name);
                return _parent;
            }
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
            public EntityTypeBuilder<TEntity> HasColumnName(string columnName)
            {
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
                /// <exception cref="NormConfigurationException">Thrown when the principal key cannot be inferred.</exception>
                public EntityTypeBuilder<TEntity> HasForeignKey(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression = null)
                {
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