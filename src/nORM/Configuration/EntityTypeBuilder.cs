using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using nORM.Core;

#nullable enable

namespace nORM.Configuration
{
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

            public void SetTableName(string name) => TableName = name;
            public void AddKey(PropertyInfo prop)
            {
                if (!KeyProperties.Contains(prop))
                    KeyProperties.Add(prop);
            }
            public void SetColumnName(PropertyInfo prop, string name) => ColumnNames[prop] = name;
            public void SetTableSplit(Type principal) => TableSplitWith = principal;
            public void AddOwned(PropertyInfo prop, IEntityTypeConfiguration? config) => OwnedNavigations[prop] = new OwnedNavigation(prop.PropertyType, config);
            public void AddShadowProperty(string name, Type clrType) => ShadowProperties[name] = new ShadowPropertyConfiguration(clrType);
            public void SetShadowColumnName(string name, string column)
            {
                if (ShadowProperties.TryGetValue(name, out var sp)) ShadowProperties[name] = sp with { ColumnName = column };
            }

            public void AddRelationship(RelationshipConfiguration relationship) => Relationships.Add(relationship);
        }

        private readonly MappingConfiguration _config = new();
        internal IEntityTypeConfiguration Configuration => _config;

        public EntityTypeBuilder<TEntity> ToTable(string name)
        {
            _config.SetTableName(name);
            return this;
        }

        public EntityTypeBuilder<TEntity> SharesTableWith<TPrincipal>()
        {
            _config.SetTableSplit(typeof(TPrincipal));
            return this;
        }

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

        public PropertyBuilder Property(Expression<Func<TEntity, object>> propertyExpression)
        {
            var prop = GetProperty(propertyExpression);
            return new PropertyBuilder(this, prop);
        }

        public ShadowPropertyBuilder Property<TProperty>(string name)
        {
            _config.AddShadowProperty(name, typeof(TProperty));
            return new ShadowPropertyBuilder(this, name);
        }

        public EntityTypeBuilder<TEntity> OwnsOne<TOwned>(Expression<Func<TEntity, TOwned>> navigation, Action<EntityTypeBuilder<TOwned>>? buildAction = null) where TOwned : class
        {
            var prop = GetProperty(navigation);
            var builder = new EntityTypeBuilder<TOwned>();
            buildAction?.Invoke(builder);
            _config.AddOwned(prop, builder.Configuration);
            return this;
        }

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
            /// Configures the relationship to have a single optional reference back to the principal entity.
            /// </summary>
            /// <param name="navigation">Optional expression specifying the navigation property on the dependent type.</param>
            /// <returns>A builder for configuring the relationship further.</returns>
            public ReferenceCollectionBuilder WithOne(Expression<Func<TDependent, TEntity?>>? navigation = null)
            {
                PropertyInfo? dependentNav = null;
                if (navigation != null)
                    dependentNav = _parent.GetProperty(navigation);
                return new ReferenceCollectionBuilder(_parent, PrincipalNavigation, dependentNav);
            }

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
                /// Configures the foreign key property used by the dependent entity in the relationship.
                /// </summary>
                /// <param name="foreignKeyExpression">Expression selecting the foreign key property on the dependent type.</param>
                /// <param name="principalKeyExpression">Optional expression selecting the principal key; required when the principal has composite keys.</param>
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