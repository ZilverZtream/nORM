using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

#nullable enable

namespace nORM.Configuration
{
    public class EntityTypeBuilder<TEntity> where TEntity : class
    {
        internal class MappingConfiguration : IEntityTypeConfiguration
        {
            public string? TableName { get; private set; }
            public PropertyInfo? KeyProperty { get; private set; }
            public Dictionary<PropertyInfo, string> ColumnNames { get; } = new();
            public Type? TableSplitWith { get; private set; }
            public Dictionary<PropertyInfo, OwnedNavigation> OwnedNavigations { get; } = new();
            public Dictionary<string, ShadowPropertyConfiguration> ShadowProperties { get; } = new();
            public List<RelationshipConfiguration> Relationships { get; } = new();

            public void SetTableName(string name) => TableName = name;
            public void SetKey(PropertyInfo prop) => KeyProperty = prop;
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
            var prop = GetProperty(keyExpression);
            _config.SetKey(prop);
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

        private PropertyInfo GetProperty(LambdaExpression expression)
        {
            if (expression.Body is MemberExpression me)
                return (PropertyInfo)me.Member;
            if (expression.Body is UnaryExpression ue && ue.Operand is MemberExpression ume)
                return (PropertyInfo)ume.Member;
            throw new ArgumentException("Expression is not a valid property selector.", nameof(expression));
        }

        public class PropertyBuilder
        {
            private readonly EntityTypeBuilder<TEntity> _parent;
            private readonly PropertyInfo _property;

            internal PropertyBuilder(EntityTypeBuilder<TEntity> parent, PropertyInfo property)
            {
                _parent = parent;
                _property = property;
            }

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

                public EntityTypeBuilder<TEntity> HasForeignKey(
                    Expression<Func<TDependent, object>> foreignKeyExpression,
                    Expression<Func<TEntity, object>>? principalKeyExpression = null)
                {
                    var fkProp = _parent.GetProperty(foreignKeyExpression);
                    var principalKey = principalKeyExpression != null
                        ? _parent.GetProperty(principalKeyExpression)
                        : _parent._config.KeyProperty;
                    _parent._config.AddRelationship(new RelationshipConfiguration(_principalNavigation, typeof(TDependent), _dependentNavigation, principalKey, fkProp));
                    return _parent;
                }
            }
        }
    }
}