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

            public void SetTableName(string name) => TableName = name;
            public void SetKey(PropertyInfo prop) => KeyProperty = prop;
            public void SetColumnName(PropertyInfo prop, string name) => ColumnNames[prop] = name;
        }

        private readonly MappingConfiguration _config = new();
        internal IEntityTypeConfiguration Configuration => _config;

        public EntityTypeBuilder<TEntity> ToTable(string name)
        {
            _config.SetTableName(name);
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
    }
}