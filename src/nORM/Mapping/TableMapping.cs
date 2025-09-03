using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using nORM.Configuration;
using nORM.Core;
using nORM.Providers;
using System.Linq.Expressions;

#nullable enable

namespace nORM.Mapping
{
    public sealed class TableMapping
    {
        public readonly Type Type;
        public string EscTable;
        public readonly Column[] Columns;
        public readonly Column[] KeyColumns;
        public readonly Column? TimestampColumn;
        public readonly Dictionary<string, Relation> Relations = new();
        public readonly DatabaseProvider Provider;
        public readonly Column? DiscriminatorColumn = null;
        public readonly Dictionary<object, TableMapping> TphMappings = new();

        public TableMapping(Type t, DatabaseProvider p, DbContext ctx, IEntityTypeConfiguration? fluentConfig)
        {
            Type = t;
            Provider = p;

            var splitAttr = t.GetCustomAttribute<TableSplitAttribute>();
            var splitType = fluentConfig?.TableSplitWith ?? splitAttr?.PrincipalType;
            if (splitType != null)
            {
                var principal = ctx.GetMapping(splitType);
                EscTable = principal.EscTable;
            }
            else
            {
                var tableName = fluentConfig?.TableName ?? t.GetCustomAttribute<TableAttribute>()?.Name ?? t.Name;
                EscTable = p.Escape(tableName);
            }

            var cols = new List<Column>();
            foreach (var prop in t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(x => x.CanRead && x.CanWrite && x.GetCustomAttribute<NotMappedAttribute>() == null))
            {
                OwnedNavigation? ownedNav = null;
                fluentConfig?.OwnedNavigations.TryGetValue(prop, out ownedNav);
                if (ownedNav != null || prop.PropertyType.GetCustomAttribute<OwnedAttribute>() != null)
                {
                    var ownedType = ownedNav?.OwnedType ?? prop.PropertyType;
                    var ownedConfig = ownedNav?.Configuration;
                    foreach (var sp in ownedType.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                        .Where(x => x.CanRead && x.CanWrite && x.GetCustomAttribute<NotMappedAttribute>() == null))
                    {
                        var getter = CreateOwnedGetter(prop, sp);
                        var setter = CreateOwnedSetter(prop, sp);
                        cols.Add(new Column(sp, p, ownedConfig, prop.Name, getter, setter));
                    }
                }
                else
                {
                    cols.Add(new Column(prop, p, fluentConfig));
                }
            }

            var discriminatorAttr = t.GetCustomAttribute<DiscriminatorColumnAttribute>();
            if (discriminatorAttr != null)
            {
                DiscriminatorColumn = cols.FirstOrDefault(c => string.Equals(c.Prop.Name, discriminatorAttr.PropertyName, StringComparison.OrdinalIgnoreCase));
                var derivedTypes = AppDomain.CurrentDomain.GetAssemblies()
                    .SelectMany(a => a.GetTypes())
                    .Where(tp => tp.BaseType == t && tp.GetCustomAttribute<DiscriminatorValueAttribute>() != null);
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

            KeyColumns = Columns.Where(c => c.IsKey).ToArray();
            TimestampColumn = Columns.FirstOrDefault(c => c.IsTimestamp);

            DiscoverRelations(ctx);
        }

        private static Func<object, object?> CreateOwnedGetter(PropertyInfo owner, PropertyInfo owned)
        {
            var entityParam = Expression.Parameter(typeof(object), "e");
            var castEntity = Expression.Convert(entityParam, owner.DeclaringType!);
            var ownerAccess = Expression.Property(castEntity, owner);
            var nullCheck = Expression.Equal(ownerAccess, Expression.Constant(null, owner.PropertyType));
            var ownedAccess = Expression.Property(ownerAccess, owned);
            Expression body = Expression.Condition(
                nullCheck,
                Expression.Constant(null, typeof(object)),
                Expression.Convert(ownedAccess, typeof(object)));
            return Expression.Lambda<Func<object, object?>>(body, entityParam).Compile();
        }

        private static Action<object, object?> CreateOwnedSetter(PropertyInfo owner, PropertyInfo owned)
        {
            var entityParam = Expression.Parameter(typeof(object), "e");
            var valueParam = Expression.Parameter(typeof(object), "v");
            var castEntity = Expression.Convert(entityParam, owner.DeclaringType!);
            var ownerAccess = Expression.Property(castEntity, owner);
            var ownerVar = Expression.Variable(owner.PropertyType, "ownedObj");

            var assignBlock = Expression.Block(new[] { ownerVar },
                Expression.Assign(ownerVar, ownerAccess),
                Expression.IfThen(
                    Expression.Equal(ownerVar, Expression.Constant(null, owner.PropertyType)),
                    Expression.Block(
                        Expression.Assign(ownerVar, Expression.New(owner.PropertyType)),
                        Expression.Assign(ownerAccess, ownerVar)
                    )
                ),
                Expression.Assign(Expression.Property(ownerVar, owned), Expression.Convert(valueParam, owned.PropertyType))
            );

            return Expression.Lambda<Action<object, object?>>(assignBlock, entityParam, valueParam).Compile();
        }

        private void DiscoverRelations(DbContext ctx)
        {
            foreach (var prop in Type.GetProperties().Where(pr => pr.GetCustomAttribute<NotMappedAttribute>() == null))
            {
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

        public void SetPrimaryKey(object entity, object value)
        {
            var keyCol = KeyColumns.FirstOrDefault(k => k.IsDbGenerated);
            if (keyCol != null)
            {
                var convertedValue = Convert.ChangeType(value, keyCol.Prop.PropertyType);
                keyCol.Setter(entity, convertedValue);
            }
        }

        public record Relation(PropertyInfo NavProp, Type DependentType, Column PrincipalKey, Column ForeignKey);
    }
}