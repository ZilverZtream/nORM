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

        public TableMapping(Type t, DatabaseProvider p, DbContext ctx, IEntityTypeConfiguration? fluentConfig)
        {
            Type = t;
            Provider = p;

            var tableName = fluentConfig?.TableName ?? t.GetCustomAttribute<TableAttribute>()?.Name ?? t.Name;
            EscTable = p.Escape(tableName);

            Columns = t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                .Where(x => x.CanRead && x.CanWrite && x.GetCustomAttribute<NotMappedAttribute>() == null)
                .Select(x => new Column(x, p, fluentConfig)).ToArray();

            KeyColumns = Columns.Where(c => c.IsKey).ToArray();
            TimestampColumn = Columns.FirstOrDefault(c => c.IsTimestamp);

            DiscoverRelations(ctx);
        }

        private void DiscoverRelations(DbContext ctx)
        {
            foreach (var prop in Type.GetProperties().Where(pr => pr.GetCustomAttribute<NotMappedAttribute>() == null))
            {
                if (typeof(IEnumerable).IsAssignableFrom(prop.PropertyType) && prop.PropertyType.IsGenericType)
                {
                    var dependentType = prop.PropertyType.GetGenericArguments()[0];
                    var dependentMap = ctx.GetMapping(dependentType);
                    var foreignKeyProp = dependentMap.Columns.FirstOrDefault(c => c.ForeignKeyPrincipalTypeName == Type.Name);

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