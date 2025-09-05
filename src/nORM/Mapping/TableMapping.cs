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
    public sealed class TableMapping
    {
        public readonly Type Type;
        public string EscTable;
        public readonly Column[] Columns;
        public readonly Column[] KeyColumns;
        public readonly Column? TimestampColumn;
        public readonly Column? TenantColumn;
        public string TableName { get; }
        public readonly Dictionary<string, Relation> Relations = new();
        public readonly DatabaseProvider Provider;
        public readonly Column? DiscriminatorColumn = null;
        public readonly Dictionary<object, TableMapping> TphMappings = new();
        private readonly IEntityTypeConfiguration? _fluentConfig;

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
            TenantColumn = Columns.FirstOrDefault(c => c.PropName == ctx.Options.TenantColumnName);

            DiscoverRelations(ctx);
        }

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

        public void SetPrimaryKey(object entity, object value)
        {
            var keyCol = KeyColumns.FirstOrDefault(k => k.IsDbGenerated);
            if (keyCol != null)
            {
                var convertedValue = Convert.ChangeType(value, keyCol.Prop.PropertyType);
                keyCol.Setter(entity, convertedValue);
            }
        }

        public record Relation(PropertyInfo NavProp, Type DependentType, Column PrincipalKey, Column ForeignKey, bool CascadeDelete = true);
    }
}