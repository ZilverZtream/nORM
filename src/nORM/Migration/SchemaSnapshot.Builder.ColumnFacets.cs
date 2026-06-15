using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Globalization;
using System.Linq;
using System.Reflection;
using nORM.Configuration;
using nORM.Mapping;

namespace nORM.Migration
{
    public static partial class SchemaSnapshotBuilder
    {
        private static ColumnAttribute? GetColumnAttribute(Column column)
            => column.IsShadow ? null : column.Prop.GetCustomAttribute<ColumnAttribute>();

        private static DatabaseGeneratedOption? GetDatabaseGeneratedOption(Column column)
            => column.IsShadow
                ? null
                : column.Prop.GetCustomAttribute<DatabaseGeneratedAttribute>()?.DatabaseGeneratedOption;

        private static int? GetMaxLength(Column column, Type clrType)
            => column.IsShadow
                ? null
                : GetMaxLength(column.Prop, clrType, GetColumnAttribute(column), ParseStringBinaryFacets(GetColumnAttribute(column)?.TypeName, clrType));

        private static int? GetMaxLength(
            Column column,
            Type clrType,
            IEntityTypeConfiguration? configuration,
            ColumnAttribute? columnAttribute,
            ScaffoldStringBinaryFacets storeFacets)
        {
            if (!column.IsShadow
                && configuration?.MaxLengths.TryGetValue(column.Prop, out var configuredMaxLength) == true)
            {
                return configuredMaxLength;
            }

            return column.IsShadow
                ? null
                : GetMaxLength(column.Prop, clrType, columnAttribute, storeFacets);
        }

        private static int? GetMaxLength(
            PropertyInfo property,
            Type clrType,
            ColumnAttribute? columnAttribute,
            ScaffoldStringBinaryFacets storeFacets)
        {
            if (clrType != typeof(string) && clrType != typeof(byte[]))
                return null;

            var maxLengthAttribute = property.GetCustomAttribute<MaxLengthAttribute>();
            var length = maxLengthAttribute?.Length > 0
                ? maxLengthAttribute.Length
                : (int?)null;

            if (clrType == typeof(string))
            {
                var stringLengthAttribute = property.GetCustomAttribute<StringLengthAttribute>();
                if (stringLengthAttribute?.MaximumLength > 0)
                {
                    length = length.HasValue
                        ? Math.Min(length.Value, stringLengthAttribute.MaximumLength)
                        : stringLengthAttribute.MaximumLength;
                }
            }

            return length ?? storeFacets.MaxLength;
        }

        private static bool? GetUnicode(
            Column column,
            Type clrType,
            IEntityTypeConfiguration? configuration,
            ScaffoldStringBinaryFacets storeFacets)
        {
            if (clrType != typeof(string))
                return null;

            if (!column.IsShadow
                && configuration?.UnicodeSettings.TryGetValue(column.Prop, out var configuredUnicode) == true)
            {
                return configuredUnicode;
            }

            return storeFacets.IsUnicode;
        }

        private static bool GetFixedLength(
            Column column,
            Type clrType,
            IEntityTypeConfiguration? configuration,
            ScaffoldStringBinaryFacets storeFacets)
        {
            if (clrType != typeof(string) && clrType != typeof(byte[]))
                return false;

            if (!column.IsShadow
                && configuration?.FixedLengthSettings.TryGetValue(column.Prop, out var configuredFixedLength) == true)
            {
                return configuredFixedLength;
            }

            return storeFacets.IsFixedLength;
        }

        private static (int? Precision, int? Scale) GetPrecision(
            Column column,
            Type clrType,
            IEntityTypeConfiguration? configuration,
            ColumnAttribute? columnAttribute)
        {
            if (!column.IsShadow
                && configuration?.Precisions.TryGetValue(column.Prop, out var configuredPrecision) == true)
            {
                return (configuredPrecision.Precision, configuredPrecision.Scale);
            }

            return TryParseDecimalPrecision(columnAttribute?.TypeName, clrType);
        }

        private static Dictionary<PropertyInfo, IndexAttribute[]> BuildIndexAttributesByProperty(IEnumerable<Column> columns)
            => columns
                .Where(static column => !column.IsShadow)
                .Select(static column => column.Prop)
                .Distinct()
                .ToDictionary(
                    static prop => prop,
                    static prop => prop.GetCustomAttributes<IndexAttribute>().ToArray());

        private static Dictionary<string, int> BuildIndexColumnCounts(IReadOnlyDictionary<PropertyInfo, IndexAttribute[]> indexAttributesByProperty)
            => indexAttributesByProperty.Values
                .SelectMany(static attrs => attrs
                    .Where(static attr => !attr.IsIncluded)
                    .Select(static attr => attr.Name))
                .GroupBy(static name => name, StringComparer.OrdinalIgnoreCase)
                .ToDictionary(static group => group.Key, static group => group.Count(), StringComparer.OrdinalIgnoreCase);

        private static void ApplyIndexAttributes(
            ColumnSchema column,
            IReadOnlyDictionary<PropertyInfo, IndexAttribute[]> indexAttributesByProperty,
            IReadOnlyDictionary<string, int> indexColumnCounts,
            PropertyInfo property)
        {
            if (!indexAttributesByProperty.TryGetValue(property, out var indexAttrs) || indexAttrs.Length == 0)
                return;

            var firstIndexAttr = indexAttrs[0];
            if (!column.IsPrimaryKey)
            {
                column.IndexName = firstIndexAttr.Name;
                column.IndexOrder = firstIndexAttr.Order;
            }

            if (indexAttrs.Any(attr =>
                    attr.IsUnique &&
                    indexColumnCounts.TryGetValue(attr.Name, out var count) &&
                    count == 1))
            {
                column.IsUnique = true;
            }

            foreach (var indexAttr in indexAttrs)
            {
                column.Indexes.Add(new ColumnIndexSchema
                {
                    Name = indexAttr.Name,
                    IsUnique = indexAttr.IsUnique,
                    Order = indexAttr.Order,
                    IsDescending = indexAttr.IsDescending,
                    IsIncluded = indexAttr.IsIncluded,
                    NullsNotDistinct = indexAttr.NullsNotDistinct,
                    NullSortOrder = indexAttr.NullSortOrder,
                    FilterSql = indexAttr.FilterSql
                });
            }
        }

        private static Column ResolveOwnerKeyColumnForOwnedFk(Column[] ownerKeyColumns, string ownedFkColumnName, string ownerTypeName)
        {
            if (ownerKeyColumns.Length == 1)
                return ownerKeyColumns[0];

            var match = Array.Find(ownerKeyColumns, c =>
                string.Equals(c.Name, ownedFkColumnName, StringComparison.OrdinalIgnoreCase));
            if (match != null)
                return match;

            if (ownedFkColumnName.Length > ownerTypeName.Length
                && ownedFkColumnName.StartsWith(ownerTypeName, StringComparison.OrdinalIgnoreCase))
            {
                var suffix = ownedFkColumnName.Substring(ownerTypeName.Length);
                match = Array.Find(ownerKeyColumns, c =>
                    string.Equals(c.Name, suffix, StringComparison.OrdinalIgnoreCase));
                if (match != null)
                    return match;
            }

            return ownerKeyColumns[0];
        }

        private static void AddJoinColumns(
            TableSchema joinTable,
            Dictionary<string, ColumnSchema> columnsByName,
            IReadOnlyList<string> fkColumns,
            IReadOnlyList<Column> principalColumns,
            string joinTableName)
        {
            for (var i = 0; i < fkColumns.Count; i++)
            {
                var fkColumn = fkColumns[i];
                if (columnsByName.ContainsKey(fkColumn))
                    continue;

                var principalColumn = principalColumns[i];
                var clrType = Nullable.GetUnderlyingType(principalColumn.Prop.PropertyType) ?? principalColumn.Prop.PropertyType;
                var column = new ColumnSchema
                {
                    Name         = fkColumn,
                    ClrType      = clrType.FullName ?? clrType.ToString(),
                    IsNullable   = false,
                    IsPrimaryKey = true,
                    IsUnique     = false,
                    IndexName    = $"PK_{joinTableName}"
                };
                columnsByName[fkColumn] = column;
                joinTable.Columns.Add(column);
            }
        }

        private static string FormatTableName(string tableName, string? schemaName)
            => string.IsNullOrWhiteSpace(schemaName) ? tableName : schemaName + "." + tableName;

        private static string ToForeignKeyActionSql(ReferentialAction action)
            => action switch
            {
                ReferentialAction.Cascade => "CASCADE",
                ReferentialAction.SetNull => "SET NULL",
                ReferentialAction.Restrict => "RESTRICT",
                ReferentialAction.SetDefault => "SET DEFAULT",
                _ => "NO ACTION"
            };

        /// <summary>
        /// Returns entity candidate types from the assembly: non-abstract classes (including
        /// nested types) that carry a <see cref="TableAttribute"/> or at least one
        /// <see cref="KeyAttribute"/> property. No visibility filter is applied because nested
        /// public types report <c>IsNestedPublic</c> rather than <c>IsPublic</c>.
        /// </summary>
        private static IEnumerable<Type> GetEntityCandidates(Assembly assembly)
        {
            Type[] types;
            try
            {
                types = assembly.GetTypes();
            }
            catch (ReflectionTypeLoadException ex)
            {
                // Some types in the assembly may fail to load (e.g. missing dependencies).
                // Use the successfully loaded subset rather than failing the entire snapshot.
                types = (ex.Types ?? Array.Empty<Type?>()).Where(t => t != null).Select(t => t!).ToArray();
            }

            return types.Where(t =>
                t.IsClass && !t.IsAbstract &&
                (t.GetCustomAttribute<TableAttribute>() != null ||
                 t.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                  .Any(p => p.GetCustomAttribute<KeyAttribute>() != null)));
        }

        /// <summary>
        /// Returns true for types that map to database scalar columns.
        /// Reference navigation properties and collection properties return false.
        /// </summary>
        private static bool IsMappableType(Type t)
        {
            // Unwrap Nullable<T> - e.g. int?, Guid?
            var underlying = Nullable.GetUnderlyingType(t);
            if (underlying != null)
                return true; // Nullable value types are always mappable

            // Value types (int, bool, DateTime, Guid, enum, etc.) are always mappable
            if (t.IsValueType)
                return true;

            // string and byte[] are the only reference types that are scalar columns
            if (t == typeof(string) || t == typeof(byte[]))
                return true;

            // All other reference types (class, interface) are navigation properties
            return false;
        }

        private static string GetTableName(Type type, TableAttribute? tableAttribute)
        {
            if (tableAttribute is null)
                return type.Name;

            return string.IsNullOrWhiteSpace(tableAttribute.Schema)
                ? tableAttribute.Name
                : tableAttribute.Schema + "." + tableAttribute.Name;
        }

        private readonly record struct ScaffoldStringBinaryFacets(int? MaxLength, bool? IsUnicode, bool IsFixedLength);

        private static ScaffoldStringBinaryFacets ParseStringBinaryFacets(string? typeName, Type clrType)
        {
            if ((clrType != typeof(string) && clrType != typeof(byte[]))
                || string.IsNullOrWhiteSpace(typeName))
            {
                return default;
            }

            var normalized = NormalizeStoreTypeName(typeName);
            var open = normalized.LastIndexOf('(');
            var close = open >= 0 ? normalized.IndexOf(')', open + 1) : -1;
            if ((open >= 0 && close != normalized.Length - 1)
                || (open < 0 && normalized.Contains(')', StringComparison.Ordinal)))
            {
                return default;
            }

            var baseName = (open >= 0 ? normalized[..open] : normalized).Trim();
            var args = open >= 0 && close > open
                ? normalized.Substring(open + 1, close - open - 1)
                    .Split(',', StringSplitOptions.TrimEntries)
                : Array.Empty<string>();
            if (args.Length > 1 || args.Any(static arg => arg.Length == 0))
                return default;

            var maxLength = args.Length == 1
                && !string.Equals(args[0], "max", StringComparison.OrdinalIgnoreCase)
                && int.TryParse(args[0], NumberStyles.None, CultureInfo.InvariantCulture, out var parsedLength)
                && parsedLength > 0
                ? parsedLength
                : (int?)null;

            return clrType == typeof(byte[])
                ? baseName switch
                {
                    "binary" => new ScaffoldStringBinaryFacets(maxLength, null, true),
                    "varbinary" => new ScaffoldStringBinaryFacets(maxLength, null, false),
                    _ => default
                }
                : baseName switch
                {
                    "nchar" or "national character" => new ScaffoldStringBinaryFacets(maxLength, true, true),
                    "nvarchar" or "national character varying" => new ScaffoldStringBinaryFacets(maxLength, true, false),
                    "char" => new ScaffoldStringBinaryFacets(maxLength, false, true),
                    "varchar" => new ScaffoldStringBinaryFacets(maxLength, false, false),
                    "text" => new ScaffoldStringBinaryFacets(null, false, false),
                    "ntext" => new ScaffoldStringBinaryFacets(null, true, false),
                    "character" => new ScaffoldStringBinaryFacets(maxLength, null, true),
                    "character varying" or "varying character" => new ScaffoldStringBinaryFacets(maxLength, null, false),
                    _ => default
                };
        }

        private static string NormalizeStoreTypeName(string typeName)
        {
            var normalized = typeName.Trim().ToLowerInvariant();
            if (normalized.StartsWith("domain (", StringComparison.Ordinal) && normalized.EndsWith(")", StringComparison.Ordinal))
            {
                var arrow = normalized.LastIndexOf("->", StringComparison.Ordinal);
                if (arrow >= 0)
                    normalized = normalized.Substring(arrow + 2, normalized.Length - arrow - 3).Trim();
            }

            var open = normalized.IndexOf('(');
            var baseName = open >= 0 ? normalized[..open].Trim() : normalized;
            var lastDot = baseName.LastIndexOf('.');
            if (lastDot >= 0 && lastDot + 1 < baseName.Length)
                normalized = baseName[(lastDot + 1)..] + (open >= 0 ? normalized[open..] : string.Empty);

            return normalized;
        }

        private static (int? Precision, int? Scale) TryParseDecimalPrecision(string? typeName, Type clrType)
        {
            if (clrType != typeof(decimal) || string.IsNullOrWhiteSpace(typeName))
                return (null, null);

            var open = typeName.LastIndexOf('(');
            var close = typeName.IndexOf(')', open + 1);
            if (open < 0 || close < 0)
                return (null, null);

            var baseName = typeName[..open].Trim();
            if (!EndsWithDelimitedTypeName(baseName, "decimal")
                && !EndsWithDelimitedTypeName(baseName, "numeric"))
            {
                return (null, null);
            }

            var precisionAndScaleText = typeName.Substring(open + 1, close - open - 1);
            var parts = precisionAndScaleText
                .Split(',', StringSplitOptions.TrimEntries);
            if (parts.Length is < 1 or > 2)
                return (null, null);

            if (int.TryParse(parts[0], NumberStyles.None, CultureInfo.InvariantCulture, out var precision)
                && precision > 0)
            {
                if (parts.Length == 1)
                    return (precision, null);

                if (int.TryParse(parts[1], NumberStyles.None, CultureInfo.InvariantCulture, out var scale)
                    && scale >= 0
                    && scale <= precision)
                {
                    return (precision, scale);
                }
            }

            return (null, null);
        }

        private static bool EndsWithDelimitedTypeName(string text, string typeName)
        {
            var trimmed = text.TrimEnd();
            if (!trimmed.EndsWith(typeName, StringComparison.OrdinalIgnoreCase))
                return false;

            var prefixLength = trimmed.Length - typeName.Length;
            return prefixLength == 0 || !IsTypeNameIdentifierChar(trimmed[prefixLength - 1]);
        }

        private static bool IsTypeNameIdentifierChar(char value)
            => char.IsLetterOrDigit(value) || value == '_';
    }
}
