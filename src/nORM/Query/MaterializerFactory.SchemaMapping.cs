using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Mapping;
using nORM.SourceGeneration;
using System.Globalization;
using nORM.Core;

namespace nORM.Query
{
    internal sealed partial class MaterializerFactory
    {
        private static OrdinalMapping CreateOrdinalMapping(DbDataReader reader, TableMapping mapping)
        {
            var fieldCount = reader.FieldCount;
            var ordinals = new int[mapping.Columns.Length];
            var isValid = true;

            // Build quick lookup for field names (case-insensitive), detecting duplicates.
            // Case-insensitive (OrdinalIgnoreCase) is a deliberate design choice: SQL column names are
            // case-insensitive in ANSI SQL and in all four supported providers (SQLite, SQL Server,
            // MySQL, PostgreSQL with quoted identifiers). Using OrdinalIgnoreCase here ensures that
            // C# property "Name" matches SQL column "name" or "NAME" without requiring exact casing,
            // which is especially important for cross-provider portability.
            // Duplicate column names (e.g., two JOINed tables both having "Id") are tracked in
            // the ambiguous set and excluded from name-based lookup to prevent silent wrong-table binding.
            var nameCount = new Dictionary<string, int>(fieldCount, StringComparer.OrdinalIgnoreCase);
            var fieldTypes = new Type[fieldCount];

            for (int i = 0; i < fieldCount; i++)
            {
                var name = reader.GetName(i);
                nameCount[name] = nameCount.GetValueOrDefault(name, 0) + 1;
                fieldTypes[i] = reader.GetFieldType(i);
            }

            var ambiguous = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var nameToOrdinal = new Dictionary<string, int>(fieldCount, StringComparer.OrdinalIgnoreCase);

            for (int i = 0; i < fieldCount; i++)
            {
                var name = reader.GetName(i);
                if (nameCount[name] > 1)
                {
                    // Ambiguous - do not add to name map, ordinal-based binding must be used.
                    ambiguous.Add(name);
                }
                else
                {
                    nameToOrdinal.TryAdd(name, i);
                }
            }

            // Map each column to its ordinal
            for (int i = 0; i < mapping.Columns.Length; i++)
            {
                var column = mapping.Columns[i];
                var propName = column.PropName;
                var expectedType = column.Prop.PropertyType;
                var foundOrdinal = UnmappedOrdinal;

                // Try direct property name match first
                if (nameToOrdinal.TryGetValue(propName, out foundOrdinal))
                {
                    if (IsTypeCompatible(fieldTypes[foundOrdinal], expectedType))
                    {
                        ordinals[i] = foundOrdinal;
                        continue;
                    }
                    // Type mismatch, continue searching
                    foundOrdinal = UnmappedOrdinal;
                }

                // Try table-qualified names for JOIN scenarios
                var tableName = mapping.TableName;
                if (!string.IsNullOrEmpty(tableName))
                {
                    var qualifiedNames = new[]
                    {
                        $"{tableName}.{propName}",
                        $"{tableName}_{propName}",
                        $"[{tableName}].[{propName}]",
                        $"`{tableName}`.`{propName}`"
                    };

                    foreach (var qualifiedName in qualifiedNames)
                    {
                        if (nameToOrdinal.TryGetValue(qualifiedName, out foundOrdinal))
                        {
                            if (IsTypeCompatible(fieldTypes[foundOrdinal], expectedType))
                            {
                                ordinals[i] = foundOrdinal;
                                break;
                            }
                            foundOrdinal = UnmappedOrdinal;
                        }
                    }
                }

                // Try escaped column name (strip bracket, backtick, and double-quote delimiters)
                if (foundOrdinal == UnmappedOrdinal && !string.IsNullOrEmpty(column.EscCol))
                {
                    var normalizedEscCol = column.EscCol.Trim('[', ']', '`', '"');
                    if (nameToOrdinal.TryGetValue(normalizedEscCol, out foundOrdinal))
                    {
                        if (!IsTypeCompatible(fieldTypes[foundOrdinal], expectedType))
                        {
                            foundOrdinal = UnmappedOrdinal;
                        }
                    }
                }

                if (foundOrdinal == UnmappedOrdinal)
                {
                    // Column not found or type incompatible - this mapping may not be suitable for this schema
                    ordinals[i] = UnmappedOrdinal;
                    isValid = false;
                }
                else
                {
                    ordinals[i] = foundOrdinal;
                }
            }

            return new OrdinalMapping(ordinals, isValid);
        }

        private static bool IsTypeCompatible(Type fieldType, Type expectedType)
        {
            if (fieldType == expectedType)
                return true;

            var underlyingExpected = Nullable.GetUnderlyingType(expectedType) ?? expectedType;
            var underlyingField = Nullable.GetUnderlyingType(fieldType) ?? fieldType;

            if (underlyingField == underlyingExpected)
                return true;

            // Allow numeric conversions (e.g., Int64 field for Int32 property)
            if (IsNumericType(underlyingField) && IsNumericType(underlyingExpected))
                return true;

            // Allow object type (can be converted)
            if (underlyingField == typeof(object))
                return true;

            // Allow specific safe conversions from string to non-string types
            if (underlyingField == typeof(string))
            {
                // Only allow string to these specific types that can handle string conversion safely
                return underlyingExpected == typeof(Guid) ||
                       underlyingExpected == typeof(DateTime) ||
                       underlyingExpected == typeof(DateTimeOffset) ||
                       underlyingExpected == typeof(TimeSpan) ||
                       underlyingExpected == typeof(DateOnly) ||   // MM1: SQLite stores DateOnly as TEXT
                       underlyingExpected == typeof(TimeOnly) ||   // MM1: SQLite stores TimeOnly as TEXT
                       underlyingExpected == typeof(bool); // SQLite stores bools as strings sometimes
            }

            return false;
        }

        private static bool IsNumericType(Type type)
        {
            return type == typeof(byte) || type == typeof(sbyte) ||
                   type == typeof(short) || type == typeof(ushort) ||
                   type == typeof(int) || type == typeof(uint) ||
                   type == typeof(long) || type == typeof(ulong) ||
                   type == typeof(float) || type == typeof(double) ||
                   type == typeof(decimal);
        }
    }
}
