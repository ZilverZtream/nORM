#nullable enable
using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Scaffolding
{
    internal static partial class ScaffoldSqliteDdlParser
    {
        public static IReadOnlyDictionary<string, string> ExtractForeignKeyProviderSemanticsByColumns(string? createTableSql)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var part in SplitCreateTableBodyParts(createTableSql))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0)
                    continue;

                if (!TryReadForeignKeyClause(trimmed, out _, out var dependentColumns, out var semanticTail))
                    continue;

                var semantics = ExtractForeignKeyProviderSemantics(semanticTail);
                if (!string.IsNullOrWhiteSpace(semantics))
                    result[BuildForeignKeyColumnKey(dependentColumns)] = semantics;
            }

            return result;
        }

        public static IReadOnlyDictionary<string, string> ExtractForeignKeyConstraintNamesByColumns(string? createTableSql)
        {
            var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

            foreach (var part in SplitCreateTableBodyParts(createTableSql))
            {
                var trimmed = part.Trim();
                if (trimmed.Length == 0
                    || !TryReadForeignKeyClause(trimmed, out var constraintName, out var dependentColumns, out _)
                    || string.IsNullOrWhiteSpace(constraintName))
                {
                    continue;
                }

                result[BuildForeignKeyColumnKey(dependentColumns)] = constraintName;
            }

            return result;
        }

        public static string BuildForeignKeyColumnKey(IEnumerable<string> dependentColumns)
            => BuildColumnListKey(dependentColumns);

        public static string BuildColumnListKey(IEnumerable<string> columns)
            => string.Join("\u001f", columns.Select(static column => column.Trim()));
    }
}
