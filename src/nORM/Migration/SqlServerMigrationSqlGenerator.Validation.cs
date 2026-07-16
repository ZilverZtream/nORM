using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Configuration;

namespace nORM.Migration
{
    public partial class SqlServerMigrationSqlGenerator
    {
        private static bool IsComputedColumn(ColumnSchema column) => column.ComputedColumnSql is not null;

        private static string FormatCollation(ColumnSchema column)
            => string.IsNullOrWhiteSpace(column.Collation)
                ? string.Empty
                : $" COLLATE {ValidateCollationIdentifier(column.Collation)}";

        private static string ValidateCollationIdentifier(string collation)
        {
            var value = collation.Trim();
            if (value.Length == 0)
                throw new ArgumentException("Collation cannot be empty.", nameof(collation));

            foreach (var ch in value)
            {
                if (!char.IsLetterOrDigit(ch) && ch != '_')
                    throw new ArgumentException($"Collation '{collation}' contains unsupported characters.");
            }

            return value;
        }

        private static string BuildComputedColumnDefinition(ColumnSchema column)
        {
            if (string.IsNullOrWhiteSpace(column.ComputedColumnSql))
                throw new NotSupportedException($"Computed column '{column.Name}' requires ComputedColumnSql for SQL Server migration generation.");
            var persisted = column.IsStoredComputedColumn ? " PERSISTED" : string.Empty;
            return $"{Esc(column.Name)} AS ({FormatCheckPredicate(column.ComputedColumnSql)}){persisted}";
        }

        // M1/X1: Allowlist for FK referential action tokens.
        // NOTE: Identical copy exists in the other three migration generators. If a shared base class is introduced, consolidate here.
        private static readonly HashSet<string> _validFkActions =
            new(StringComparer.OrdinalIgnoreCase) { "NO ACTION", "CASCADE", "SET NULL", "RESTRICT", "SET DEFAULT" };

        private static string ValidateFkAction(string action, string constraintName)
        {
            ArgumentNullException.ThrowIfNull(action);
            if (!_validFkActions.Contains(action))
                throw new ArgumentException(
                    $"Invalid FK referential action '{action}' in constraint '{constraintName}'. " +
                    "Allowed values: NO ACTION, CASCADE, SET NULL, RESTRICT, SET DEFAULT.");
            return action;
        }

        private static string FormatIndexColumns(string[] columnNames, bool[] descending, IndexNullSortOrder[] _)
            => string.Join(", ", columnNames.Select((name, index) =>
                Esc(name) + (index < descending.Length && descending[index] ? " DESC" : string.Empty)));

        private static string FormatIncludedColumns(string[] includedColumnNames)
            => includedColumnNames.Length == 0
                ? string.Empty
                : " INCLUDE (" + string.Join(", ", includedColumnNames.Select(Esc)) + ")";

        private static void EnsureNoNullsNotDistinct(bool nullsNotDistinct, string indexName)
        {
            if (nullsNotDistinct)
                throw new NotSupportedException($"SQL Server does not support PostgreSQL NULLS NOT DISTINCT semantics for index '{indexName}'. Keep that unique-index behavior in PostgreSQL-specific migration code.");
        }

        private static void EnsureNoNullSortOrders(IndexNullSortOrder[] nullSortOrders, string indexName)
        {
            if (nullSortOrders.Any(static order => order != IndexNullSortOrder.Default))
                throw new NotSupportedException($"SQL Server does not support provider-neutral NULLS FIRST/LAST index ordering for index '{indexName}'. Keep that ordering in provider-specific migration code.");
        }

        private static string FormatFilter(string? filterSql)
            => string.IsNullOrWhiteSpace(filterSql) ? string.Empty : " WHERE " + filterSql.Trim();

        private static string FormatIdentityPart(ColumnSchema column)
        {
            if (!column.IsIdentity)
                return string.Empty;

            var seed = column.IdentitySeed ?? 1;
            var increment = column.IdentityIncrement ?? 1;
            if (increment == 0)
                throw new InvalidOperationException($"Identity increment for column '{column.Name}' cannot be zero.");

            return $" IDENTITY({seed.ToString(System.Globalization.CultureInfo.InvariantCulture)},{increment.ToString(System.Globalization.CultureInfo.InvariantCulture)})";
        }

        private static void EnsureNoExpressionIndexes(IReadOnlyList<ExpressionIndexSchema> expressionIndexes, string tableName)
        {
            if (expressionIndexes.Count > 0)
                throw new NotSupportedException($"SQL Server does not support direct expression indexes on table '{tableName}'. Use computed columns plus normal indexes.");
        }

        /// <summary>
        /// Builds the inline FOREIGN KEY constraint SQL fragment for a CREATE TABLE or ALTER TABLE statement.
        /// </summary>
        private static string BuildFkConstraintSql(ForeignKeySchema fk)
        {
            ArgumentNullException.ThrowIfNull(fk);
            var depCols = string.Join(", ", fk.DependentColumns.Select(Esc));
            var refCols = string.Join(", ", fk.PrincipalColumns.Select(Esc));
            var onDelete = ValidateFkAction(fk.OnDelete, fk.ConstraintName);
            var onUpdate = ValidateFkAction(fk.OnUpdate, fk.ConstraintName);
            var sql = $"CONSTRAINT {Esc(fk.ConstraintName)} FOREIGN KEY ({depCols}) REFERENCES {EscTable(fk.PrincipalTable)}({refCols})";
            if (!string.Equals(onDelete, "NO ACTION", StringComparison.OrdinalIgnoreCase))
                sql += $" ON DELETE {onDelete}";
            if (!string.Equals(onUpdate, "NO ACTION", StringComparison.OrdinalIgnoreCase))
                sql += $" ON UPDATE {onUpdate}";
            return sql;
        }

        private static string BuildCheckConstraintSql(CheckConstraintSchema check)
        {
            ArgumentNullException.ThrowIfNull(check);
            return $"CONSTRAINT {Esc(check.ConstraintName)} CHECK ({FormatCheckPredicate(check.Sql)})";
        }

        private static string FormatCheckPredicate(string sql)
        {
            ArgumentNullException.ThrowIfNull(sql);
            var trimmed = sql.Trim();
            if (trimmed.StartsWith("CHECK", StringComparison.OrdinalIgnoreCase))
            {
                var open = trimmed.IndexOf('(');
                var close = trimmed.LastIndexOf(')');
                if (open >= 0 && close > open)
                    trimmed = trimmed.Substring(open + 1, close - open - 1).Trim();
            }
            return trimmed;
        }

        // NOTE: identical copies of ResolveType and ValidateFkAction exist in the other three generators;
        // consolidate into a shared base class or static helper if one is introduced in the future.

        // Cache for ResolveType to avoid scanning all loaded assemblies on every call.
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<string, Type?> _resolveTypeCache
            = new(StringComparer.Ordinal);

        // Resolve a CLR type by its full name, scanning loaded assemblies when Type.GetType fails.
        // Results are cached in _resolveTypeCache to avoid repeated AppDomain scans.
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Type resolution by assembly-qualified name is not trim-safe; the target type may be removed by the linker.")]
        private static Type? ResolveType(string typeName)
        {
            if (string.IsNullOrEmpty(typeName))
                return null;
            return _resolveTypeCache.GetOrAdd(typeName, static name =>
            {
                var t = Type.GetType(name);
                if (t != null) return t;
                foreach (var asm in AppDomain.CurrentDomain.GetAssemblies())
                {
                    t = asm.GetType(name);
                    if (t != null) return t;
                }
                return null;
            });
        }
    }
}
