using System;
using System.Collections.Generic;
using System.Linq;

namespace nORM.Migration
{
    public partial class SqliteMigrationSqlGenerator
    {
        private static void ValidateSqliteIdentityMetadata(SchemaDiff diff)
        {
            foreach (var table in EnumerateTablesForIdentityValidation(diff))
                ValidateSqliteIdentityColumns(table.Name, table.Columns);

            foreach (var group in diff.AddedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                ValidateSqliteIdentityColumns(table.Name, MergeColumnsForIdentityValidation(table.Columns, group.Select(static item => item.Column)));
            }

            foreach (var group in diff.DroppedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                ValidateSqliteIdentityColumns(table.Name, MergeColumnsForIdentityValidation(table.Columns, group.Select(static item => item.Column)));
            }

            foreach (var group in diff.AlteredColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var oldColumnsByName = group.ToDictionary(static item => item.NewColumn.Name, static item => item.OldColumn, StringComparer.OrdinalIgnoreCase);
                var oldColumns = table.Columns
                    .Select(column => oldColumnsByName.TryGetValue(column.Name, out var oldColumn) ? oldColumn : column)
                    .ToArray();
                ValidateSqliteIdentityColumns(table.Name, oldColumns);
            }
        }

        private static IEnumerable<TableSchema> EnumerateTablesForIdentityValidation(SchemaDiff diff)
        {
            foreach (var table in diff.AddedTables)
                yield return table;
            foreach (var table in diff.DroppedTables)
                yield return table;
            foreach (var (table, _) in diff.AddedColumns)
                yield return table;
            foreach (var (table, _) in diff.DroppedColumns)
                yield return table;
            foreach (var (table, _, _) in diff.AlteredColumns)
                yield return table;
            foreach (var (table, _) in diff.AddedForeignKeys)
                yield return table;
            foreach (var (table, _) in diff.DroppedForeignKeys)
                yield return table;
            foreach (var (table, _) in diff.AddedCheckConstraints)
                yield return table;
            foreach (var (table, _) in diff.DroppedCheckConstraints)
                yield return table;
            foreach (var (table, _) in diff.AddedExpressionIndexes)
                yield return table;
            foreach (var (table, _) in diff.DroppedExpressionIndexes)
                yield return table;
            foreach (var (table, _, _, _, _) in diff.AddedIndexes)
                yield return table;
            foreach (var (table, _) in diff.DroppedIndexes)
                yield return table;
        }

        private static IReadOnlyList<ColumnSchema> MergeColumnsForIdentityValidation(
            IEnumerable<ColumnSchema> tableColumns,
            IEnumerable<ColumnSchema> operationColumns)
        {
            var columns = tableColumns.ToDictionary(static column => column.Name, StringComparer.OrdinalIgnoreCase);
            foreach (var column in operationColumns)
                columns[column.Name] = column;
            return columns.Values.ToArray();
        }

        private static void ValidateSqliteIdentityColumns(string tableName, IReadOnlyList<ColumnSchema> columns)
        {
            var identityColumns = columns.Where(static column => column.IsIdentity).ToArray();
            if (identityColumns.Length == 0)
                return;

            if (identityColumns.Length > 1)
                throw new NotSupportedException($"SQLite supports at most one identity column per table. Table '{tableName}' has identity metadata on: {string.Join(", ", identityColumns.Select(static column => column.Name))}.");

            var identityColumn = identityColumns[0];
            var primaryKeyColumns = columns.Where(static column => column.IsPrimaryKey).ToArray();
            if (!identityColumn.IsPrimaryKey)
                throw new NotSupportedException($"SQLite identity column '{tableName}.{identityColumn.Name}' must also be the single primary key column; otherwise AUTOINCREMENT would be ignored.");

            if (primaryKeyColumns.Length != 1)
                throw new NotSupportedException($"SQLite identity column '{tableName}.{identityColumn.Name}' cannot be part of a composite primary key because AUTOINCREMENT requires a single INTEGER PRIMARY KEY column.");

            if (!IsSqliteIntegerIdentityType(identityColumn))
                throw new NotSupportedException($"SQLite identity column '{tableName}.{identityColumn.Name}' must map to INTEGER PRIMARY KEY. CLR type '{identityColumn.ClrType}' maps to '{GetSqlType(identityColumn)}'.");

            if (identityColumn.IdentitySeed is not null || identityColumn.IdentityIncrement is not null)
                throw new NotSupportedException($"SQLite identity column '{tableName}.{identityColumn.Name}' cannot use identity seed or increment options.");

            if (!string.IsNullOrWhiteSpace(identityColumn.DefaultValue))
                throw new NotSupportedException($"SQLite identity column '{tableName}.{identityColumn.Name}' cannot also define a default value.");
        }

        private static bool IsSqliteIntegerIdentityType(ColumnSchema column)
            => !string.Equals(column.ClrType, typeof(bool).FullName, StringComparison.Ordinal)
               && string.Equals(GetSqlType(column), "INTEGER", StringComparison.OrdinalIgnoreCase);
    }
}
