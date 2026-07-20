using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using nORM.Configuration;

namespace nORM.Migration
{
    /// <summary>
    /// Generates SQL Server compatible migration scripts based on a schema diff.
    /// </summary>
    /// <remarks>
    /// Provider: Microsoft SQL Server (T-SQL dialect). Uses bracket-escaped identifiers,
    /// IDENTITY(1,1) for auto-increment columns, and dynamic T-SQL for DEFAULT constraint
    /// discovery and removal. FK referential actions are validated against an allowlist
    /// before being interpolated into DDL.
    /// </remarks>
    public partial class SqlServerMigrationSqlGenerator : IMigrationSqlGenerator
    {
        /// <summary>Maximum length for NVARCHAR variable used in dynamic default-constraint lookup.</summary>
        private const int ConstraintNameVarMaxLength = 200;

        /// <summary>Maximum SQL Server length for bounded NVARCHAR before NVARCHAR(MAX) is required.</summary>
        private const int MaxBoundedNVarCharLength = 4000;

        /// <summary>Maximum SQL Server length for bounded VARCHAR before VARCHAR(MAX) is required.</summary>
        private const int MaxBoundedVarCharLength = 8000;

        /// <summary>Maximum SQL Server length for bounded VARBINARY before VARBINARY(MAX) is required.</summary>
        private const int MaxBoundedVarBinaryLength = 8000;

        /// <summary>Default SQL Server fallback type for unmapped CLR types.</summary>
        private const string FallbackSqlType = "NVARCHAR(MAX)";

        private static readonly Dictionary<string, string> TypeMap = new()
        {
            { typeof(int).FullName!, "INT" },
            { typeof(long).FullName!, "BIGINT" },
            { typeof(short).FullName!, "SMALLINT" },
            { typeof(byte).FullName!, "TINYINT" },
            { typeof(bool).FullName!, "BIT" },
            { typeof(string).FullName!, "NVARCHAR(MAX)" },
            { typeof(DateTime).FullName!, "DATETIME2" },
            { typeof(decimal).FullName!, "DECIMAL(18,2)" },
            { typeof(double).FullName!, "FLOAT" },
            { typeof(float).FullName!, "REAL" },
            { typeof(Guid).FullName!, "UNIQUEIDENTIFIER" },
            // X2: expanded type map
            { typeof(byte[]).FullName!, "VARBINARY(MAX)" },
            { typeof(DateOnly).FullName!, "DATE" },
            { typeof(TimeOnly).FullName!, "TIME" },
            { typeof(DateTimeOffset).FullName!, "DATETIMEOFFSET" },
            { typeof(TimeSpan).FullName!, "TIME" },
            { typeof(char).FullName!, "NCHAR(1)" },
            { typeof(sbyte).FullName!, "SMALLINT" },
            { typeof(ushort).FullName!, "INT" },
            { typeof(uint).FullName!, "BIGINT" },
            { typeof(ulong).FullName!, "DECIMAL(20,0)" }
        };

        // Escape SQL Server identifiers to prevent SQL injection via identifier names.
        private static string Esc(string id)
        {
            ArgumentNullException.ThrowIfNull(id);
            return $"[{id.Replace("]", "]]")}]";
        }

        private static string EscTable(string id)
        {
            ArgumentNullException.ThrowIfNull(id);
            return string.Join(".", id.Split('.').Select(Esc));
        }

        private static string? GetSchemaName(string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);
            var dot = tableName.IndexOf('.');
            return dot <= 0 ? null : tableName[..dot];
        }

        private static IReadOnlyList<string> GetAddedTableSchemas(IEnumerable<TableSchema> tables)
            => tables.Select(table => GetSchemaName(table.Name))
                .Where(static schema => !string.IsNullOrWhiteSpace(schema))
                .Cast<string>()
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(static schema => schema, StringComparer.OrdinalIgnoreCase)
                .ToArray();

        // Escape a value interpolated inside a SQL single-quoted string literal.
        private static string EscLiteral(string s)
        {
            ArgumentNullException.ThrowIfNull(s);
            return s.Replace("'", "''");
        }

        /// <summary>
        /// Builds a SQL Server column comment as an <c>sp_addextendedproperty</c> call registering an
        /// <c>MS_Description</c> at the SCHEMA/TABLE/COLUMN levels — SQL Server's native column comment
        /// mechanism. Every interpolated value is emitted as an <c>N'...'</c> literal with quotes doubled so
        /// neither the comment text nor the identifiers can break out of the literal.
        /// </summary>
        private static string BuildColumnCommentSql(TableSchema table, ColumnSchema column)
        {
            var dot = table.Name.IndexOf('.');
            var schema = dot > 0 ? table.Name[..dot] : "dbo";
            var bareTable = dot > 0 ? table.Name[(dot + 1)..] : table.Name;
            return $"EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'{EscLiteral(column.Comment!)}', " +
                   $"@level0type=N'SCHEMA', @level0name=N'{EscLiteral(schema)}', " +
                   $"@level1type=N'TABLE', @level1name=N'{EscLiteral(bareTable)}', " +
                   $"@level2type=N'COLUMN', @level2name=N'{EscLiteral(column.Name)}'";
        }

        private static (string IndexName, bool IsUnique, string[] ColumnNames, bool[] Descending, IndexNullSortOrder[] NullSortOrders, string[] IncludedColumnNames, bool NullsNotDistinct, string? FilterSql) ResolveIndex(
            TableSchema table,
            string indexName,
            bool isUnique,
            string[] columnNames,
            bool[] descending)
        {
            foreach (var index in SchemaDiffer.GetExplicitIndexes(table))
            {
                if (string.Equals(index.IndexName, indexName, StringComparison.OrdinalIgnoreCase))
                    return index;
            }

            return (indexName, isUnique, columnNames, descending, Array.Empty<IndexNullSortOrder>(), Array.Empty<string>(), false, null);
        }

        private static string BuildIndexSql(TableSchema table, string indexName, bool isUnique, string[] columnNames, bool[] descending)
        {
            var index = ResolveIndex(table, indexName, isUnique, columnNames, descending);
            EnsureNoNullsNotDistinct(index.NullsNotDistinct, index.IndexName);
            EnsureNoNullSortOrders(index.NullSortOrders, index.IndexName);
            var unique = index.IsUnique ? "UNIQUE " : string.Empty;
            return $"CREATE {unique}INDEX {Esc(index.IndexName)} ON {EscTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)}){FormatIncludedColumns(index.IncludedColumnNames)}{FormatFilter(index.FilterSql)}";
        }

        private static bool IsImplicitUniqueColumn(ColumnSchema column)
            => column.IsUnique
               && !column.IsPrimaryKey
               && string.IsNullOrWhiteSpace(column.IndexName)
               && column.Indexes.Count == 0;

        private static string GetPrimaryKeyConstraintName(TableSchema table, IReadOnlyList<ColumnSchema> pkCols)
            => pkCols.FirstOrDefault(static c => c.IsPrimaryKey && !string.IsNullOrWhiteSpace(c.IndexName))?.IndexName
               ?? $"PK_{table.Name}";

        private static string GetUniqueConstraintName(TableSchema table, ColumnSchema column)
            => $"UQ_{table.Name}_{column.Name}";

        private static string GetDefaultConstraintName(TableSchema table, ColumnSchema column)
            => !string.IsNullOrWhiteSpace(column.DefaultConstraintName)
                ? column.DefaultConstraintName!
                : $"DF_{table.Name}_{column.Name}";

        private static string BuildPrimaryKeyConstraintSql(TableSchema table, IReadOnlyList<ColumnSchema> pkCols)
            => $"CONSTRAINT {Esc(GetPrimaryKeyConstraintName(table, pkCols))} PRIMARY KEY ({string.Join(", ", pkCols.Select(c => Esc(c.Name)))})";

        private static string BuildUniqueConstraintSql(TableSchema table, ColumnSchema column)
            => $"CONSTRAINT {Esc(GetUniqueConstraintName(table, column))} UNIQUE ({Esc(column.Name)})";

        private static string FormatInlineDefaultClause(TableSchema table, ColumnSchema column)
        {
            if (string.IsNullOrEmpty(column.DefaultValue))
                return string.Empty;

            var value = DefaultValueValidator.Validate(column.DefaultValue);
            return !string.IsNullOrWhiteSpace(column.DefaultConstraintName)
                ? $" CONSTRAINT {Esc(GetDefaultConstraintName(table, column))} DEFAULT ({value})"
                : $" DEFAULT {value}";
        }

        private static string BuildAddDefaultConstraintSql(TableSchema table, ColumnSchema column)
            => $"ALTER TABLE {EscTable(table.Name)} ADD CONSTRAINT {Esc(GetDefaultConstraintName(table, column))} DEFAULT ({DefaultValueValidator.Validate(column.DefaultValue)}) FOR {Esc(column.Name)}";

        private static bool DefaultBindingChanged(ColumnSchema oldColumn, ColumnSchema newColumn)
            => !string.Equals(oldColumn.DefaultValue, newColumn.DefaultValue, StringComparison.Ordinal)
               || !string.Equals(oldColumn.DefaultConstraintName, newColumn.DefaultConstraintName, StringComparison.OrdinalIgnoreCase);

        private static void ValidateUnsupportedIdentityOperations(SchemaDiff diff)
        {
            foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
            {
                if (oldCol.IsIdentity != newCol.IsIdentity
                    || oldCol.IdentitySeed != newCol.IdentitySeed
                    || oldCol.IdentityIncrement != newCol.IdentityIncrement)
                    throw new NotSupportedException(
                        $"Changing identity metadata for column '{newCol.Name}' on table '{table.Name}' is not supported by SQL Server migration generation. " +
                        "Drop and recreate the table or use a provider-specific custom migration.");
            }

            foreach (var (table, column) in diff.AddedColumns)
            {
                if (column.IsIdentity)
                    throw new NotSupportedException(
                        $"Adding identity column '{column.Name}' to existing table '{table.Name}' is not supported by SQL Server migration generation. " +
                        "Create a new table or use a provider-specific custom migration.");
            }

            foreach (var (table, column) in diff.DroppedColumns)
            {
                if (column.IsIdentity)
                    throw new NotSupportedException(
                        $"Dropping identity column '{column.Name}' from table '{table.Name}' is not supported by SQL Server migration generation because the Down migration cannot restore identity metadata safely. " +
                        "Drop and recreate the table or use a provider-specific custom migration.");
            }
        }

        /// <summary>
        /// Generates SQL Server specific statements for applying the provided schema changes.
        /// </summary>
        /// <param name="diff">The computed differences between the current and desired schema.</param>
        /// <returns>The SQL statements to upgrade and downgrade the database schema.</returns>
        public MigrationSqlStatements GenerateSql(SchemaDiff diff)
        {
            ArgumentNullException.ThrowIfNull(diff);
            ValidateUnsupportedIdentityOperations(diff);

            var up = new List<string>();
            var down = new List<string>();
            var primaryKeyChanges = SchemaDiffer.GetPrimaryKeyChanges(diff);

            // â”€ UP: correct DDL dependency ordering â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
            // FK constraints must be dropped BEFORE the columns/tables they reference
            // are removed. Symmetric rule for DOWN: FK constraints added in UP must be
            // dropped BEFORE the columns/tables added in UP are removed.

            foreach (var schema in GetAddedTableSchemas(diff.AddedTables))
                up.Add($"IF SCHEMA_ID(N'{EscLiteral(schema)}') IS NULL EXEC(N'CREATE SCHEMA {Esc(schema).Replace("'", "''")}')");

            // UP-1: Drop FK constraints first (before columns/tables they depend on).
            foreach (var (table, fk) in diff.DroppedForeignKeys)
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(fk.ConstraintName)}");
            foreach (var (table, check) in diff.DroppedCheckConstraints)
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(check.ConstraintName)}");
            foreach (var (table, indexName) in diff.DroppedIndexes)
                up.Add($"DROP INDEX {Esc(indexName)} ON {EscTable(table.Name)}");

            // UP-2: Drop tables.
            foreach (var table in diff.DroppedTables)
                up.Add($"DROP TABLE {EscTable(table.Name)}");

            // UP-3: Drop columns (safe â€” FKs on those columns are removed in UP-1).
            // E: Before dropping a column, find and drop any DEFAULT constraint on it first.
            foreach (var group in diff.DroppedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var pkCols = table.Columns.Where(static c => c.IsPrimaryKey).ToArray();
                if (group.Any(static item => item.Column.IsPrimaryKey) && pkCols.Length > 0)
                    up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetPrimaryKeyConstraintName(table, pkCols))}");
                foreach (var column in group.Select(static item => item.Column).Where(IsImplicitUniqueColumn))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetUniqueConstraintName(table, column))}");
            }
            foreach (var (table, column) in diff.DroppedColumns)
            {
                var dropVar = $"@__drop_{new string(column.Name.Where(char.IsLetterOrDigit).ToArray()).ToUpperInvariant()}";
                // Truncate to a safe length for a T-SQL variable name (max 128 chars including @).
                if (dropVar.Length > 128) dropVar = dropVar.Substring(0, 128);
                up.Add($"DECLARE {dropVar} NVARCHAR({ConstraintNameVarMaxLength}) = (SELECT name FROM sys.default_constraints WHERE parent_object_id=OBJECT_ID('{EscLiteral(table.Name)}') AND COL_NAME(parent_object_id,parent_column_id)='{EscLiteral(column.Name)}') IF {dropVar} IS NOT NULL EXEC('ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT ['+{dropVar}+']')");
                up.Add($"ALTER TABLE {EscTable(table.Name)} DROP COLUMN {Esc(column.Name)}");
            }

            // UP-3b: Rename columns BEFORE anything that references the new names â€”
            // altered-column statements and rebuilt indexes for a renamed column
            // reference the post-rename name, which must exist by then.
            foreach (var (table, oldColName, newCol) in diff.RenamedColumns)
                up.Add($"EXEC sp_rename '{EscLiteral(table.Name)}.{EscLiteral(oldColName)}', '{EscLiteral(newCol.Name)}', 'COLUMN'");

            // UP-4: Alter existing columns.
            {
                foreach (var (table, oldPkCols, _) in primaryKeyChanges.Where(static change => change.OldPrimaryKeyColumns.Length > 0))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetPrimaryKeyConstraintName(table, oldPkCols))}");

                int upAltIdx = 0;
                foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
                {
                    if (IsComputedColumn(newCol) || IsComputedColumn(oldCol))
                    {
                        up.Add($"ALTER TABLE {EscTable(table.Name)} DROP COLUMN {Esc(oldCol.Name)}");
                        if (IsComputedColumn(newCol))
                            up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildComputedColumnDefinition(newCol)}");
                        else
                            up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {Esc(newCol.Name)} {GetSqlType(newCol)}{FormatCollation(newCol)} {(newCol.IsNullable ? "NULL" : "NOT NULL")}");
                        upAltIdx++;
                        continue;
                    }

                    var columnDefinitionChanged = ColumnDefinitionChanged(oldCol, newCol);
                    var defaultChanged = DefaultBindingChanged(oldCol, newCol);
                    var needsDefaultRebind = defaultChanged || (columnDefinitionChanged && oldCol.DefaultValue != null);
                    if (IsImplicitUniqueColumn(oldCol) && !IsImplicitUniqueColumn(newCol))
                        up.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetUniqueConstraintName(table, oldCol))}");

                    // SQL Server binds defaults as constraints. Drop the existing constraint
                    // before ALTER COLUMN, then add the intended default back after the shape change.
                    if (needsDefaultRebind)
                    {
                        // D: use sequential index for a deterministic, stable T-SQL variable name.
                        var upVar = $"@__df_{upAltIdx}";
                        up.Add($"DECLARE {upVar} NVARCHAR({ConstraintNameVarMaxLength}) = (SELECT name FROM sys.default_constraints WHERE parent_object_id=OBJECT_ID('{EscLiteral(table.Name)}') AND COL_NAME(parent_object_id,parent_column_id)='{EscLiteral(newCol.Name)}') IF {upVar} IS NOT NULL EXEC('ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT ['+{upVar}+']')");
                    }
                    if (columnDefinitionChanged)
                    {
                        var newDef = $"{Esc(newCol.Name)} {GetSqlType(newCol)}{FormatCollation(newCol)} {(newCol.IsNullable ? "NULL" : "NOT NULL")}";
                        up.Add($"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {newDef}");
                    }
                    if (needsDefaultRebind && newCol.DefaultValue != null)
                        up.Add(BuildAddDefaultConstraintSql(table, newCol));
                    if (!IsImplicitUniqueColumn(oldCol) && IsImplicitUniqueColumn(newCol))
                        up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildUniqueConstraintSql(table, newCol)}");

                    upAltIdx++;
                }

                foreach (var (table, _, newPkCols) in primaryKeyChanges.Where(static change => change.NewPrimaryKeyColumns.Length > 0))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildPrimaryKeyConstraintSql(table, newPkCols)}");
            }

            // UP-5: Create new tables (including inline FK constraints).
            foreach (var table in diff.AddedTables)
            {
                EnsureNoExpressionIndexes(table.ExpressionIndexes, table.Name);
                var colDefs = table.Columns.Select(c =>
                {
                    if (IsComputedColumn(c))
                        return BuildComputedColumnDefinition(c);
                    var identityPart = FormatIdentityPart(c);
                    return $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)}{identityPart} {(c.IsNullable ? "NULL" : "NOT NULL")}{FormatInlineDefaultClause(table, c)}";
                }).ToList();

                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add(BuildPrimaryKeyConstraintSql(table, pkCols));

                foreach (var uc in table.Columns.Where(IsImplicitUniqueColumn))
                    colDefs.Add(BuildUniqueConstraintSql(table, uc));

                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));
                foreach (var check in table.CheckConstraints)
                    colDefs.Add(BuildCheckConstraintSql(check));

                up.Add($"CREATE TABLE {EscTable(table.Name)} ({string.Join(", ", colDefs)})");

                foreach (var index in SchemaDiffer.GetExplicitIndexes(table))
                {
                    var unique = index.IsUnique ? "UNIQUE " : string.Empty;
                    EnsureNoNullsNotDistinct(index.NullsNotDistinct, index.IndexName);
                    EnsureNoNullSortOrders(index.NullSortOrders, index.IndexName);
                    up.Add($"CREATE {unique}INDEX {Esc(index.IndexName)} ON {EscTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)}){FormatIncludedColumns(index.IncludedColumnNames)}{FormatFilter(index.FilterSql)}");
                }

                foreach (var c in table.Columns.Where(static c => !string.IsNullOrWhiteSpace(c.Comment)))
                    up.Add(BuildColumnCommentSql(table, c));
            }

            // UP-6: Add columns to existing tables.
            foreach (var (table, column) in diff.AddedColumns)
            {
                if (IsComputedColumn(column))
                {
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildComputedColumnDefinition(column)}");
                    continue;
                }

                if (!column.IsNullable && column.DefaultValue == null)
                    throw new InvalidOperationException(
                        $"Cannot generate ADD COLUMN '{column.Name}' NOT NULL on table '{table.Name}' without a DefaultValue. " +
                        "Set ColumnSchema.DefaultValue to a SQL literal or make the column nullable.");

                var nullPart = column.IsNullable ? "NULL" : "NOT NULL";
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} {nullPart}{FormatInlineDefaultClause(table, column)}";
                up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {colDef}");
                if (!string.IsNullOrWhiteSpace(column.Comment))
                    up.Add(BuildColumnCommentSql(table, column));
            }
            foreach (var group in diff.AddedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var pkCols = table.Columns.Where(static c => c.IsPrimaryKey).ToArray();
                if (group.Any(static item => item.Column.IsPrimaryKey) && pkCols.Length > 0)
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildPrimaryKeyConstraintSql(table, pkCols)}");
                foreach (var column in group.Select(static item => item.Column).Where(IsImplicitUniqueColumn))
                    up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildUniqueConstraintSql(table, column)}");
            }

            // UP-7: Add FK constraints last (all tables and columns are in place).
            foreach (var (table, check) in diff.AddedCheckConstraints)
                up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildCheckConstraintSql(check)}");
            foreach (var (table, indexName, isUnique, columnNames, descending) in diff.AddedIndexes)
                up.Add(BuildIndexSql(table, indexName, isUnique, columnNames, descending));
            foreach (var (table, fk) in diff.AddedForeignKeys)
                up.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildFkConstraintSql(fk)}");
            foreach (var (table, expressionIndex) in diff.AddedExpressionIndexes)
                throw new NotSupportedException($"SQL Server does not support direct expression index '{expressionIndex.Name}' on table '{table.Name}'. Use a computed column plus a normal index.");

            // â”€ DOWN: reverse of UP, with symmetric FK ordering â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

            // DOWN-1: Drop FK constraints that were added in UP-7 (before touching their columns).
            foreach (var (table, fk) in diff.AddedForeignKeys)
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(fk.ConstraintName)}");
            foreach (var (table, check) in diff.AddedCheckConstraints)
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(check.ConstraintName)}");
            foreach (var (table, indexName, _, _, _) in diff.AddedIndexes)
                down.Add($"DROP INDEX {Esc(indexName)} ON {EscTable(table.Name)}");

            // DOWN-2: Drop columns that were added in UP-6.
            foreach (var group in diff.AddedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var pkCols = table.Columns.Where(static c => c.IsPrimaryKey).ToArray();
                if (group.Any(static item => item.Column.IsPrimaryKey) && pkCols.Length > 0)
                    down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetPrimaryKeyConstraintName(table, pkCols))}");
                foreach (var column in group.Select(static item => item.Column).Where(IsImplicitUniqueColumn))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetUniqueConstraintName(table, column))}");
            }
            foreach (var (table, column) in diff.AddedColumns)
                down.Add($"ALTER TABLE {EscTable(table.Name)} DROP COLUMN {Esc(column.Name)}");

            // DOWN-3: Drop tables that were created in UP-5.
            foreach (var table in diff.AddedTables)
                down.Add($"DROP TABLE IF EXISTS {EscTable(table.Name)}");

            // DOWN-3b: Rename columns back BEFORE anything that references the old
            // names â€” alteration reverts and restored indexes use pre-rename names.
            foreach (var (table, oldColName, newCol) in diff.RenamedColumns)
                down.Add($"EXEC sp_rename '{EscLiteral(table.Name)}.{EscLiteral(newCol.Name)}', '{EscLiteral(oldColName)}', 'COLUMN'");

            // DOWN-4: Reverse column alterations from UP-4.
            {
                foreach (var (table, _, newPkCols) in primaryKeyChanges.Where(static change => change.NewPrimaryKeyColumns.Length > 0))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetPrimaryKeyConstraintName(table, newPkCols))}");

                int downAltIdx = 0;
                foreach (var (table, newCol, oldCol) in diff.AlteredColumns)
                {
                    if (IsComputedColumn(newCol) || IsComputedColumn(oldCol))
                    {
                        down.Add($"ALTER TABLE {EscTable(table.Name)} DROP COLUMN {Esc(newCol.Name)}");
                        if (IsComputedColumn(oldCol))
                            down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildComputedColumnDefinition(oldCol)}");
                        else
                            down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {Esc(oldCol.Name)} {GetSqlType(oldCol)}{FormatCollation(oldCol)} {(oldCol.IsNullable ? "NULL" : "NOT NULL")}");
                        downAltIdx++;
                        continue;
                    }

                    var columnDefinitionChanged = ColumnDefinitionChanged(oldCol, newCol);
                    var defaultChanged = DefaultBindingChanged(oldCol, newCol);
                    var needsDefaultRebind = defaultChanged || (columnDefinitionChanged && newCol.DefaultValue != null);
                    if (IsImplicitUniqueColumn(newCol) && !IsImplicitUniqueColumn(oldCol))
                        down.Add($"ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT {Esc(GetUniqueConstraintName(table, newCol))}");

                    if (needsDefaultRebind)
                    {
                        // D: use sequential index for a deterministic, stable T-SQL variable name.
                        var downVar = $"@__df_{downAltIdx}";
                        down.Add($"DECLARE {downVar} NVARCHAR({ConstraintNameVarMaxLength}) = (SELECT name FROM sys.default_constraints WHERE parent_object_id=OBJECT_ID('{EscLiteral(table.Name)}') AND COL_NAME(parent_object_id,parent_column_id)='{EscLiteral(oldCol.Name)}') IF {downVar} IS NOT NULL EXEC('ALTER TABLE {EscTable(table.Name)} DROP CONSTRAINT ['+{downVar}+']')");
                    }
                    if (columnDefinitionChanged)
                    {
                        var oldDef = $"{Esc(oldCol.Name)} {GetSqlType(oldCol)}{FormatCollation(oldCol)} {(oldCol.IsNullable ? "NULL" : "NOT NULL")}";
                        down.Add($"ALTER TABLE {EscTable(table.Name)} ALTER COLUMN {oldDef}");
                    }
                    if (needsDefaultRebind && oldCol.DefaultValue != null)
                        down.Add(BuildAddDefaultConstraintSql(table, oldCol));
                    if (!IsImplicitUniqueColumn(newCol) && IsImplicitUniqueColumn(oldCol))
                        down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildUniqueConstraintSql(table, oldCol)}");

                    downAltIdx++;
                }

                foreach (var (table, oldPkCols, _) in primaryKeyChanges.Where(static change => change.OldPrimaryKeyColumns.Length > 0))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildPrimaryKeyConstraintSql(table, oldPkCols)}");
            }

            // DOWN-5: Restore columns that were dropped in UP-3.
            // C: include DefaultValue in the column definition so NOT NULL restore doesn't fail.
            foreach (var (table, column) in diff.DroppedColumns)
            {
                if (IsComputedColumn(column))
                {
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildComputedColumnDefinition(column)}");
                    continue;
                }
                var colDef = $"{Esc(column.Name)} {GetSqlType(column)}{FormatCollation(column)} {(column.IsNullable ? "NULL" : "NOT NULL")}{FormatInlineDefaultClause(table, column)}";
                down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {colDef}");
            }
            foreach (var group in diff.DroppedColumns.GroupBy(static item => item.Table.Name, StringComparer.OrdinalIgnoreCase))
            {
                var table = group.First().Table;
                var pkCols = table.Columns.Where(static c => c.IsPrimaryKey).ToArray();
                if (group.Any(static item => item.Column.IsPrimaryKey) && pkCols.Length > 0)
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildPrimaryKeyConstraintSql(table, pkCols)}");
                foreach (var column in group.Select(static item => item.Column).Where(IsImplicitUniqueColumn))
                    down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildUniqueConstraintSql(table, column)}");
            }

            // DOWN-6: Restore tables that were dropped in UP-2.
            foreach (var table in diff.DroppedTables)
            {
                EnsureNoExpressionIndexes(table.ExpressionIndexes, table.Name);
                var colDefs = table.Columns.Select(c =>
                {
                    if (IsComputedColumn(c))
                        return BuildComputedColumnDefinition(c);
                    var identityPart = FormatIdentityPart(c);
                    return $"{Esc(c.Name)} {GetSqlType(c)}{FormatCollation(c)}{identityPart} {(c.IsNullable ? "NULL" : "NOT NULL")}{FormatInlineDefaultClause(table, c)}";
                }).ToList();
                var pkCols = table.Columns.Where(c => c.IsPrimaryKey).ToList();
                if (pkCols.Count > 0)
                    colDefs.Add(BuildPrimaryKeyConstraintSql(table, pkCols));
                foreach (var uc in table.Columns.Where(IsImplicitUniqueColumn))
                    colDefs.Add(BuildUniqueConstraintSql(table, uc));
                foreach (var fk in table.ForeignKeys)
                    colDefs.Add(BuildFkConstraintSql(fk));
                foreach (var check in table.CheckConstraints)
                    colDefs.Add(BuildCheckConstraintSql(check));
                down.Add($"CREATE TABLE {EscTable(table.Name)} ({string.Join(", ", colDefs)})");
                foreach (var index in SchemaDiffer.GetExplicitIndexes(table))
                {
                    var unique = index.IsUnique ? "UNIQUE " : string.Empty;
                    EnsureNoNullsNotDistinct(index.NullsNotDistinct, index.IndexName);
                    EnsureNoNullSortOrders(index.NullSortOrders, index.IndexName);
                    down.Add($"CREATE {unique}INDEX {Esc(index.IndexName)} ON {EscTable(table.Name)} ({FormatIndexColumns(index.ColumnNames, index.Descending, index.NullSortOrders)}){FormatIncludedColumns(index.IncludedColumnNames)}{FormatFilter(index.FilterSql)}");
                }
            }

            // DOWN-7: Restore FK constraints that were dropped in UP-1.
            foreach (var (table, check) in diff.DroppedCheckConstraints)
                down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildCheckConstraintSql(check)}");
            foreach (var (table, indexName, isUnique, columnNames, descending) in diff.DroppedIndexes
                .Select(droppedIndex =>
                {
                    var resolved = ResolveIndex(droppedIndex.Table, droppedIndex.IndexName, false, Array.Empty<string>(), Array.Empty<bool>());
                    return (droppedIndex.Table, resolved.IndexName, resolved.IsUnique, resolved.ColumnNames, resolved.Descending);
                }))
                down.Add(BuildIndexSql(table, indexName, isUnique, columnNames, descending));
            foreach (var (table, fk) in diff.DroppedForeignKeys)
                down.Add($"ALTER TABLE {EscTable(table.Name)} ADD {BuildFkConstraintSql(fk)}");
            foreach (var (table, expressionIndex) in diff.DroppedExpressionIndexes)
                throw new NotSupportedException($"SQL Server does not support direct expression index '{expressionIndex.Name}' on table '{table.Name}'. Use a computed column plus a normal index.");

            // Temporal companions LAST: history mirrors and trigger re-emission reference the main

            // table's post-change shape in each direction, established by the statements above.

            EmitTemporalDdl(up, down, diff);


            return new MigrationSqlStatements(up, down);
        }

        /// <summary>
        /// Determines the SQL Server column type corresponding to the supplied column schema.
        /// Types not present in the internal mapping fall back to <c>NVARCHAR(MAX)</c>.
        /// </summary>
        /// <param name="column">The column description including the CLR type.</param>
        /// <returns>The SQL Server data type name.</returns>
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026:RequiresUnreferencedCode",
            Justification = "ResolveType is only used for design-time enum-to-underlying-type mapping; the enum type should be present in the loaded assembly set.")]
        private static string GetSqlType(ColumnSchema column)
        {
            ArgumentNullException.ThrowIfNull(column);

            // An explicit HasColumnType (StoreType) overrides the CLR-derived type entirely.
            if (!string.IsNullOrEmpty(column.StoreType))
                return column.StoreType;

            if (TryGetDecimalWithPrecisionType(column, out var decimalSql))
                return decimalSql;

            if (TryGetBoundedStringOrBinaryType(column, out var boundedSql))
                return boundedSql;

            // X2: handle enum types by mapping to their underlying integral type
            if (!TypeMap.TryGetValue(column.ClrType, out var sql))
            {
                var clrType = ResolveType(column.ClrType);
                if (clrType != null && clrType.IsEnum)
                {
                    var underlying = Enum.GetUnderlyingType(clrType);
                    if (TypeMap.TryGetValue(underlying.FullName!, out sql))
                        return sql;
                }
                return FallbackSqlType;
            }
            return sql;
        }

        private static bool TryGetBoundedStringOrBinaryType(ColumnSchema column, out string sql)
        {
            sql = string.Empty;
            if (column.MaxLength is not > 0)
            {
                if (string.Equals(column.ClrType, typeof(string).FullName, StringComparison.Ordinal)
                    && column.IsUnicode == false
                    && !column.IsFixedLength)
                {
                    sql = "VARCHAR(MAX)";
                    return true;
                }

                return false;
            }

            if (string.Equals(column.ClrType, typeof(string).FullName, StringComparison.Ordinal))
            {
                var unicode = column.IsUnicode != false;
                var fixedLength = column.IsFixedLength;
                var boundedLimit = unicode ? MaxBoundedNVarCharLength : MaxBoundedVarCharLength;
                if (column.MaxLength.Value <= boundedLimit)
                {
                    var typeName = unicode
                        ? fixedLength ? "NCHAR" : "NVARCHAR"
                        : fixedLength ? "CHAR" : "VARCHAR";
                    sql = $"{typeName}({column.MaxLength.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)})";
                    return true;
                }

                if (!fixedLength)
                {
                    sql = unicode ? "NVARCHAR(MAX)" : "VARCHAR(MAX)";
                    return true;
                }

                return false;
            }

            if (string.Equals(column.ClrType, typeof(byte[]).FullName, StringComparison.Ordinal))
            {
                if (column.MaxLength.Value <= MaxBoundedVarBinaryLength)
                {
                    var typeName = column.IsFixedLength ? "BINARY" : "VARBINARY";
                    sql = $"{typeName}({column.MaxLength.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)})";
                    return true;
                }

                if (!column.IsFixedLength)
                {
                    sql = "VARBINARY(MAX)";
                    return true;
                }

                return false;
            }

            return false;
        }

        private static bool TryGetDecimalWithPrecisionType(ColumnSchema column, out string sql)
        {
            sql = string.Empty;
            if (!string.Equals(column.ClrType, typeof(decimal).FullName, StringComparison.Ordinal)
                || column.Precision is not > 0
                || (column.Scale.HasValue && (column.Scale.Value < 0 || column.Scale.Value > column.Precision.Value)))
            {
                return false;
            }

            var precision = column.Precision.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
            sql = column.Scale.HasValue
                ? $"DECIMAL({precision},{column.Scale.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)})"
                : $"DECIMAL({precision})";
            return true;
        }

        private static bool ColumnDefinitionChanged(ColumnSchema oldCol, ColumnSchema newCol) =>
            !string.Equals(oldCol.ClrType, newCol.ClrType, StringComparison.Ordinal)
            || !string.Equals(oldCol.StoreType, newCol.StoreType, StringComparison.OrdinalIgnoreCase)
            || oldCol.MaxLength != newCol.MaxLength
            || oldCol.IsUnicode != newCol.IsUnicode
            || oldCol.IsFixedLength != newCol.IsFixedLength
            || oldCol.Precision != newCol.Precision
            || oldCol.Scale != newCol.Scale
            || oldCol.IsNullable != newCol.IsNullable
            || !string.Equals(oldCol.Collation, newCol.Collation, StringComparison.OrdinalIgnoreCase)
            || !string.Equals(oldCol.ComputedColumnSql, newCol.ComputedColumnSql, StringComparison.OrdinalIgnoreCase)
            || oldCol.IsStoredComputedColumn != newCol.IsStoredComputedColumn;

    }
}
