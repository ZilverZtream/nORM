using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using System.Data.Common;
using System.Linq.Expressions;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class MySqlProvider
    {
        /// <summary>
        /// Introspects live column definitions via INFORMATION_SCHEMA.COLUMNS.
        /// Uses COLUMN_TYPE which already includes precision/length (e.g. decimal(10,4)).
        /// Returns empty list when the table does not yet exist.
        /// </summary>
        public override async Task<IReadOnlyList<LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbConnection conn, string tableName, CancellationToken ct = default)
        {
            var result = new List<LiveColumnInfo>();
            try
            {
                await using var cmd = conn.CreateCommand();
                var (schemaOverride, bareTable) = SplitSchemaTable(tableName);
                cmd.CommandText = schemaOverride != null
                    ? @"
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @t AND TABLE_SCHEMA = @s
ORDER BY ORDINAL_POSITION"
                    : @"
SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = @t AND TABLE_SCHEMA = DATABASE()
ORDER BY ORDINAL_POSITION";
                var p = cmd.CreateParameter(); p.ParameterName = "@t"; p.Value = bareTable; cmd.Parameters.Add(p);
                if (schemaOverride != null)
                {
                    var ps = cmd.CreateParameter(); ps.ParameterName = "@s"; ps.Value = schemaOverride; cmd.Parameters.Add(ps);
                }
                await using var rdr = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await rdr.ReadAsync(ct).ConfigureAwait(false))
                {
                    var name = rdr.GetString(0);
                    var sqlType = rdr.GetString(1);
                    var isNullable = rdr.GetString(2).Equals("YES", StringComparison.OrdinalIgnoreCase);
                    result.Add(new LiveColumnInfo(name, sqlType, isNullable));
                }
            }
            catch (DbException dbEx) when (IsObjectNotFoundError(dbEx))
            {
                // Table does not exist yet - return empty list.
            }
            return result;
        }

        /// <summary>
        /// Splits a possibly schema-qualified table name into its schema and table components.
        /// </summary>
        private static (string? Schema, string Table) SplitSchemaTable(string tableName)
        {
            var dot = tableName.IndexOf('.');
            if (dot < 0)
                return (null, tableName.Trim('`'));
            return (tableName[..dot].Trim('`'), tableName[(dot + 1)..].Trim('`'));
        }

        /// <summary>
        /// MySQL cannot use a TEXT column as a primary key without a prefix length.
        /// Use bounded VARCHAR/DATETIME columns for temporal tags.
        /// </summary>
        public override string GetCreateTagsTableSql()
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $@"CREATE TABLE IF NOT EXISTS {table} ({tagCol} VARCHAR(450) NOT NULL, {tsCol} DATETIME(6) NOT NULL, PRIMARY KEY ({tagCol})) ENGINE=InnoDB
-- DELIMITER
ALTER TABLE {table} MODIFY COLUMN {tsCol} DATETIME(6) NOT NULL";
        }

        /// <summary>
        /// Creates temporal tags using the MySQL server UTC clock so tag timestamps
        /// are comparable to trigger-generated history windows.
        /// </summary>
        public override string GetCreateTagSql(string pTagName, string pTimestamp)
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"INSERT INTO {table} ({tagCol}, {tsCol}) VALUES ({pTagName}, UTC_TIMESTAMP(6))";
        }

        internal override bool UsesDatabaseClockForTemporalTags => true;

        /// <summary>
        /// Generates the SQL statement to create the temporal history table for an entity.
        /// When liveColumns are supplied, column types are taken from the live DB schema.
        /// </summary>
        public override string GenerateCreateHistoryTableSql(
            TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null)
        {
            var historyTable = Escape(mapping.TableName + "_History");
            // Prefer the INTROSPECTED physical column set: the live table can carry columns
            // that exist only physically (an owned collection's FK, a raw ADD COLUMN), and a
            // history table missing them loses data. The mapping set is the offline fallback.
            var columns = liveColumns is { Count: > 0 }
                ? string.Join(",\n    ", liveColumns.Select(live =>
                    $"{Escape(live.Name)} {live.SqlType}{(live.IsNullable ? "" : " NOT NULL")}"))
                : string.Join(",\n    ", mapping.Columns.Select(c =>
                {
                    // History rows copy the main table's converter-encoded values, so the
                    // fallback types by the PROVIDER representation.
                    return $"{Escape(c.Name)} {GetSqlType(c.Converter?.ProviderType ?? c.Prop.PropertyType)}";
                }));

            return $@"
CREATE TABLE {historyTable} (
    `__VersionId` BIGINT AUTO_INCREMENT PRIMARY KEY,
    `__ValidFrom` DATETIME(6) NOT NULL,
    `__ValidTo` DATETIME(6) NOT NULL,
    `__Operation` CHAR(1) NOT NULL,
    {columns}
) ENGINE=InnoDB;";
        }

        /// <summary>
        /// Generates the SQL needed to create triggers that populate the temporal history table.
        /// </summary>
        public override string GenerateTemporalTriggersSql(TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null)
        {
            // DDL text lives in MySqlTemporalDdl, shared with the migration generator so a
            // migration that reshapes a temporal table re-emits identical triggers.
            var statements = MySqlTemporalDdl.BuildTriggerStatements(
                Escape,
                mapping.TableName,
                liveColumns is { Count: > 0 }
                    ? liveColumns.Select(c => c.Name).ToArray()
                    : mapping.Columns.Select(c => c.Name).ToArray(),
                mapping.KeyColumns.Select(c => c.Name).ToArray(),
                mapping.TenantColumn?.Name);
            return "\n" + string.Join("\n-- DELIMITER\n", statements);
        }
    }
}
