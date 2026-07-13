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
            var liveMap = liveColumns?
                .ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase)
                ?? new Dictionary<string, LiveColumnInfo>(0);

            var columns = string.Join(",\n    ", mapping.Columns.Select(c =>
            {
                if (liveMap.TryGetValue(c.Name, out var live))
                    return $"{Escape(c.Name)} {live.SqlType}{(live.IsNullable ? "" : " NOT NULL")}";
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
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columns = string.Join(", ", mapping.Columns.Select(c => Escape(c.Name)));
            var newColumns = string.Join(", ", mapping.Columns.Select(c => "NEW." + Escape(c.Name)));
            var oldColumns = string.Join(", ", mapping.Columns.Select(c => "OLD." + Escape(c.Name)));
            var keyCondition = string.Join(" AND ", mapping.KeyColumns.Select(c => $"{Escape(c.Name)} = OLD.{Escape(c.Name)}"));
            // Scope the history close to the same tenant (SEC-MT): the history table is not PK-unique
            // (its key includes the validity range), so matching on the entity key alone could close a
            // different tenant's open row. No-op when the tenant column is already part of the key.
            if (mapping.TenantColumn is { } tc && !mapping.KeyColumns.Any(k => k.Name == tc.Name))
                keyCondition += $" AND {Escape(tc.Name)} = OLD.{Escape(tc.Name)}";

            return $@"
DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_ai")};
-- DELIMITER
CREATE TRIGGER {Escape(mapping.TableName + "_ai")} AFTER INSERT ON {table}
FOR EACH ROW
BEGIN
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(6), '9999-12-31', 'I', {newColumns});
END;
-- DELIMITER
DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_au")};
-- DELIMITER
CREATE TRIGGER {Escape(mapping.TableName + "_au")} AFTER UPDATE ON {table}
FOR EACH ROW
BEGIN
    UPDATE {history} SET `__ValidTo` = UTC_TIMESTAMP(6) WHERE `__ValidTo` = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(6), '9999-12-31', 'U', {newColumns});
END;
-- DELIMITER
DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_ad")};
-- DELIMITER
CREATE TRIGGER {Escape(mapping.TableName + "_ad")} AFTER DELETE ON {table}
FOR EACH ROW
BEGIN
    UPDATE {history} SET `__ValidTo` = UTC_TIMESTAMP(6) WHERE `__ValidTo` = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (`__ValidFrom`, `__ValidTo`, `__Operation`, {columns})
    VALUES (UTC_TIMESTAMP(6), UTC_TIMESTAMP(6), 'D', {oldColumns});
END;";
        }
    }
}
