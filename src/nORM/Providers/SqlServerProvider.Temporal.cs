using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class SqlServerProvider
    {
        /// <summary>
        /// SQL Server does not support CREATE TABLE IF NOT EXISTS.
        /// Uses an OBJECT_ID check to create the tags table only when absent.
        /// Uses NVARCHAR (not TEXT) and DATETIME2 (not TEXT) for correct SQL Server types.
        /// </summary>
        public override string GetCreateTagsTableSql()
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $@"IF OBJECT_ID(N'__NormTemporalTags', N'U') IS NULL
CREATE TABLE {table} ({tagCol} NVARCHAR(450) NOT NULL, {tsCol} DATETIME2 NOT NULL, PRIMARY KEY ({tagCol}))";
        }

        /// <summary>
        /// Creates temporal tags using the SQL Server UTC database clock so tag
        /// timestamps are comparable to trigger-generated history windows.
        /// </summary>
        public override string GetCreateTagSql(string pTagName, string pTimestamp)
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"INSERT INTO {table} ({tagCol}, {tsCol}) VALUES ({pTagName}, SYSUTCDATETIME())";
        }

        internal override bool UsesDatabaseClockForTemporalTags => true;

        /// <summary>
        /// SQL Server uses SELECT TOP 1, not LIMIT 1.
        /// </summary>
        public override string GetHistoryTableExistsProbeSql(string escapedHistoryTable)
            => $"SELECT TOP 1 1 FROM {escapedHistoryTable}";

        /// <summary>
        /// SQL Server error 208 = "Invalid object name" = object/table does not exist.
        /// This is a definitive schema error, not a permission or connectivity failure.
        /// </summary>
        public override bool IsObjectNotFoundError(DbException ex)
            => ex is SqlException sqlEx && sqlEx.Number == SqlErrorObjectNotFound;

        /// <summary>
        /// Introspects live column definitions via INFORMATION_SCHEMA.COLUMNS.
        /// Reconstructs full type strings including precision/scale/length.
        /// Returns empty list when the table does not yet exist.
        /// </summary>
        public override async Task<IReadOnlyList<LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbConnection conn, string tableName, CancellationToken ct = default)
        {
            var result = new List<LiveColumnInfo>();
            try
            {
                await using var cmd = conn.CreateCommand();
                var (schema, bareTable) = SplitSchemaTable(tableName, "dbo");
                cmd.CommandText = @"
SELECT c.COLUMN_NAME, c.DATA_TYPE, c.CHARACTER_MAXIMUM_LENGTH,
       c.NUMERIC_PRECISION, c.NUMERIC_SCALE, c.IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS c
WHERE c.TABLE_NAME = @t AND c.TABLE_SCHEMA = @s
ORDER BY c.ORDINAL_POSITION";
                var p = cmd.CreateParameter(); p.ParameterName = "@t"; p.Value = bareTable; cmd.Parameters.Add(p);
                var ps = cmd.CreateParameter(); ps.ParameterName = "@s"; ps.Value = schema; cmd.Parameters.Add(ps);
                await using var rdr = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await rdr.ReadAsync(ct).ConfigureAwait(false))
                {
                    var name = rdr.GetString(0);
                    var dataType = rdr.GetString(1).ToLowerInvariant();
                    var charMax = rdr.IsDBNull(2) ? (int?)null : rdr.GetInt32(2);
                    var numPrec = rdr.IsDBNull(3) ? (byte?)null : rdr.GetByte(3);
                    var numScale = rdr.IsDBNull(4) ? (int?)null : rdr.GetInt32(4);
                    var isNullable = rdr.GetString(5).Equals("YES", StringComparison.OrdinalIgnoreCase);

                    var sqlType = dataType switch
                    {
                        "nvarchar" or "varchar" or "char" or "nchar" or "varbinary" or "binary" when charMax == -1 =>
                            $"{dataType}(max)",
                        "nvarchar" or "varchar" or "char" or "nchar" or "varbinary" or "binary" when charMax.HasValue =>
                            $"{dataType}({charMax})",
                        "nvarchar" or "varchar" or "char" or "nchar" or "varbinary" or "binary" =>
                            dataType, // null charMax: legacy types (text/ntext/image) or unknown length
                        "decimal" or "numeric" =>
                            (numPrec.HasValue && numScale.HasValue) ? $"{dataType}({numPrec},{numScale})" : dataType,
                        _ => dataType
                    };
                    result.Add(new LiveColumnInfo(name, sqlType, isNullable));
                }
            }
            catch (DbException dbEx) when (IsObjectNotFoundError(dbEx))
            {
                // Table does not exist yet -- return empty list.
            }
            return result;
        }

        private static (string Schema, string Table) SplitSchemaTable(string tableName, string defaultSchema)
        {
            var dot = tableName.IndexOf('.');
            if (dot < 0)
                return (defaultSchema, tableName.Trim('[', ']'));
            return (tableName[..dot].Trim('[', ']'), tableName[(dot + 1)..].Trim('[', ']'));
        }

        /// <summary>
        /// Generates SQL to create a history table used for temporal tracking.
        /// When liveColumns are supplied, column types are taken from the live DB schema.
        /// </summary>
        /// <param name="mapping">The mapping describing the entity table.</param>
        /// <param name="liveColumns">Live column info from the main table, or null to use CLR defaults.</param>
        /// <returns>DDL statement that creates the history table.</returns>
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
                    var sqlType = GetSqlType(c.Converter?.ProviderType ?? c.Prop.PropertyType);
                    return $"{Escape(c.Name)} {sqlType}";
                }));

            return $@"CREATE TABLE {historyTable} (
    {Escape("__VersionId")} BIGINT IDENTITY(1,1) PRIMARY KEY,
    {Escape("__ValidFrom")} DATETIME2 NOT NULL,
    {Escape("__ValidTo")} DATETIME2 NOT NULL,
    {Escape("__Operation")} CHAR(1) NOT NULL,
    {columns}
);";
        }

        /// <summary>
        /// Generates SQL Server triggers that maintain the temporal history table.
        /// </summary>
        /// <param name="mapping">The mapping describing the target table.</param>
        /// <param name="liveColumns">Live physical column info from the main table, or null to use the mapped set.</param>
        /// <returns>DDL statements creating the temporal triggers.</returns>
        public override string GenerateTemporalTriggersSql(TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null)
        {
            // DDL text lives in SqlServerTemporalDdl, shared with the migration generator so a
            // migration that reshapes a temporal table re-emits identical triggers.
            var statements = SqlServerTemporalDdl.BuildTriggerStatements(
                Escape,
                mapping.TableName,
                liveColumns is { Count: > 0 }
                    ? liveColumns.Select(c => c.Name).ToArray()
                    : mapping.Columns.Select(c => c.Name).ToArray(),
                mapping.KeyColumns.Select(c => c.Name).ToArray(),
                mapping.TenantColumn?.Name);
            // Each statement must run in its own batch (CREATE TRIGGER must lead a batch).
            return "\n" + string.Join("\nGO\n", statements);
        }

        /// <inheritdoc />
        public override bool SupportsProviderNativeTemporalTables => true;

        /// <inheritdoc />
        public override string GenerateProviderNativeTemporalBootstrapSql(TableMapping mapping)
        {
            if (mapping.KeyColumns.Length == 0)
                throw new NormConfigurationException(
                    $"SQL Server system-versioned temporal table '{mapping.Type.Name}' requires a primary key.");

            var (schema, tableName) = SplitSchemaTable(mapping.TableName, "dbo");
            var escapedTable = Escape(mapping.TableName);
            var escapedHistory = Escape(schema + "." + tableName + "_SystemHistory");
            var startColumn = Escape("__NormSysStartTime");
            var endColumn = Escape("__NormSysEndTime");
            var startConstraint = Escape("DF_" + tableName + "_NormSysStart");
            var endConstraint = Escape("DF_" + tableName + "_NormSysEnd");
            var qualifiedName = (schema + "." + tableName).Replace("'", "''", StringComparison.Ordinal);

            return $@"
IF COL_LENGTH(N'{qualifiedName}', N'__NormSysStartTime') IS NULL
BEGIN
    ALTER TABLE {escapedTable} ADD
        {startColumn} DATETIME2 GENERATED ALWAYS AS ROW START HIDDEN NOT NULL
            CONSTRAINT {startConstraint} DEFAULT SYSUTCDATETIME(),
        {endColumn} DATETIME2 GENERATED ALWAYS AS ROW END HIDDEN NOT NULL
            CONSTRAINT {endConstraint} DEFAULT CONVERT(DATETIME2, '9999-12-31 23:59:59.9999999'),
        PERIOD FOR SYSTEM_TIME ({startColumn}, {endColumn});
END;

IF NOT EXISTS (
    SELECT 1
    FROM sys.tables
    WHERE object_id = OBJECT_ID(N'{qualifiedName}') AND temporal_type = 2
)
BEGIN
    ALTER TABLE {escapedTable}
        SET (SYSTEM_VERSIONING = ON (
            HISTORY_TABLE = {escapedHistory},
            DATA_CONSISTENCY_CHECK = ON
        ));
END;";
        }

        /// <inheritdoc />
        public override string GetProviderNativeTemporalAsOfFromClause(TableMapping mapping, string timestampParameterName)
        {
            EnsureValidParameterName(timestampParameterName, nameof(timestampParameterName));
            return $"{Escape(mapping.TableName)} FOR SYSTEM_TIME AS OF {timestampParameterName}";
        }
    }
}
