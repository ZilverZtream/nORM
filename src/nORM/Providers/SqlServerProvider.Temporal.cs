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
            var liveMap = liveColumns?
                .ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase)
                ?? new Dictionary<string, LiveColumnInfo>(0);

            var columns = string.Join(",\n    ", mapping.Columns.Select(c =>
            {
                if (liveMap.TryGetValue(c.Name, out var live))
                    return $"{Escape(c.Name)} {live.SqlType}{(live.IsNullable ? "" : " NOT NULL")}";
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
        /// <returns>DDL statements creating the temporal triggers.</returns>
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columns = string.Join(", ", mapping.Columns.Select(c => Escape(c.Name)));
            var insertedColumns = string.Join(", ", mapping.Columns.Select(c => "i." + Escape(c.Name)));
            var deletedColumns = string.Join(", ", mapping.Columns.Select(c => "d." + Escape(c.Name)));
            var keyCondition = string.Join(" AND ", mapping.KeyColumns.Select(c => $"h.{Escape(c.Name)} = d.{Escape(c.Name)}"));
            var keyConditionH2 = string.Join(" AND ", mapping.KeyColumns.Select(c => $"h2.{Escape(c.Name)} = d.{Escape(c.Name)}"));
            // Scope the history close to the same tenant (SEC-MT): the history table is not PK-unique
            // (its key includes the validity range), so matching on the entity key alone could close a
            // different tenant's open row. No-op when the tenant column is already part of the key.
            if (mapping.TenantColumn is { } tc && !mapping.KeyColumns.Any(k => k.Name == tc.Name))
            {
                keyCondition += $" AND h.{Escape(tc.Name)} = d.{Escape(tc.Name)}";
                keyConditionH2 += $" AND h2.{Escape(tc.Name)} = d.{Escape(tc.Name)}";
            }

            return $@"
DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_TemporalInsert")};
GO
CREATE TRIGGER {Escape(mapping.TableName + "_TemporalInsert")} ON {table} AFTER INSERT AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = SYSUTCDATETIME();
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columns})
    SELECT @Now, '9999-12-31', 'I', {insertedColumns} FROM inserted i;
END;
GO

DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_TemporalUpdate")};
GO
CREATE TRIGGER {Escape(mapping.TableName + "_TemporalUpdate")} ON {table} AFTER UPDATE AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = SYSUTCDATETIME();
    UPDATE h SET h.__ValidTo = @Now
    FROM {history} h
    JOIN deleted d ON {keyCondition}
    WHERE h.__ValidTo = '9999-12-31'
      AND NOT EXISTS (
        SELECT 1 FROM {history} h2
        WHERE h2.__ValidFrom = @Now AND {keyConditionH2}
    );
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columns})
    SELECT @Now, '9999-12-31', 'U', {insertedColumns} FROM inserted i;
END;
GO

DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_TemporalDelete")};
GO
CREATE TRIGGER {Escape(mapping.TableName + "_TemporalDelete")} ON {table} AFTER DELETE AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @Now DATETIME2 = SYSUTCDATETIME();
    UPDATE h SET h.__ValidTo = @Now
    FROM {history} h
    JOIN deleted d ON {keyCondition}
    WHERE h.__ValidTo = '9999-12-31'
      AND NOT EXISTS (
        SELECT 1 FROM {history} h2
        WHERE h2.__ValidFrom = @Now AND {keyConditionH2}
    );
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columns})
    SELECT @Now, @Now, 'D', {deletedColumns} FROM deleted d;
END;";
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
