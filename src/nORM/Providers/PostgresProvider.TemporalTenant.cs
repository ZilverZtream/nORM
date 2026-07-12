using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class PostgresProvider
    {
        /// <summary>Splits "schema.table" into parts; defaults to the specified schema when unqualified.</summary>
        /// <param name="tableName">Table name that may include a schema prefix separated by a dot.</param>
        /// <param name="defaultSchema">Schema name to use when <paramref name="tableName"/> is unqualified.</param>
        /// <returns>A tuple of (Schema, Table) with surrounding double-quote characters stripped.</returns>
        private static (string Schema, string Table) SplitSchemaTable(string tableName, string defaultSchema)
        {
            var dot = tableName.IndexOf('.');
            if (dot < 0)
                return (defaultSchema, tableName.Trim('"'));
            return (tableName[..dot].Trim('"'), tableName[(dot + 1)..].Trim('"'));
        }

        /// <summary>
        /// Introspects live column definitions via information_schema.columns.
        /// Reconstructs full type strings including numeric precision/scale and character length.
        /// Returns empty list when the table does not yet exist.
        /// </summary>
        public override async Task<IReadOnlyList<LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbConnection conn, string tableName, CancellationToken ct = default)
        {
            var result = new List<LiveColumnInfo>();
            try
            {
                await using var cmd = conn.CreateCommand();
                var (schema, bareTable) = SplitSchemaTable(tableName, "public");
                cmd.CommandText = @"
SELECT column_name, data_type, character_maximum_length,
       numeric_precision, numeric_scale, is_nullable
FROM information_schema.columns
WHERE table_name = @t AND table_schema = @s
ORDER BY ordinal_position";
                var p = cmd.CreateParameter(); p.ParameterName = "@t"; p.Value = bareTable; cmd.Parameters.Add(p);
                var ps = cmd.CreateParameter(); ps.ParameterName = "@s"; ps.Value = schema; cmd.Parameters.Add(ps);
                await using var rdr = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await rdr.ReadAsync(ct).ConfigureAwait(false))
                {
                    var name = rdr.GetString(0);
                    var dataType = rdr.GetString(1).ToLowerInvariant();
                    var charMax = rdr.IsDBNull(2) ? (int?)null : rdr.GetInt32(2);
                    var numPrec = rdr.IsDBNull(3) ? (int?)null : rdr.GetInt32(3);
                    var numScale = rdr.IsDBNull(4) ? (int?)null : rdr.GetInt32(4);
                    var isNullable = rdr.GetString(5).Equals("YES", StringComparison.OrdinalIgnoreCase);

                    var sqlType = dataType switch
                    {
                        "character varying" or "varchar" =>
                            charMax.HasValue ? $"varchar({charMax})" : "text",
                        "character" or "char" =>
                            charMax.HasValue ? $"char({charMax})" : "char",
                        "numeric" or "decimal" =>
                            (numPrec.HasValue && numScale.HasValue) ? $"numeric({numPrec},{numScale})" : "numeric",
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

        /// <summary>
        /// Generates the SQL definition for the temporal history table corresponding to the entity mapping.
        /// When liveColumns are supplied, column types are taken from the live DB schema.
        /// </summary>
        /// <param name="mapping">The entity mapping to create history storage for.</param>
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
                return $"{Escape(c.Name)} {GetPostgresType(c.Prop.PropertyType)}";
            }));

            return $@"
CREATE TABLE {historyTable} (
    ""__VersionId"" BIGSERIAL PRIMARY KEY,
    ""__ValidFrom"" TIMESTAMP NOT NULL,
    ""__ValidTo"" TIMESTAMP NOT NULL,
    ""__Operation"" CHAR(1) NOT NULL,
    {columns}
);";
        }

        /// <summary>
        /// Use a timestamp column for temporal tag lookups so tag timestamps have
        /// the same UTC-without-kind contract as generated history rows.
        /// </summary>
        public override string GetCreateTagsTableSql()
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"CREATE TABLE IF NOT EXISTS {table} ({tagCol} TEXT NOT NULL, {tsCol} TIMESTAMP NOT NULL, PRIMARY KEY ({tagCol}))";
        }

        /// <summary>
        /// Creates temporal tags using the PostgreSQL UTC database clock so tag
        /// timestamps are comparable to trigger-generated history windows.
        /// </summary>
        public override string GetCreateTagSql(string pTagName, string pTimestamp)
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"INSERT INTO {table} ({tagCol}, {tsCol}) VALUES ({pTagName}, (now() at time zone 'utc'))";
        }

        internal override bool UsesDatabaseClockForTemporalTags => true;

        /// <summary>
        /// Produces the trigger definitions required to track changes in the temporal history table.
        /// </summary>
        /// <param name="mapping">The mapping describing the target table.</param>
        /// <returns>DDL statements that create the temporal triggers.</returns>
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columns = string.Join(", ", mapping.Columns.Select(c => Escape(c.Name)));
            var newColumns = string.Join(", ", mapping.Columns.Select(c => "NEW." + Escape(c.Name)));
            var oldColumns = string.Join(", ", mapping.Columns.Select(c => "OLD." + Escape(c.Name)));
            var keyCondition = string.Join(" AND ", mapping.KeyColumns.Select(c => $"{Escape(c.Name)} = OLD.{Escape(c.Name)}"));
            var functionName = Escape(mapping.TableName + "_TemporalFunction");

            return $@"
CREATE OR REPLACE FUNCTION {functionName}() RETURNS TRIGGER AS $$
DECLARE v_now TIMESTAMP := (now() at time zone 'utc');
BEGIN
    IF (TG_OP = 'INSERT') THEN
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, '9999-12-31', 'I', {newColumns});
    ELSIF (TG_OP = 'UPDATE') THEN
        UPDATE {history} SET ""__ValidTo"" = v_now
        WHERE ""__ValidTo"" = '9999-12-31' AND {keyCondition};
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, '9999-12-31', 'U', {newColumns});
    ELSIF (TG_OP = 'DELETE') THEN
        UPDATE {history} SET ""__ValidTo"" = v_now
        WHERE ""__ValidTo"" = '9999-12-31' AND {keyCondition};
        INSERT INTO {history} (""__ValidFrom"", ""__ValidTo"", ""__Operation"", {columns})
        VALUES (v_now, v_now, 'D', {oldColumns});
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS {Escape(mapping.TableName + "_TemporalTrigger")} ON {table};
CREATE TRIGGER {Escape(mapping.TableName + "_TemporalTrigger")}
AFTER INSERT OR UPDATE OR DELETE ON {table}
FOR EACH ROW EXECUTE FUNCTION {functionName}();";
        }

        /// <inheritdoc />
        public override bool SupportsNativeTenantSessionContext => true;

        /// <inheritdoc />
        public override string GetSetNativeTenantSessionContextSql(string sessionKey, string tenantParameterName)
        {
            EnsureValidParameterName(tenantParameterName, nameof(tenantParameterName));
            if (string.IsNullOrWhiteSpace(sessionKey) || sessionKey.Contains('\'', StringComparison.Ordinal))
                throw new ArgumentException("Session key must be non-empty and must not contain single quotes.", nameof(sessionKey));
            return $"SELECT set_config('{sessionKey}', {tenantParameterName}, false);";
        }

        /// <inheritdoc />
        public override string GenerateNativeTenantPolicySql(TableMapping mapping, string sessionKey)
        {
            var tenantCol = mapping.TenantColumn
                ?? throw new NormConfigurationException(
                    $"Entity '{mapping.Type.Name}' does not map tenant column required for native PostgreSQL RLS.");
            if (string.IsNullOrWhiteSpace(sessionKey) || sessionKey.Contains('\'', StringComparison.Ordinal))
                throw new ArgumentException("Session key must be non-empty and must not contain single quotes.", nameof(sessionKey));

            var table = Escape(mapping.TableName);
            var policy = Escape("norm_rls_" + mapping.TableName);
            return $@"
ALTER TABLE {table} ENABLE ROW LEVEL SECURITY;
ALTER TABLE {table} FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS {policy} ON {table};
CREATE POLICY {policy} ON {table}
USING ({tenantCol.EscCol}::text = current_setting('{sessionKey}', true))
WITH CHECK ({tenantCol.EscCol}::text = current_setting('{sessionKey}', true));";
        }

        /// <inheritdoc />
        public override string GenerateDropNativeTenantPolicySql(TableMapping mapping)
        {
            if (mapping.TenantColumn == null)
                throw new NormConfigurationException(
                    $"Entity '{mapping.Type.Name}' does not map tenant column required for native PostgreSQL RLS.");

            var table = Escape(mapping.TableName);
            var policy = Escape("norm_rls_" + mapping.TableName);
            return $@"
DROP POLICY IF EXISTS {policy} ON {table};
ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY;
ALTER TABLE {table} DISABLE ROW LEVEL SECURITY;";
        }
    }
}
