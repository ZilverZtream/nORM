using System;
using System.Collections.Generic;
using System.Linq;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Single source of truth for SQLite temporal-versioning trigger DDL. Consumed by both the
    /// runtime bootstrap (<see cref="SqliteProvider.GenerateTemporalTriggersSql"/>, mapping-driven)
    /// and the migration generator (schema-driven): a migration that recreates or reshapes a
    /// temporal main table must re-emit these triggers from the NEW column set, and the two DDL
    /// producers must never drift apart.
    /// </summary>
    internal static class SqliteTemporalDdl
    {
        /// <summary>
        /// strftime %f carries milliseconds — datetime('now') is second-precision, which collapses
        /// versions written within one second and answers AsOf timestamps between sub-second
        /// updates with the wrong version. The millisecond text compares lexically with both the
        /// second-precision rows of pre-existing history tables and the fractional format
        /// Microsoft.Data.Sqlite binds for .NET DateTime parameters.
        /// </summary>
        internal const string NowExpression = "strftime('%Y-%m-%d %H:%M:%f', 'now')";

        /// <summary>The three versioning trigger names for a temporal table, in emit order.</summary>
        internal static string[] GetTriggerNames(string tableName)
            => new[] { tableName + "_ai", tableName + "_au", tableName + "_ad" };

        /// <summary>
        /// Builds the three CREATE TRIGGER IF NOT EXISTS statements that maintain a temporal
        /// history table, one statement per list element.
        /// </summary>
        /// <param name="escape">Identifier escaper of the caller (provider Escape or generator Esc;
        /// both produce SQLite double-quoted identifiers).</param>
        /// <param name="tableName">Unescaped main table name.</param>
        /// <param name="columnNames">All main-table column names in declaration order.</param>
        /// <param name="keyColumnNames">Primary key column names.</param>
        /// <param name="tenantColumnName">Tenant column name, or null when the table is not tenant-scoped.</param>
        internal static IReadOnlyList<string> BuildTriggersSql(
            Func<string, string> escape,
            string tableName,
            IReadOnlyList<string> columnNames,
            IReadOnlyList<string> keyColumnNames,
            string? tenantColumnName)
        {
            var table = escape(tableName);
            var history = escape(tableName + "_History");
            var columnList = string.Join(", ", columnNames.Select(escape));
            var newColumns = string.Join(", ", columnNames.Select(c => "NEW." + escape(c)));
            var oldColumns = string.Join(", ", columnNames.Select(c => "OLD." + escape(c)));
            var keyCondition = keyColumnNames.Count > 0
                ? string.Join(" AND ", keyColumnNames.Select(c => $"{escape(c)} = OLD.{escape(c)}"))
                : "1=1";
            // Scope the history close to the same tenant. The history table is not itself PK-unique
            // (its key includes __ValidFrom), so matching on the entity key alone could close another
            // tenant's open history row; adding the tenant predicate keeps temporal writes tenant-isolated
            // like every other write path (SEC-MT). No-op when the tenant column is already part of the key.
            if (tenantColumnName is { } tc && !keyColumnNames.Any(k => string.Equals(k, tc, StringComparison.Ordinal)))
                keyCondition += $" AND {escape(tc)} = OLD.{escape(tc)}";

            return new[]
            {
                @$"CREATE TRIGGER IF NOT EXISTS {escape(tableName + "_ai")} AFTER INSERT ON {table}
BEGIN
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES ({NowExpression}, '9999-12-31', 'I', {newColumns});
END;",
                @$"CREATE TRIGGER IF NOT EXISTS {escape(tableName + "_au")} AFTER UPDATE ON {table}
BEGIN
    UPDATE {history} SET __ValidTo = {NowExpression} WHERE __ValidTo = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES ({NowExpression}, '9999-12-31', 'U', {newColumns});
END;",
                @$"CREATE TRIGGER IF NOT EXISTS {escape(tableName + "_ad")} AFTER DELETE ON {table}
BEGIN
    UPDATE {history} SET __ValidTo = {NowExpression} WHERE __ValidTo = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES ({NowExpression}, {NowExpression}, 'D', {oldColumns});
END;"
            };
        }
    }
}
