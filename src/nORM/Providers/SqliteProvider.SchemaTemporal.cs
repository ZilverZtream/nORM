using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public partial class SqliteProvider
    {        /// <summary>
        /// Translates JSON value access using SQLite's <c>json_extract</c> function.
        /// </summary>
        /// <param name="columnName">JSON column to access.</param>
        /// <param name="jsonPath">JSON path expression.</param>
        /// <returns>SQL fragment retrieving the requested JSON value.</returns>
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        {
            ArgumentNullException.ThrowIfNull(columnName);
            ArgumentNullException.ThrowIfNull(jsonPath);
            ValidateJsonPath(jsonPath);
            return $"json_extract({columnName}, '{jsonPath}')";
        }

        /// <summary>
        /// SQLite table-not-found errors use SQLITE_ERROR (code 1) combined with
        /// the "no such table" message. Code 1 alone is too broad (also covers syntax errors).
        /// Other error codes (SQLITE_PERM=3, SQLITE_CANTOPEN=14, etc.) indicate operational failures
        /// that must NOT be silently treated as "table absent."
        /// </summary>
        public override bool IsObjectNotFoundError(DbException ex)
            => ex is SqliteException sqliteEx
               && sqliteEx.SqliteErrorCode == 1
               && ex.Message.Contains("no such table", StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// Introspects live column definitions via PRAGMA table_info.
        /// Returns empty list when the table does not yet exist.
        /// </summary>
        public override async Task<IReadOnlyList<LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbConnection conn, string tableName, CancellationToken ct = default)
        {
            var result = new List<LiveColumnInfo>();
            try
            {
                await using var cmd = conn.CreateCommand();
                // PRAGMA table_info columns: cid[0], name[1], type[2], notnull[3], dflt_value[4], pk[5]
                var bare = tableName.Trim('"');
                cmd.CommandText = $"PRAGMA table_info(\"{bare.Replace("\"", "\"\"")}\")";
                await using var rdr = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await rdr.ReadAsync(ct).ConfigureAwait(false))
                {
                    var name = rdr.GetString(1);
                    var sqlType = rdr.GetString(2);
                    var notNull = rdr.GetInt32(3) != 0;
                    result.Add(new LiveColumnInfo(name, string.IsNullOrEmpty(sqlType) ? "TEXT" : sqlType, !notNull));
                }
            }
            catch (DbException dbEx) when (IsObjectNotFoundError(dbEx))
            {
                // Table does not exist yet - return empty list so caller falls back to CLR defaults.
            }
            return result;
        }

        /// <summary>
        /// Generates SQL to create a history table. Column types use the same SQLite type
        /// mapping as the main table (GetSqliteType), ensuring the history schema mirrors
        /// the main table exactly (INTEGER for int/bool/long, REAL for decimal/double/float,
        /// BLOB for byte[], TEXT for strings and everything else).
        /// When liveColumns are supplied, live SQL types and nullability override CLR defaults.
        /// </summary>
        /// <param name="mapping">The entity mapping being tracked.</param>
        /// <param name="liveColumns">Live column info from the main table, or null to use CLR defaults.</param>
        /// <returns>DDL statement that creates the history table.</returns>
        public override string GenerateCreateHistoryTableSql(
            TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null)
        {
            var liveMap = liveColumns?
                .ToDictionary(c => c.Name, StringComparer.OrdinalIgnoreCase)
                ?? new Dictionary<string, LiveColumnInfo>(0);

            var columns = string.Join(",\n                ", mapping.Columns.Select(c =>
            {
                if (liveMap.TryGetValue(c.Name, out var live))
                    return $"{Escape(c.Name)} {live.SqlType}{(live.IsNullable ? "" : " NOT NULL")}";
                // History rows copy the main table's converter-encoded values, so the
                // fallback types by the PROVIDER representation.
                var sqlType = GetSqliteType(c.Converter?.ProviderType ?? c.Prop.PropertyType);
                var nullability = IsNullableOrReferenceType(c.Prop.PropertyType) ? "" : " NOT NULL";
                return $"{Escape(c.Name)} {sqlType}{nullability}";
            }));
            return @$"CREATE TABLE IF NOT EXISTS {Escape(mapping.TableName + "_History")} (
                __VersionId INTEGER PRIMARY KEY AUTOINCREMENT,
                __ValidFrom TEXT NOT NULL,
                __ValidTo TEXT NOT NULL,
                __Operation TEXT NOT NULL,
                {columns}
            );";
        }

        /// <summary>
        /// Returns true when the type can hold a SQL NULL value (reference types and Nullable&lt;T&gt;).
        /// Used by <see cref="GenerateCreateHistoryTableSql"/> to decide NOT NULL constraints.
        /// </summary>
        private static bool IsNullableOrReferenceType(Type t) =>
            !t.IsValueType || (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>));

        /// <summary>
        /// Generates trigger definitions that maintain the temporal history table.
        /// </summary>
        /// <param name="mapping">The mapping describing the source table.</param>
        /// <returns>DDL statements creating the triggers.</returns>
        public override string GenerateTemporalTriggersSql(TableMapping mapping)
        {
            var table = Escape(mapping.TableName);
            var history = Escape(mapping.TableName + "_History");
            var columnList = string.Join(", ", mapping.Columns.Select(c => Escape(c.Name)));
            var newColumns = string.Join(", ", mapping.Columns.Select(c => "NEW." + Escape(c.Name)));
            var oldColumns = string.Join(", ", mapping.Columns.Select(c => "OLD." + Escape(c.Name)));
            var keyCondition = mapping.KeyColumns.Length > 0
                ? string.Join(" AND ", mapping.KeyColumns.Select(c => $"{Escape(c.Name)} = OLD.{Escape(c.Name)}"))
                : "1=1";
            // Scope the history close to the same tenant. The history table is not itself PK-unique
            // (its key includes __ValidFrom), so matching on the entity key alone could close another
            // tenant's open history row; adding the tenant predicate keeps temporal writes tenant-isolated
            // like every other write path (SEC-MT). No-op when the tenant column is already part of the key.
            if (mapping.TenantColumn is { } tc && !mapping.KeyColumns.Any(k => k.Name == tc.Name))
                keyCondition += $" AND {Escape(tc.Name)} = OLD.{Escape(tc.Name)}";

            // strftime %f carries milliseconds — datetime('now') is second-precision,
            // which collapses versions written within one second and answers AsOf
            // timestamps between sub-second updates with the wrong version. The
            // millisecond text compares lexically with both the second-precision
            // rows of pre-existing history tables and the fractional format
            // Microsoft.Data.Sqlite binds for .NET DateTime parameters.
            const string nowExpr = "strftime('%Y-%m-%d %H:%M:%f', 'now')";
            return @$"
CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_ai")} AFTER INSERT ON {table}
BEGIN
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES ({nowExpr}, '9999-12-31', 'I', {newColumns});
END;

CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_au")} AFTER UPDATE ON {table}
BEGIN
    UPDATE {history} SET __ValidTo = {nowExpr} WHERE __ValidTo = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES ({nowExpr}, '9999-12-31', 'U', {newColumns});
END;

CREATE TRIGGER IF NOT EXISTS {Escape(mapping.TableName + "_ad")} AFTER DELETE ON {table}
BEGIN
    UPDATE {history} SET __ValidTo = {nowExpr} WHERE __ValidTo = '9999-12-31' AND {keyCondition};
    INSERT INTO {history} (__ValidFrom, __ValidTo, __Operation, {columnList})
    VALUES ({nowExpr}, {nowExpr}, 'D', {oldColumns});
END;";
        }

        /// <summary>
        /// Creates temporal tags using SQLite's database clock so tag timestamps
        /// are comparable to trigger-generated history windows.
        /// </summary>
        public override string GetCreateTagSql(string pTagName, string pTimestamp)
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"INSERT INTO {table} ({tagCol}, {tsCol}) VALUES ({pTagName}, strftime('%Y-%m-%d %H:%M:%f', 'now'))";
        }

        internal override bool UsesDatabaseClockForTemporalTags => true;

        /// <summary>
        /// Verifies that the provided connection is a <see cref="SqliteConnection"/>, as required
        /// by this provider.
        /// </summary>
        /// <param name="connection">The connection to validate.</param>
        /// <exception cref="InvalidOperationException">Thrown if the connection is not compatible.</exception>
        protected override void ValidateConnection(DbConnection connection)
        {
            base.ValidateConnection(connection);
            if (connection is not SqliteConnection)
                throw new InvalidOperationException("A SqliteConnection is required for SqliteProvider.");
        }

        /// <summary>
        /// Determines if the SQLite provider can be used in the current environment by
        /// verifying that the required <c>Microsoft.Data.Sqlite</c> assembly is available
        /// and that the SQLite engine meets the minimum version requirement.
        /// </summary>
        public override async Task<bool> IsAvailableAsync()
        {
            var type = Type.GetType("Microsoft.Data.Sqlite.SqliteConnection, Microsoft.Data.Sqlite");
            if (type == null) return false;

            await using var cn = (DbConnection)Activator.CreateInstance(type)!;
            cn.ConnectionString = "Data Source=:memory:";
            try
            {
                await cn.OpenAsync().ConfigureAwait(false);
                await using var cmd = cn.CreateCommand();
                cmd.CommandText = "select sqlite_version()";
                var result = await cmd.ExecuteScalarAsync().ConfigureAwait(false);
                if (result is not string versionStr)
                    throw new NormDatabaseException("Unable to retrieve database version.", cmd.CommandText, null, null);
                var version = new Version(versionStr);
                return version >= MinimumSqliteVersion;
            }
            catch (DbException)
            {
                return false;
            }
            catch (InvalidOperationException)
            {
                return false;
            }
        }

        /// <inheritdoc />
        protected override async Task<string?> GetServerVersionStringAsync(DbConnection connection, CancellationToken ct)
        {
            await using var cmd = connection.CreateCommand();
            cmd.CommandText = "select sqlite_version()";
            return await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false) as string;
        }

        /// <inheritdoc />
        protected override string? GetServerVersionString(DbConnection connection)
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = "select sqlite_version()";
            return cmd.ExecuteScalar() as string;
        }

        /// <summary>
        /// Creates a savepoint within a SQLite transaction allowing partial rollbacks.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The active SQLite transaction.</param>
        /// <param name="name">Name of the savepoint to create.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public override Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken - a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            if (transaction is SqliteTransaction sqliteTransaction)
            {
                sqliteTransaction.Save(name);
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction));
        }

        /// <summary>
        /// Rolls back a SQLite transaction to the specified savepoint.
        /// Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The active SQLite transaction.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public override Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken - a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            if (transaction is SqliteTransaction sqliteTransaction)
            {
                sqliteTransaction.Rollback(name);
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction));
        }
    }
}
