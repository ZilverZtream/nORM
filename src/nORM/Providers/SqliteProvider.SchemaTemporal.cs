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
        /// A plain ':memory:' (or Mode=Memory) database without shared cache is PRIVATE to its
        /// connection: two connections with the identical string see different data, so cache
        /// keys must include the connection instance. Shared-cache in-memory databases are
        /// addressable by name and stay connection-string-keyed.
        /// </summary>
        public override bool IsConnectionScopedDatabase(string connectionString)
            => (connectionString.Contains(":memory:", StringComparison.OrdinalIgnoreCase)
                || connectionString.Contains("Mode=Memory", StringComparison.OrdinalIgnoreCase))
               && !connectionString.Contains("Cache=Shared", StringComparison.OrdinalIgnoreCase)
               && !connectionString.Contains("cache=shared", StringComparison.Ordinal);

        /// <summary>
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
            // Prefer the INTROSPECTED physical column set: the live table can carry columns
            // that exist only physically (an owned collection's FK, a raw ADD COLUMN), and a
            // history table missing them loses data or makes owned rows uncorrelatable to
            // their owners at a past timestamp. The mapping set is the offline fallback.
            var columns = liveColumns is { Count: > 0 }
                ? string.Join(",\n                ", liveColumns.Select(live =>
                    $"{Escape(live.Name)} {live.SqlType}{(live.IsNullable ? "" : " NOT NULL")}"))
                : string.Join(",\n                ", mapping.Columns.Select(c =>
                {
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
        /// Generates trigger definitions that maintain the temporal history table. The DDL text
        /// lives in <see cref="SqliteTemporalDdl"/>, shared with the migration generator so a
        /// migration that reshapes a temporal table re-emits IDENTICAL triggers from the new schema.
        /// </summary>
        /// <param name="mapping">The mapping describing the source table.</param>
        /// <param name="liveColumns">Live physical column info from the main table, or null to use the mapped set.</param>
        /// <returns>DDL statements creating the triggers.</returns>
        public override string GenerateTemporalTriggersSql(TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null)
        {
            var statements = SqliteTemporalDdl.BuildTriggersSql(
                Escape,
                mapping.TableName,
                liveColumns is { Count: > 0 }
                    ? liveColumns.Select(c => c.Name).ToArray()
                    : mapping.Columns.Select(c => c.Name).ToArray(),
                mapping.KeyColumns.Select(c => c.Name).ToArray(),
                mapping.TenantColumn?.Name);
            return "\n" + string.Join("\n\n", statements);
        }

        /// <summary>
        /// SQLite validity windows are TEXT compared lexically. The triggers write fixed
        /// three-decimal milliseconds (strftime %f), but Microsoft.Data.Sqlite's default
        /// DateTime text trims trailing fractional zeros - "53.59" sorts BEFORE "53.590", so
        /// an AsOf at an exact transition instant whose milliseconds end in zero matched the
        /// OLD version's window too. Bind the timestamp as fixed three-decimal text so the
        /// lexical comparison aligns with the stored format.
        /// </summary>
        public override object FormatTemporalAsOfParameterValue(DateTime timestamp)
            => timestamp.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture);

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

        /// <summary>
        /// Releases a previously created savepoint (RELEASE SAVEPOINT), keeping its work but removing it as a
        /// rollback target. Checks the CancellationToken before executing so that pre-cancelled tokens
        /// correctly throw <see cref="OperationCanceledException"/>.
        /// </summary>
        /// <param name="transaction">The active SQLite transaction.</param>
        /// <param name="name">Name of the savepoint to release.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        public override Task ReleaseSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            // Honour the CancellationToken - a pre-cancelled token must throw immediately.
            ct.ThrowIfCancellationRequested();

            if (transaction is SqliteTransaction sqliteTransaction)
            {
                sqliteTransaction.Release(name);
                ct.ThrowIfCancellationRequested();
                return Task.CompletedTask;
            }
            throw new ArgumentException("Transaction must be a SqliteTransaction.", nameof(transaction));
        }
    }
}
