using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using nORM.Query;
using System.Threading;
using System.Threading.Tasks;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using nORM.Configuration;

#nullable enable

namespace nORM.Providers
{
    public abstract partial class DatabaseProvider
    {
        /// <summary>
        /// Describes a column as it actually exists in the live database.
        /// Used by <see cref="IntrospectTableColumnsAsync"/> and
        /// <see cref="GenerateCreateHistoryTableSql"/> to match history-table column
        /// types to the main table rather than deriving them from CLR defaults.
        /// </summary>
        /// <param name="Name">Column name.</param>
        /// <param name="SqlType">Provider SQL type.</param>
        /// <param name="IsNullable">Whether the database column allows NULL.</param>
        public record LiveColumnInfo(string Name, string SqlType, bool IsNullable);

        /// <summary>
        /// Introspects the live column definitions for the named table.
        /// Returns an empty list when the table does not yet exist, allowing callers
        /// to fall back to CLR-default type mapping.
        /// Providers override this to use their native schema-inspection facilities.
        /// </summary>
        public virtual Task<IReadOnlyList<LiveColumnInfo>> IntrospectTableColumnsAsync(
            DbConnection conn, string tableName, CancellationToken ct = default)
            => Task.FromResult<IReadOnlyList<LiveColumnInfo>>(Array.Empty<LiveColumnInfo>());

        /// <summary>
        /// Generates the SQL required to create a history table for temporal table support.
        /// When <paramref name="liveColumns"/> is supplied, column types are taken from the live
        /// DB schema rather than derived from CLR property types, ensuring the history table
        /// mirrors any custom precision/length settings on the main table.
        /// </summary>
        /// <param name="mapping">The table mapping representing the entity.</param>
        /// <param name="liveColumns">Live column info from the main table, or null to use CLR defaults.</param>
        /// <returns>The SQL statement that creates the history table.</returns>
        public abstract string GenerateCreateHistoryTableSql(
            TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null);

        /// <summary>
        /// Generates the SQL required to create triggers for maintaining the temporal history table.
        /// When <paramref name="liveColumns"/> is supplied, the trigger column list is taken from
        /// the live physical schema — the table can carry columns that exist only physically (an
        /// owned collection's foreign key, a raw ADD COLUMN), and triggers built from the mapped
        /// property set alone would silently omit their values from history.
        /// </summary>
        /// <param name="mapping">The table mapping representing the entity.</param>
        /// <param name="liveColumns">Live column info from the main table, or null to use the mapped set.</param>
        /// <returns>The SQL script containing the trigger definitions.</returns>
        public abstract string GenerateTemporalTriggersSql(TableMapping mapping, IReadOnlyList<LiveColumnInfo>? liveColumns = null);

        /// <summary>
        /// Gets whether this provider can use database-native temporal tables for
        /// time-travel reads and provider-owned history storage.
        /// </summary>
        public virtual bool SupportsProviderNativeTemporalTables => false;

        /// <summary>
        /// Generates reviewable DDL that enables provider-native temporal storage
        /// for an existing mapped table.
        /// </summary>
        /// <param name="mapping">Mapped entity table.</param>
        public virtual string GenerateProviderNativeTemporalBootstrapSql(TableMapping mapping)
            => throw new NormUnsupportedFeatureException(
                $"{GetType().Name} does not support provider-native temporal tables.");

        /// <summary>
        /// Converts an AsOf timestamp to the value actually bound for history-window comparison.
        /// The default passes the <see cref="DateTime"/> through - providers whose history
        /// columns are real datetime types compare natively. Providers that store validity
        /// windows as TEXT and compare lexically (SQLite) override this to bind a string whose
        /// format matches the trigger-written text EXACTLY; the driver's default DateTime text
        /// trims trailing fractional zeros, which sorts BEFORE the stored fixed-width
        /// milliseconds and silently returns the OLD version at exact boundaries.
        /// </summary>
        /// <param name="timestamp">The AsOf timestamp being bound.</param>
        public virtual object FormatTemporalAsOfParameterValue(DateTime timestamp) => timestamp;

        /// <summary>
        /// Returns the provider-specific <c>FROM</c> source for an as-of read against
        /// a provider-native temporal table.
        /// </summary>
        /// <param name="mapping">Mapped entity table.</param>
        /// <param name="timestampParameterName">Bound timestamp parameter name.</param>
        public virtual string GetProviderNativeTemporalAsOfFromClause(TableMapping mapping, string timestampParameterName)
            => throw new NormUnsupportedFeatureException(
                $"{GetType().Name} does not support provider-native temporal table as-of queries.");

        /// <summary>
        /// Gets whether this provider can store the current tenant ID in provider-native
        /// session context for database-native RLS policies.
        /// </summary>
        public virtual bool SupportsNativeTenantSessionContext => false;

        /// <summary>
        /// Returns SQL that stores the current tenant ID in provider-native session context.
        /// </summary>
        /// <param name="sessionKey">Provider session key.</param>
        /// <param name="tenantParameterName">Parameter name containing the current tenant ID.</param>
        public virtual string GetSetNativeTenantSessionContextSql(string sessionKey, string tenantParameterName)
            => throw new NormUnsupportedFeatureException(
                $"{GetType().Name} does not support provider-native tenant session context.");

        /// <summary>
        /// Generates optional provider-native RLS policy DDL for a mapped tenant table.
        /// Applications own executing and reviewing this DDL.
        /// </summary>
        /// <param name="mapping">Mapped entity table.</param>
        /// <param name="sessionKey">Provider session key that contains the current tenant ID.</param>
        public virtual string GenerateNativeTenantPolicySql(TableMapping mapping, string sessionKey)
            => throw new NormUnsupportedFeatureException(
                $"{GetType().Name} does not support provider-native tenant policy DDL generation.");

        /// <summary>
        /// Generates optional provider-native DDL that removes nORM's tenant RLS
        /// policy objects for a mapped tenant table.
        /// </summary>
        /// <param name="mapping">Mapped entity table.</param>
        public virtual string GenerateDropNativeTenantPolicySql(TableMapping mapping)
            => throw new NormUnsupportedFeatureException(
                $"{GetType().Name} does not support provider-native tenant policy DDL generation.");

        /// <summary>
        /// Returns provider-specific SQL to create the temporal tags table if it does not exist.
        /// Default uses IF NOT EXISTS syntax with TEXT column types (SQLite/MySQL/Postgres).
        /// SQL Server overrides this to use OBJECT_ID check and NVARCHAR/DATETIME2 types.
        /// </summary>
        public virtual string GetCreateTagsTableSql()
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"CREATE TABLE IF NOT EXISTS {table} ({tagCol} TEXT NOT NULL, {tsCol} TEXT NOT NULL, PRIMARY KEY ({tagCol}))";
        }

        /// <summary>
        /// Returns provider-specific SQL to probe for the existence of a history table.
        /// Default uses SELECT 1 ... LIMIT 1 (SQLite/MySQL/Postgres).
        /// SQL Server overrides this to use SELECT TOP 1.
        /// </summary>
        /// <param name="escapedHistoryTable">The already-escaped history table name.</param>
        public virtual string GetHistoryTableExistsProbeSql(string escapedHistoryTable)
            => $"SELECT 1 FROM {escapedHistoryTable} LIMIT 1";

        /// <summary>
        /// Returns true when the DbException definitively indicates a table/object
        /// does not exist (schema error), as opposed to a permission denied or connectivity error.
        /// Only return true for definitive "object not found" schema errors so that operational
        /// failures propagate rather than being silently swallowed.
        /// </summary>
        public virtual bool IsObjectNotFoundError(DbException ex)
        {
            // Default: message-based fallback for providers without typed exception support.
            var msg = ex.Message;
            return msg.Contains("no such table", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("doesn't exist", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("does not exist", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("Invalid object name", StringComparison.OrdinalIgnoreCase)
                || msg.Contains("relation", StringComparison.OrdinalIgnoreCase) && msg.Contains("does not exist", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Returns provider-specific SQL to look up a temporal tag's timestamp.
        /// All identifiers are escaped using the provider's Escape method.
        /// </summary>
        /// <param name="paramName">The already-prefixed parameter name for the tag name value.</param>
        public virtual string GetTagLookupSql(string paramName)
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"SELECT {tsCol} FROM {table} WHERE {tagCol} = {paramName}";
        }

        /// <summary>
        /// Returns provider-specific SQL to insert a temporal tag record.
        /// All identifiers are escaped using the provider's Escape method.
        /// </summary>
        /// <param name="pTagName">The already-prefixed parameter name for the tag name value.</param>
        /// <param name="pTimestamp">The already-prefixed parameter name for the timestamp value.</param>
        public virtual string GetCreateTagSql(string pTagName, string pTimestamp)
        {
            var table = Escape("__NormTemporalTags");
            var tagCol = Escape("TagName");
            var tsCol = Escape("Timestamp");
            return $"INSERT INTO {table} ({tagCol}, {tsCol}) VALUES ({pTagName}, {pTimestamp})";
        }

        /// <summary>
        /// Indicates whether <see cref="GetCreateTagSql"/> obtains the tag timestamp
        /// from the database server clock instead of the caller-bound timestamp
        /// parameter. Providers with trigger-managed temporal history should use the
        /// same clock source for tags and history windows.
        /// </summary>
        internal virtual bool UsesDatabaseClockForTemporalTags => false;

    }
}
