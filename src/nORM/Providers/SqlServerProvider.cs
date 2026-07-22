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
    /// <summary>
    /// Database provider implementation for Microsoft SQL Server.
    /// Responsible for dialect translation, bulk operations and temporal table support.
    /// </summary>
    public sealed partial class SqlServerProvider : BulkOperationProvider
    {
        /// <summary>Maximum number of cached DataTable schemas used for bulk-delete key tables.</summary>
        private const int KeyTableSchemaCacheSize = 100;

        /// <summary>
        /// Minimum SQL Server version required (13.0 = SQL Server 2016).
        /// SQL Server 2016 introduced JSON_VALUE, STRING_SPLIT, and other features used by this provider.
        /// </summary>
        private static readonly Version MinimumSqlServerVersion = new(13, 0);

        /// <summary>SQL Server error number for "Invalid object name" (table/view does not exist).</summary>
        private const int SqlErrorObjectNotFound = 208;

        private const int SqlBulkCopySmallBatchThreshold = 512;

        private static readonly ConcurrentLruCache<TableMapping, DataTable> _keyTableSchemas = new(maxSize: KeyTableSchemaCacheSize);

        /// <summary>
        /// SQL Server ROWVERSION columns are server-managed and cannot receive explicit values on INSERT.
        /// Exclude them from the INSERT column list; the server populates them automatically.
        /// </summary>
        public override Column[] GetInsertColumns(TableMapping m)
            => m.InsertColumns.Where(c => !c.IsTimestamp).ToArray();

        // SQL Server does NOT flip SupportsConventionKeyStoreGeneration on. Unlike MySQL/PostgreSQL — whose
        // conventions accept an explicit value into a plain-INT column, so they are backward-compatible with an
        // existing non-identity table — SQL Server realizes the convention key as IDENTITY and must emit
        // SET IDENTITY_INSERT for explicit-value inserts, which FAILS on any table whose key is not actually an
        // identity column. That breaks every existing plain-INT (non-IDENTITY) table AND a large swath of
        // nORM's own live test suite (~20+ classes create raw plain-INT-PK tables and seed explicit keys). The
        // benefit (zero-config store-gen vs one [DatabaseGenerated(Identity)] annotation) does not justify that
        // blast radius, so SQL Server stays opt-in. The IDENTITY_INSERT hooks below stay inert until the flip.
        // See honor_nonzero_key_convention_plan.

        /// <summary>
        /// An IDENTITY column rejects an explicitly-supplied value unless IDENTITY_INSERT is ON; the
        /// convention key's explicit-value run wraps its batch/command in this. The statement runs on the
        /// same connection as the INSERT and is paired with <see cref="GetIdentityInsertDisableSql"/>. Inert
        /// until SQL Server enables the store-generated-key convention.
        /// </summary>
        internal override string GetIdentityInsertEnableSql(TableMapping m) => $"SET IDENTITY_INSERT {m.EscTable} ON;";

        /// <summary>Restores the default after the explicit-value insert (see <see cref="GetIdentityInsertEnableSql"/>).</summary>
        internal override string GetIdentityInsertDisableSql(TableMapping m) => $"SET IDENTITY_INSERT {m.EscTable} OFF;";

        /// <summary>
        /// ROWVERSION regenerates on every UPDATE; read the fresh token back inline
        /// so the tracked instance's next save compares against the current value.
        /// </summary>
        internal override string GetUpdateTokenOutputClause(TableMapping m)
            => m.TimestampColumn is { } tokenColumn ? $" OUTPUT INSERTED.{tokenColumn.EscCol}" : string.Empty;

        /// <summary>
        /// ROWVERSION is generated on INSERT as well; for application-supplied keys
        /// (no identity retrieval) read it back inline so the tracked instance's
        /// first UPDATE or DELETE does not compare a stale in-memory token.
        /// </summary>
        internal override string GetInsertTokenOutputClause(TableMapping m)
            => m.TimestampColumn is { } tokenColumn ? $" OUTPUT INSERTED.{tokenColumn.EscCol}" : string.Empty;

        /// <summary>
        /// SQL Server uses TOP(n)/OFFSET-FETCH paging syntax rather than LIMIT.
        /// </summary>
        public override bool UsesFetchOffsetPaging => true;

        /// <summary>
        /// Microsoft.Data.SqlClient's async reader path is measurably slower for the small,
        /// already-buffered result shapes nORM's runtime query fast paths target. Prefer the
        /// synchronous reader path there while leaving the broader async pipeline unchanged.
        /// </summary>
        internal override bool PrefersSyncFastPathExecution => true;

        // ROWVERSION auto-increments on every UPDATE and is read back via the OUTPUT clause, so nORM
        // must not client-manage the [Timestamp] token here.
        internal override bool SupportsNativeRowVersion => true;

        internal override bool PrefersSyncCompiledQueryExecution => true;

        /// <summary>
        /// T-SQL has no boolean value type, so a predicate used as a comparable
        /// value converts through CASE to BIT.
        /// </summary>
        internal override string BooleanPredicateAsValueSql(string predicateSql)
            => $"CAST(CASE WHEN {predicateSql} THEN 1 ELSE 0 END AS BIT)";

        /// <summary>
        /// SqlCommand.Prepare requires explicit Size/Scale metadata on every
        /// variable-length and temporal parameter and throws when any is missing,
        /// while SQL Server's ad-hoc plan cache already parameterizes batched
        /// writes — preparing adds failure modes without a performance win.
        /// </summary>
        internal override bool SupportsPreparedBatchCommands => false;

        internal override bool PrefersSyncQueryPlanExecution => true;

        // SQL Server T-SQL does not support row-tuple comparisons in WHERE IN
        // (e.g. WHERE (pk1, pk2) IN (SELECT ...)). Composite-PK CUD uses a JOIN form instead.
        internal override bool SupportsRowTupleComparison => false;

        /// <inheritdoc />
        public override string ForceCaseSensitiveStringComparison(string sql) => $"{sql} COLLATE Latin1_General_100_BIN2";

        private readonly IDbParameterFactory? _parameterFactory;
        private readonly bool _isDialectOnly;

        /// <summary>Default constructor using the built-in Microsoft.Data.SqlClient parameter factory.</summary>
        public SqlServerProvider()
        {
            _parameterFactory = null;
            _isDialectOnly = false;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SqlServerProvider"/> class with an injected
        /// parameter factory. Pass a non-SqlClient factory to run the provider in dialect-only mode -
        /// useful for cross-provider parity tests where SQL Server dialect is exercised against a
        /// foreign engine such as SQLite. Native connection-type and server-version validation is
        /// skipped in dialect-only mode.
        /// </summary>
        /// <param name="parameterFactory">Factory responsible for creating database parameters.</param>
        public SqlServerProvider(IDbParameterFactory parameterFactory)
        {
            _parameterFactory = parameterFactory ?? throw new ArgumentNullException(nameof(parameterFactory));
            _isDialectOnly = true;
        }

        /// <summary>
        /// Maximum length of a single SQL statement supported by SQL Server.
        /// </summary>
        /// <remarks>
        /// SQL Server supports batch sizes up to 65,536 x 4,096 bytes of network packet data.
        /// The previous value of 8,000 was incorrectly derived from the max row size, not the
        /// max query text size. SQL Server's actual query text limit is governed by
        /// max_recursion and memory, not a fixed character count. Using 256 MB as a safe ceiling
        /// avoids rejecting valid wide or complex queries.
        /// </remarks>
        public override int MaxSqlLength => 268_435_456;

        /// <summary>
        /// Maximum number of parameters allowed in a single SQL Server command.
        /// </summary>
        public override int MaxParameters => 2_100;

        /// <inheritdoc />
        public override ProviderCapabilities Capabilities => new(
            "SQL Server",
            MinimumSqlServerVersion,
            MaxParameters,
            true,
            true,
            true,
            true,
            "Requires Microsoft.Data.SqlClient and SQL Server 2016 or newer.");

        /// <summary>
        /// Escapes an identifier such as a table or column name using SQL Server brackets.
        /// Handles multi-part identifiers (schema.table) correctly by escaping each part.
        /// </summary>
        /// <param name="id">The identifier to escape (e.g., "table" or "schema.table").</param>
        /// <returns>The escaped identifier (e.g., "[table]" or "[schema].[table]").</returns>
        /// <remarks>
        /// Properly escapes multi-part identifiers.
        /// "schema.table" becomes "[schema].[table]", not "[schema.table]".
        /// </remarks>
        public override string Escape(string id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return id;

            // Split by dot and escape each part individually to support schema.table notation
            if (id.Contains('.'))
            {
                // ID-7: Double embedded ] characters to prevent SQL injection via identifiers
                return string.Join(".", id.Split('.').Select(part => $"[{part.Replace("]", "]]")}]"));
            }

            // ID-7: Double embedded ] characters to prevent SQL injection via identifiers
            return $"[{id.Replace("]", "]]")}]";
        }

        /// <summary>
        /// SQL Server does not support <c>LIMIT</c>; the single-row correlated subquery uses
        /// <c>SELECT TOP 1 ... ORDER BY ...</c> instead.
        /// </summary>
        public override string BuildCorrelatedTopOneSubquery(string selectSql, string tableSql, string alias, string whereSql, string orderBySql)
            => $"(SELECT TOP 1 {selectSql} FROM {tableSql} {alias} WHERE {whereSql} ORDER BY {orderBySql})";

        /// <summary>T-SQL pages with OFFSET/FETCH rather than LIMIT/OFFSET.</summary>
        internal override string BuildCorrelatedSingleRowSubquery(string selectSql, string tableSql, string alias, string whereSql, string orderBySql, string? offsetSql)
            => offsetSql == null
                ? BuildCorrelatedTopOneSubquery(selectSql, tableSql, alias, whereSql, orderBySql)
                : $"(SELECT {selectSql} FROM {tableSql} {alias} WHERE {whereSql} ORDER BY {orderBySql} OFFSET {offsetSql} ROWS FETCH NEXT 1 ROWS ONLY)";

        /// <summary>
        /// SQL Server has no <c>LIMIT</c>; the single-row scalar subquery injects <c>TOP 1</c>
        /// after the leading <c>SELECT</c> (before any <c>DISTINCT</c>), keeping any ORDER BY.
        /// With an offset (ElementAt) it uses <c>OFFSET n ROWS FETCH NEXT 1 ROWS ONLY</c>, which
        /// relies on the inner ORDER BY the ElementAt path guarantees.
        /// </summary>
        internal override string BuildScalarLimitedSubquery(string innerSelectSql, string? offsetSql)
        {
            if (offsetSql != null)
                return $"({innerSelectSql} OFFSET {offsetSql} ROWS FETCH NEXT 1 ROWS ONLY)";
            const string sel = "SELECT ";
            return innerSelectSql.StartsWith(sel, StringComparison.OrdinalIgnoreCase)
                ? $"(SELECT TOP 1 {innerSelectSql.Substring(sel.Length)})"
                : $"({innerSelectSql})";
        }

        /// <summary>T-SQL rejects subqueries inside GROUP BY; CROSS APPLY exposes the key as a column.</summary>
        internal override string? AppliedScalarColumnClause(string scalarSql, string escapedAlias, string escapedColumn)
            => $" CROSS APPLY (SELECT {scalarSql} AS {escapedColumn}) {escapedAlias}";

        /// <summary>T-SQL cannot select a bare predicate; CASE-to-BIT materializes it as bool.</summary>
        internal override string BooleanPredicateAsValue(string predicateSql)
            => $"CAST(CASE WHEN {predicateSql} THEN 1 ELSE 0 END AS BIT)";

        /// <summary>
        /// Adds SQL Server paging clauses to the SQL builder using <c>OFFSET</c> and <c>FETCH</c>.
        /// </summary>
        /// <param name="sb">The SQL builder to append to.</param>
        /// <param name="limit">Maximum number of rows to return.</param>
        /// <param name="offset">Number of rows to skip before starting to return rows.</param>
        /// <param name="limitParameterName">Parameter name for the limit value.</param>
        /// <param name="offsetParameterName">Parameter name for the offset value.</param>
        public override void ApplyPaging(OptimizedSqlBuilder sb, int? limit, int? offset, string? limitParameterName, string? offsetParameterName)
        {
            EnsureValidParameterName(limitParameterName, nameof(limitParameterName));
            EnsureValidParameterName(offsetParameterName, nameof(offsetParameterName));

            bool hasLimit = limitParameterName != null || limit.HasValue;
            bool hasOffset = offsetParameterName != null || offset.HasValue;
            if (hasLimit || hasOffset)
            {
                // Use case-insensitive comparison so that lower-case or mixed-case
                // ORDER BY clauses (e.g. "order by name") are detected correctly and a
                // duplicate ORDER BY is not appended.
                if (!HasTopLevelOrderBy(sb.ToString())) sb.Append(" ORDER BY (SELECT NULL)");
                // T-SQL FETCH rejects a row count below one, but LINQ's Take contract
                // for a non-positive count is an EMPTY window (and the Take-then-Skip
                // rewrite can produce a negative composite even from valid inputs).
                // Empty windows route the OFFSET past any addressable row instead of
                // emitting an invalid FETCH. Parameterized counts embed that decision
                // as a CASE: the engine rejects a BARE CASE (and any bigint-typed
                // expression) in OFFSET/FETCH but accepts an int-typed CASE inside
                // arithmetic, hence the "(0 + CASE ...)" shape and the int-max
                // sentinel (both verified against a live server).
                string offsetSql = offsetParameterName ?? (offset.HasValue ? offset.Value.ToString() : "0");
                if (limitParameterName != null)
                {
                    sb.Append(" OFFSET (0 + CASE WHEN (").Append(limitParameterName)
                      .Append(") < 1 THEN 2147483647 ELSE (").Append(offsetSql).Append(") END) ROWS");
                    sb.Append(" FETCH NEXT (0 + CASE WHEN (").Append(limitParameterName)
                      .Append(") < 1 THEN 1 ELSE (").Append(limitParameterName).Append(") END) ROWS ONLY");
                }
                else if (limit.HasValue && limit.Value <= 0)
                {
                    sb.Append(" OFFSET 2147483647 ROWS");
                }
                else
                {
                    sb.Append(" OFFSET ").Append(offsetSql).Append(" ROWS");
                    if (limit.HasValue)
                        sb.Append(" FETCH NEXT ").Append(limit.Value).Append(" ROWS ONLY");
                }
            }
        }

        /// <summary>
        /// Returns <c>true</c> if the SQL string contains an <c>ORDER BY</c> clause at the
        /// top-level scope (depth-0 parentheses). Uses a mini-lexer that skips:
        /// <list type="bullet">
        ///   <item>single-quoted string literals (<c>'...'</c>, <c>''</c> escape),</item>
        ///   <item>double-quoted identifiers (<c>"..."</c>),</item>
        ///   <item>bracket-quoted identifiers (<c>[...]</c>),</item>
        ///   <item>line comments (<c>-- comment</c>),</item>
        ///   <item>block comments (<c>/* comment */</c>).</item>
        /// </list>
        /// This prevents an <c>ORDER BY</c> appearing inside a comment or literal from being
        /// mistaken for a real ORDER BY clause (Q1 fix).
        /// </summary>
        private static bool HasTopLevelOrderBy(string sql)
        {
            int depth = 0;
            int i = 0;
            int len = sql.Length;
            while (i < len)
            {
                var ch = sql[i];

                // Skip line comments: -- skip to end of line
                if (ch == '-' && i + 1 < len && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < len && sql[i] != '\n' && sql[i] != '\r') i++;
                    continue;
                }

                // Skip block comments: /* skip to matching */
                if (ch == '/' && i + 1 < len && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i + 1 < len && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                    if (i + 1 < len) i += 2; // consume */
                    continue;
                }

                // Skip single-quoted string literals ('...', '' escape)
                if (ch == '\'')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == '\'') { i++; if (i < len && sql[i] == '\'') i++; else break; }
                        else i++;
                    }
                    continue;
                }

                // Skip double-quoted identifiers ("...", "" escape)
                if (ch == '"')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == '"') { i++; if (i < len && sql[i] == '"') i++; else break; }
                        else i++;
                    }
                    continue;
                }

                // Skip bracket-quoted identifiers ([...]) -- SQL Server specific
                // Q1 fix: handle escaped ]] inside brackets (]] represents a literal ] character).
                // Without this, [a]]ORDER BYb] would terminate at the first ] and expose
                // "ORDER BYb]" to the ORDER BY scanner, producing a false positive.
                if (ch == '[')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == ']')
                        {
                            i++;
                            // ]] is an escaped literal ] -- continue scanning inside the identifier
                            if (i < len && sql[i] == ']') { i++; continue; }
                            // Single ] closes the identifier
                            break;
                        }
                        i++;
                    }
                    continue;
                }

                if (ch == '(') { depth++; i++; continue; }
                if (ch == ')') { depth--; i++; continue; }

                if (depth == 0 && i + 8 <= len &&
                    (sql[i] == 'O' || sql[i] == 'o') &&
                    string.Compare(sql, i, "ORDER BY", 0, 8, StringComparison.OrdinalIgnoreCase) == 0)
                    return true;

                i++;
            }
            return false;
        }
    }
}
