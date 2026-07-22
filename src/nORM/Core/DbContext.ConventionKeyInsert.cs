using System;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using nORM.Internal;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        // Direct-path INSERT helpers for a store-generated convention key (EF Core parity) whose value is at
        // its default: the key column is omitted so the database generates it, and the value is read back. The
        // batched SaveChanges path lives in DbContext.WriteBatches.cs; these serve the direct/prepared
        // InsertAsync paths (and the MySQL command-generated batch path uses BuildInsertFromColumns).

        /// <summary>
        /// Inserts an entity whose store-generated convention key is at its default value: the key column is
        /// omitted so the database generates it, and the generated value is read back onto the entity via the
        /// provider's identity-retrieval clause (RETURNING on SQLite/PostgreSQL, LAST_INSERT_ID on MySQL).
        /// Mirrors how a DB-generated key is hydrated on the direct write paths. Explicit (non-default)
        /// convention keys never reach here — they insert the value as-is on the normal path. The caller
        /// performs any tracker accept (the prepared path via AcceptTrackedInsert, the transactional path via
        /// SyncTrackerAfterDirectWrite).
        /// </summary>
        private async Task<int> ExecuteConventionDefaultInsertAsync(object entity, TableMapping map, DbTransaction? transaction, CancellationToken ct)
        {
            await using var scope = new CommandScope(RawConnection, transaction);
            await using var cmd = scope.CreateCommand();
            var cols = map.InsertColumnsWithoutConventionKey;
            cmd.CommandText = BuildConventionDefaultInsertSql(map, cols);
            cmd.CommandTimeout = ToSecondsClamped(Options.TimeoutConfiguration.BaseTimeout);
            AddParametersOptimized(cmd, map, entity, WriteOperation.Insert, insertColumns: cols);
            var newId = await cmd.ExecuteScalarWithInterceptionAsync(this, ct).ConfigureAwait(false);
            if (newId != null && newId != DBNull.Value)
                map.SetPrimaryKey(entity, newId);
            Options.CacheProvider?.InvalidateTag(map.TableName);
            return 1;
        }

        /// <summary>
        /// Builds the INSERT for a store-generated convention key's default-value row — the key column omitted
        /// (<paramref name="cols"/> = <see cref="TableMapping.InsertColumnsWithoutConventionKey"/>) plus the
        /// provider's identity-retrieval clause to read the generated key back. Uses @PropName parameters to
        /// match <see cref="AddParametersOptimized"/>. Mirrors <see cref="Providers.DatabaseProvider.BuildInsert"/>.
        /// </summary>
        private string BuildConventionDefaultInsertSql(TableMapping map, Column[] cols)
        {
            var prefix = _p.GetIdentityRetrievalPrefix(map);
            var fragment = _p.GetIdentityRetrievalString(map);
            if (cols.Length == 0)
                return $"INSERT INTO {map.EscTable}{prefix} {_p.DefaultValuesInsertClause}{fragment}";
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var valParams = string.Join(", ", cols.Select(c => _p.ParamPrefix + c.PropName));
            return $"INSERT INTO {map.EscTable} ({colNames}){prefix} VALUES ({valParams}){fragment}";
        }

        /// <summary>
        /// Plain INSERT for exactly <paramref name="cols"/> with no identity-retrieval clause — used by the
        /// command-generated-key batch path (MySQL), which reads the generated key from the command object
        /// (LAST_INSERT_ID) rather than a returned result set. For the convention key's default-value run
        /// <paramref name="cols"/> omits the key. Uses @PropName parameters to match <see cref="AddParametersOptimized"/>.
        /// </summary>
        private string BuildInsertFromColumns(TableMapping map, Column[] cols)
        {
            if (cols.Length == 0)
                return $"INSERT INTO {map.EscTable} {_p.DefaultValuesInsertClause}";
            var colNames = string.Join(", ", cols.Select(c => c.EscCol));
            var valParams = string.Join(", ", cols.Select(c => _p.ParamPrefix + c.PropName));
            return $"INSERT INTO {map.EscTable} ({colNames}) VALUES ({valParams})";
        }
    }
}
