using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using nORM.Configuration;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        /// <summary>
        /// Syncs the many-to-many join table rows for a single owner entity.
        /// For Added/Modified owners: computes delta from snapshot, DELETEs removed rows, INSERTs new rows.
        /// For Deleted owners: DELETEs ALL join rows for this entity.
        /// </summary>
        private async Task ExecuteJoinTableSyncAsync(object entity, EntityEntry entry, DbTransaction? transaction, CancellationToken ct)
        {
            var map = entry.Mapping;
            var tenantId = Options.TenantProvider != null ? GetRequiredTenantId(map, "many-to-many sync") : null;
            var leftTenantCol = Options.TenantProvider != null ? RequireTenantColumn(map, "many-to-many sync") : null;
            var hasTenantFilter = Options.TenantProvider != null;

            foreach (var jtm in map.ManyToManyJoins)
            {
                var leftKeyValues = jtm.GetLeftKeyValues(entity);
                if (leftKeyValues == null) continue;
                var leftPk = jtm.CreateLeftKeyFromValues(leftKeyValues);
                if (leftPk == null) continue;

                await using var cmdScope = new CommandScope(RawConnection, transaction);
                await using var cmd = cmdScope.CreateCommand();

                if (entry.State == EntityState.Deleted)
                {
                    cmd.Parameters.Clear();
                    var leftParamNames = AddKeyParams(cmd, _p.ParamPrefix, "lpk", leftKeyValues);
                    var tenantFilter = BuildJoinTenantFilter(map, jtm.LeftKeyColumns, leftTenantCol, leftParamNames, $"{_p.ParamPrefix}jtenant");
                    cmd.CommandText = $"DELETE FROM {jtm.EscTableName} WHERE {BuildJoinTablePredicate(jtm.EscLeftFkColumns, leftParamNames)}{tenantFilter}";
                    if (hasTenantFilter)
                        cmd.AddParam($"{_p.ParamPrefix}jtenant", tenantId!);
                    await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                    continue;
                }

                // Compute current set of right PKs from the collection.
                // SEC1: Also keep a pk?entity map so we can validate right-entity tenant before INSERT.
                var collection = jtm.LeftCollectionGetter(entity);
                var currentSet = new HashSet<object>();
                var rightEntityByPk = new Dictionary<object, object>();
                var rightKeyValuesByPk = new Dictionary<object, object?[]>();
                if (collection != null)
                {
                    foreach (var item in collection)
                    {
                        if (item == null) continue;
                        var rightValues = jtm.GetRightKeyValues(item);
                        if (rightValues == null) continue;
                        var rpk = jtm.CreateRightKeyFromValues(rightValues);
                        if (rpk != null)
                        {
                            currentSet.Add(rpk);
                            rightEntityByPk[rpk] = item;
                            rightKeyValuesByPk[rpk] = rightValues;
                        }
                    }
                }

                // For Added entities, snapshot is irrelevant - insert everything.
                // For Modified entities, use the snapshot to compute delta.
                HashSet<object> snapshot;
                if (entry.State == EntityState.Added ||
                    entry.ManyToManySnapshots == null ||
                    !entry.ManyToManySnapshots.TryGetValue(jtm.LeftNavPropertyName, out var snap))
                {
                    snapshot = new HashSet<object>();
                }
                else
                {
                    snapshot = snap;
                }

                var toAdd = currentSet.Except(snapshot).ToList();
                var toRemove = snapshot.Except(currentSet).ToList();

                // DELETE removed join rows, scoped to current tenant
                foreach (var removedPk in toRemove)
                {
                    var removedValues = jtm.GetRightKeyValuesFromKey(removedPk);
                    if (removedValues == null) continue;

                    cmd.Parameters.Clear();
                    var leftParamNames = AddKeyParams(cmd, _p.ParamPrefix, "lp", leftKeyValues);
                    var rightParamNames = AddKeyParams(cmd, _p.ParamPrefix, "rp", removedValues);
                    var tenantFilter = BuildJoinTenantFilter(map, jtm.LeftKeyColumns, leftTenantCol, leftParamNames, $"{_p.ParamPrefix}jtenant");
                    cmd.CommandText = $"DELETE FROM {jtm.EscTableName} WHERE {BuildJoinTableMergedPredicate(jtm, leftParamNames, rightParamNames)}{tenantFilter}";
                    if (hasTenantFilter)
                        cmd.AddParam($"{_p.ParamPrefix}jtenant", tenantId!);
                    await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                }

                // INSERT new join rows (idempotent / ignore duplicates)
                foreach (var addedPk in toAdd)
                {
                    if (!rightKeyValuesByPk.TryGetValue(addedPk, out var addedRightValues))
                        continue;

                    // SEC1: Validate right entity tenant before inserting the join row to prevent
                    // cross-tenant join-table contamination when tenancy is active on the left side.
                    if (Options.TenantProvider != null && rightEntityByPk.TryGetValue(addedPk, out var rightEntity))
                    {
                        var rightMap = GetMapping(jtm.RightType);
                        var rightTenantCol = RequireTenantColumn(rightMap, "many-to-many sync right side");
                        var rightTenant = rightTenantCol.Getter(rightEntity);
                        if (rightTenant == null)
                            throw new NormConfigurationException(
                                $"Cannot add M2M relation: related entity '{jtm.RightType.Name}' has null tenant ID. " +
                                "Explicitly set the tenant ID on the related entity.");
                        if (!TenantIdsEqual(rightTenant, tenantId!))
                            throw new NormConfigurationException(
                                $"Cannot add cross-tenant M2M relation: related entity tenant '{rightTenant}' " +
                                $"does not match current tenant '{tenantId}'.");
                    }

                    cmd.Parameters.Clear();
                    var leftParamNames = AddKeyParams(cmd, _p.ParamPrefix, "lp", leftKeyValues);
                    var rightParamNames = AddKeyParams(cmd, _p.ParamPrefix, "rp", addedRightValues);
                    cmd.CommandText = BuildJoinTableInsertIfMissingSql(jtm, leftParamNames, rightParamNames);
                    await cmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                }
            }
        }


        private static string[] AddKeyParams(DbCommand cmd, string prefix, string baseName, IReadOnlyList<object?> values)
        {
            var names = new string[values.Count];
            for (var i = 0; i < values.Count; i++)
            {
                names[i] = $"{prefix}{baseName}{i}";
                cmd.AddParam(names[i], values[i]!);
            }

            return names;
        }

        private static string BuildJoinTablePredicate(IReadOnlyList<string> escapedColumns, IReadOnlyList<string> parameterNames)
        {
            if (escapedColumns.Count != parameterNames.Count)
                throw new NormConfigurationException(
                    $"Many-to-many join predicate expected {escapedColumns.Count} parameters but received {parameterNames.Count}.");

            var sb = new StringBuilder();
            for (var i = 0; i < escapedColumns.Count; i++)
            {
                if (i > 0)
                    sb.Append(" AND ");
                sb.Append(escapedColumns[i]).Append(" = ").Append(parameterNames[i]);
            }

            return sb.ToString();
        }

        private static string BuildJoinTableMergedPredicate(JoinTableMapping jtm, IReadOnlyList<string> leftParameterNames, IReadOnlyList<string> rightParameterNames)
        {
            var (columns, parameters) = BuildJoinTableMergedColumnParameters(jtm, leftParameterNames, rightParameterNames);
            return BuildJoinTablePredicate(columns, parameters);
        }

        private static string BuildJoinTenantFilter(TableMapping map, IReadOnlyList<Column> leftKeyColumns, Column? tenantColumn, IReadOnlyList<string> leftParameterNames, string tenantParameterName)
        {
            if (tenantColumn == null)
                return "";

            var entityKeyPredicate = BuildJoinTablePredicate(leftKeyColumns.Select(c => c.EscCol).ToArray(), leftParameterNames);
            return $" AND EXISTS (SELECT 1 FROM {map.EscTable} WHERE {entityKeyPredicate} AND {tenantColumn.EscCol} = {tenantParameterName})";
        }

        private static string BuildJoinTableInsertIfMissingSql(JoinTableMapping jtm, IReadOnlyList<string> leftParameterNames, IReadOnlyList<string> rightParameterNames)
        {
            var (columns, parameters) = BuildJoinTableMergedColumnParameters(jtm, leftParameterNames, rightParameterNames);
            var predicate = BuildJoinTablePredicate(columns, parameters);

            return $"INSERT INTO {jtm.EscTableName} ({string.Join(", ", columns)}) SELECT {string.Join(", ", parameters)} WHERE NOT EXISTS (SELECT 1 FROM {jtm.EscTableName} WHERE {predicate})";
        }

        private static (IReadOnlyList<string> Columns, IReadOnlyList<string> Parameters) BuildJoinTableMergedColumnParameters(
            JoinTableMapping jtm,
            IReadOnlyList<string> leftParameterNames,
            IReadOnlyList<string> rightParameterNames)
        {
            if (jtm.EscLeftFkColumns.Count != leftParameterNames.Count)
                throw new NormConfigurationException(
                    $"Many-to-many join insert expected {jtm.EscLeftFkColumns.Count} left parameters but received {leftParameterNames.Count}.");
            if (jtm.EscRightFkColumns.Count != rightParameterNames.Count)
                throw new NormConfigurationException(
                    $"Many-to-many join insert expected {jtm.EscRightFkColumns.Count} right parameters but received {rightParameterNames.Count}.");

            var columns = new List<string>(jtm.EscLeftFkColumns.Count + jtm.EscRightFkColumns.Count);
            var parameters = new List<string>(columns.Capacity);
            var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            AddUnique(jtm.EscLeftFkColumns, leftParameterNames);
            AddUnique(jtm.EscRightFkColumns, rightParameterNames);
            return (columns, parameters);

            void AddUnique(IReadOnlyList<string> sourceColumns, IReadOnlyList<string> sourceParameters)
            {
                for (var i = 0; i < sourceColumns.Count; i++)
                {
                    if (!seen.Add(sourceColumns[i]))
                        continue;
                    columns.Add(sourceColumns[i]);
                    parameters.Add(sourceParameters[i]);
                }
            }
        }
    }
}
