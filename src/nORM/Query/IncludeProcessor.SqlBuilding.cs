using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Execution;

namespace nORM.Query
{
    /// <summary>
    /// Include-chain SQL construction: the per-level SELECT/EXISTS predicate assembly, the temporal
    /// FROM source, and the composite/join key predicate builders. Kept separate from the
    /// materialization and many-to-many partials so each stays focused on one responsibility.
    /// </summary>
    internal sealed partial class IncludeProcessor
    {
        private static string BuildColumnValuePredicate(
            DbCommand cmd,
            string paramPrefix,
            string paramBaseName,
            string? tableAlias,
            IReadOnlyList<string> escapedColumns,
            IReadOnlyList<object?[]> keyGroups)
        {
            if (keyGroups.Count == 0)
                return "1 = 0";

            var sb = new System.Text.StringBuilder();
            sb.Append('(');
            for (var groupIndex = 0; groupIndex < keyGroups.Count; groupIndex++)
            {
                if (groupIndex > 0)
                    sb.Append(" OR ");

                var values = keyGroups[groupIndex];
                if (values.Length != escapedColumns.Count)
                    throw new NormConfigurationException(
                        $"Many-to-many key predicate expected {escapedColumns.Count} values but received {values.Length}.");

                sb.Append('(');
                for (var columnIndex = 0; columnIndex < escapedColumns.Count; columnIndex++)
                {
                    if (columnIndex > 0)
                        sb.Append(" AND ");

                    var parameterName = $"{paramPrefix}{paramBaseName}{groupIndex}_{columnIndex}";
                    if (!string.IsNullOrEmpty(tableAlias))
                        sb.Append(tableAlias).Append('.');
                    sb.Append(escapedColumns[columnIndex]).Append(" = ").Append(parameterName);
                    cmd.AddParam(parameterName, values[columnIndex]!);
                }
                sb.Append(')');
            }
            sb.Append(')');
            return sb.ToString();
        }

        private static string BuildColumnValuePredicate(
            DbCommand cmd,
            string paramPrefix,
            string paramBaseName,
            string? tableAlias,
            IReadOnlyList<Column> columns,
            IReadOnlyList<object?[]> keyGroups)
            => BuildColumnValuePredicate(
                cmd,
                paramPrefix,
                paramBaseName,
                tableAlias,
                columns.Select(c => c.EscCol).ToArray(),
                keyGroups);

        private static string BuildColumnJoinPredicate(
            string leftAlias,
            IReadOnlyList<string> leftEscColumns,
            string rightAlias,
            IReadOnlyList<Column> rightColumns)
        {
            if (leftEscColumns.Count != rightColumns.Count)
                throw new NormConfigurationException(
                    $"Many-to-many join table key width {leftEscColumns.Count} does not match mapped entity key width {rightColumns.Count}.");

            var sb = new System.Text.StringBuilder();
            for (var i = 0; i < leftEscColumns.Count; i++)
            {
                if (i > 0)
                    sb.Append(" AND ");
                sb.Append(leftAlias).Append('.').Append(leftEscColumns[i])
                  .Append(" = ")
                  .Append(rightAlias).Append('.').Append(rightColumns[i].EscCol);
            }

            return sb.ToString();
        }

        private static void AppendCompositeKeyPredicate(System.Text.StringBuilder sb, TableMapping.Relation relation, string tableAlias, List<string[]> paramGroups)
        {
            if (paramGroups.Count == 0)
            {
                sb.Append("1 = 0");
                return;
            }

            sb.Append('(');
            for (var groupIndex = 0; groupIndex < paramGroups.Count; groupIndex++)
            {
                if (groupIndex > 0)
                    sb.Append(" OR ");

                var group = paramGroups[groupIndex];
                if (group.Length != relation.ForeignKeys.Count)
                    throw new NormConfigurationException(
                        $"Composite Include relationship '{relation.NavProp.Name}' expected {relation.ForeignKeys.Count} key values but received {group.Length}.");

                sb.Append('(');
                for (var columnIndex = 0; columnIndex < relation.ForeignKeys.Count; columnIndex++)
                {
                    if (columnIndex > 0)
                        sb.Append(" AND ");
                    sb.Append(tableAlias).Append('.').Append(relation.ForeignKeys[columnIndex].EscCol)
                      .Append(" = ")
                      .Append(group[columnIndex]);
                }
                sb.Append(')');
            }
            sb.Append(')');
        }

        /// <summary>
        /// Returns the FROM source for an eager-load level: the live table normally, or —
        /// when the root query runs under AsOf — the same history-window derived table the
        /// root FROM uses, so related rows reconstruct at the SAME timestamp instead of
        /// silently mixing live relations onto historical roots. The timestamp parameter is
        /// added to <paramref name="cmd"/> once and shared by every level.
        /// </summary>
        private string GetFromSource(TableMapping map, DbCommand cmd, DateTime? asOf, string innerAlias)
        {
            if (asOf == null)
                return map.EscTable;

            var pn = _ctx.RawProvider.ParamPrefix + "asof";
            if (!cmd.Parameters.Contains(pn))
                cmd.AddParam(pn, _ctx.RawProvider.FormatTemporalAsOfParameterValue(asOf.Value));

            if (_ctx.Options.TemporalStorageMode == nORM.Configuration.TemporalStorageMode.ProviderNative)
                return _ctx.RawProvider.GetProviderNativeTemporalAsOfFromClause(map, pn);

            var history = _ctx.RawProvider.Escape(map.TableName + "_History");
            var cols = string.Join(", ", map.Columns.Select(c => c.EscCol));
            var validFrom = _ctx.RawProvider.Escape("__ValidFrom");
            var validTo = _ctx.RawProvider.Escape("__ValidTo");
            return $"(SELECT {cols} FROM {history} {innerAlias} WHERE {pn} >= {innerAlias}.{validFrom} AND {pn} < {innerAlias}.{validTo})";
        }

        private string BuildSql(IReadOnlyList<TableMapping.Relation> path, TableMapping[] mappings, List<string> paramNames, List<string[]> paramGroups, DbCommand cmd, DateTime? asOf = null, IReadOnlyList<IncludeFilter?>? filters = null, Dictionary<string, object?>? filterParams = null)
        {
            var tenantActive = _ctx.Options.TenantProvider != null;
            if (tenantActive)
            {
                for (var i = 0; i < path.Count; i++)
                {
                    var tp = $"{_ctx.RawProvider.ParamPrefix}tkn{i}";
                    cmd.AddParam(tp, _ctx.GetRequiredTenantId(mappings[i], "include path load"));
                }
            }

            var sb = PooledStringBuilder.Rent();
            try
            {
                for (int i = 0; i < path.Count; i++)
                {
                    var map = mappings[i];
                    var alias = _ctx.RawProvider.Escape("__inc" + i.ToString(System.Globalization.CultureInfo.InvariantCulture));

                    var fromSource = GetFromSource(map, cmd, asOf,
                        _ctx.RawProvider.Escape("__incw" + i.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                    sb.Append("SELECT ").Append(alias).Append(".* FROM ").Append(fromSource).Append(' ').Append(alias)
                      .Append(" WHERE ");
                    AppendIncludeLevelPredicate(sb, path, mappings, i, alias, paramNames, paramGroups, tenantActive, cmd, asOf);

                    if (tenantActive)
                    {
                        var tenantCol = _ctx.RequireTenantColumn(map, "include path load");
                        var tp = $"{_ctx.RawProvider.ParamPrefix}tkn{i}";
                        sb.Append(" AND ").Append(alias).Append('.').Append(tenantCol.EscCol).Append(" = ").Append(tp);
                    }

                    // Apply the general global filters (e.g. soft-delete) to each eager-loaded level.
                    // ApplyGlobalFilters only filters the root LINQ tree; this hand-built Include SQL must
                    // repeat the predicate or a soft-deleted / cross-tenant child leaks into the graph.
                    var globalFilterSql = GlobalFilterFragment.Build(_ctx, map, alias, cmd);
                    if (globalFilterSql != null)
                        sb.Append(" AND ").Append(globalFilterSql);

                    // Filtered Include (Include(o => o.Lines.Where(pred))): the predicate was rendered to
                    // SQL against this level's alias at plan-build time; AND it on and bind the compiled
                    // parameters it references from the captured main-command values so closure captures
                    // re-bind per execution instead of freezing the first run's value into the cached plan.
                    var includeFilter = filters != null && i < filters.Count ? filters[i] : null;
                    if (includeFilter != null)
                    {
                        BindIncludeFilterParams(cmd, includeFilter, filterParams);
                        sb.Append(" AND (").Append(includeFilter.Sql).Append(')');
                    }

                    // Separate multiple result-set statements with semicolons;
                    // skip the trailing one so the final SQL is clean.
                    if (i < path.Count - 1)
                        sb.Append(';');
                }

                return sb.ToString();
            }
            finally
            {
                PooledStringBuilder.Return(sb);
            }
        }

        /// <summary>
        /// Binds the compiled parameters a filtered-Include predicate references onto the eager-load
        /// command, taking the provider values the main query already bound (converters applied) from
        /// <paramref name="filterParams"/>. Mirrors the split-query dependent-filter binding; a missing
        /// snapshot value binds DBNull rather than throwing so a malformed capture fails as a
        /// no-match, never as silently wrong rows.
        /// </summary>
        private static void BindIncludeFilterParams(DbCommand cmd, IncludeFilter filter, Dictionary<string, object?>? filterParams)
        {
            foreach (var name in filter.Parameters)
            {
                if (cmd.Parameters.Contains(name))
                    continue;
                object? value = null;
                filterParams?.TryGetValue(name, out value);
                cmd.AddParam(name, value ?? DBNull.Value);
            }
        }

        private void AppendIncludeLevelPredicate(
            System.Text.StringBuilder sb,
            IReadOnlyList<TableMapping.Relation> path,
            TableMapping[] mappings,
            int level,
            string currentAlias,
            List<string> rootParamNames,
            List<string[]> rootParamGroups,
            bool tenantActive,
            DbCommand cmd,
            DateTime? asOf)
        {
            var relation = path[level];
            if (level == 0)
            {
                if (relation.IsComposite)
                {
                    AppendCompositeKeyPredicate(sb, relation, currentAlias, rootParamGroups);
                }
                else
                {
                    sb.Append(currentAlias).Append('.').Append(relation.ForeignKey.EscCol)
                      .Append(" IN (").Append(PooledStringBuilder.Join(rootParamNames, ",")).Append(')');
                }
                return;
            }

            var previousAlias = _ctx.RawProvider.Escape("__inc" + (level - 1).ToString(System.Globalization.CultureInfo.InvariantCulture) + "_p" + level.ToString(System.Globalization.CultureInfo.InvariantCulture));
            var previousSource = GetFromSource(mappings[level - 1], cmd, asOf,
                _ctx.RawProvider.Escape("__incw" + (level - 1).ToString(System.Globalization.CultureInfo.InvariantCulture) + "_p" + level.ToString(System.Globalization.CultureInfo.InvariantCulture)));
            sb.Append("EXISTS(SELECT 1 FROM ").Append(previousSource).Append(' ').Append(previousAlias)
              .Append(" WHERE ");
            AppendIncludeLevelPredicate(sb, path, mappings, level - 1, previousAlias, rootParamNames, rootParamGroups, tenantActive, cmd, asOf);
            sb.Append(" AND ");
            AppendRelationJoinPredicate(sb, path[level], currentAlias, previousAlias);
            if (tenantActive)
            {
                var tenantCol = _ctx.RequireTenantColumn(mappings[level - 1], "include path load");
                var tp = $"{_ctx.RawProvider.ParamPrefix}tkn{level - 1}";
                sb.Append(" AND ").Append(previousAlias).Append('.').Append(tenantCol.EscCol).Append(" = ").Append(tp);
            }
            sb.Append(')');
        }

        private static void AppendRelationJoinPredicate(System.Text.StringBuilder sb, TableMapping.Relation relation, string dependentAlias, string principalAlias)
        {
            for (var i = 0; i < relation.ForeignKeys.Count; i++)
            {
                if (i > 0)
                    sb.Append(" AND ");
                sb.Append(dependentAlias).Append('.').Append(relation.ForeignKeys[i].EscCol)
                  .Append(" = ").Append(principalAlias).Append('.').Append(relation.PrincipalKeys[i].EscCol);
            }
        }
    }
}
