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
        /// Inserts, updates or deletes owned collection items for a single owner entity.
        /// For Added owners: INSERT all items. For Modified owners: DELETE then INSERT.
        /// For Deleted owners: DELETE all items (called BEFORE the owner is deleted).
        /// </summary>
        private async Task SaveOwnedCollectionsAsync(object owner, TableMapping ownerMap, EntityState ownerState, DbTransaction? transaction, CancellationToken ct)
        {
            if (ownerMap.KeyColumns.Length == 0) return;

            foreach (var ownedMap in ownerMap.OwnedCollections)
            {
                // Resolve which owner key column the FK on the owned table references.
                // For single-key owners this is trivial; for composite-key owners we use
                // name matching to find the right key column rather than always using index 0.
                var ownerKeyCol = ResolveOwnerKeyColumnForOwnedFk(ownerMap.KeyColumns, ownedMap.ForeignKeyColumn, ownerMap.Type.Name);
                var ownerKey = ownerKeyCol.Getter(owner);
                if (ownerKey == null) continue;

                if (ownerState == EntityState.Modified || ownerState == EntityState.Deleted)
                {
                    // DELETE existing owned items - use a dedicated command so that the
                    // INSERT command below starts fully fresh (no prepared-statement residue).
                    await using var delScope = new CommandScope(RawConnection, transaction);
                    await using var delCmd = delScope.CreateCommand();
                    var delSql = $"DELETE FROM {ownedMap.EscTable} WHERE {ownedMap.EscForeignKeyColumn} = @ownerPk";
                    var dp = delCmd.CreateParameter();
                    dp.ParameterName = "@ownerPk";
                    dp.Value = ownerKey;
                    delCmd.Parameters.Add(dp);

                    // X1: Scope DELETE to current tenant when multi-tenancy is configured
                    // on the owned child table, preventing cross-tenant data destruction.
                    if (Options.TenantProvider != null)
                    {
                        var ownedTenantCol = Array.Find(ownedMap.Columns, c => c.PropName == Options.TenantColumnName)
                            ?? throw new NormConfigurationException(
                                $"TenantProvider is configured, but owned collection '{ownedMap.OwnedType.Name}' " +
                                $"does not map tenant column '{Options.TenantColumnName}'. nORM fails closed for tenant-scoped owned deletes.");
                        delSql += $" AND {ownedTenantCol.EscCol} = @tenantId";
                        var tp = delCmd.CreateParameter();
                        tp.ParameterName = "@tenantId";
                        tp.Value = GetRequiredTenantId(ownedTenantCol, ownedMap.OwnedType, "owned collection delete");
                        delCmd.Parameters.Add(tp);
                    }

                    delCmd.CommandText = delSql;
                    await delCmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                }

                if (ownerState == EntityState.Deleted) continue; // no re-insert for deleted owners

                // INSERT all current owned items
                var collection = ownedMap.CollectionGetter(owner);
                if (collection == null) continue;
                var items = ((System.Collections.IEnumerable)collection).Cast<object>().ToList();
                if (items.Count == 0) continue;

                var insertCols = Array.FindAll(ownedMap.Columns, c => !c.IsDbGenerated);
                var colNames = string.Join(", ", insertCols.Select(c => c.EscCol).Prepend(ownedMap.EscForeignKeyColumn));

                // SAVE1: Detect tenant column on owned table once before the INSERT loop.
                Column? insertTenantCol = null;
                object? insertTenantId = null;
                if (Options.TenantProvider != null)
                {
                    insertTenantCol = Array.Find(insertCols, c => c.PropName == Options.TenantColumnName)
                        ?? throw new NormConfigurationException(
                            $"TenantProvider is configured, but owned collection '{ownedMap.OwnedType.Name}' " +
                            $"does not map tenant column '{Options.TenantColumnName}'. nORM fails closed for tenant-scoped owned inserts.");
                    insertTenantId = GetRequiredTenantId(insertTenantCol, ownedMap.OwnedType, "owned collection insert");
                }

                // INSERT each item individually - avoids multi-statement batch issues
                // across providers (e.g. SQLite drivers that stop after the first statement).
                foreach (var item in items)
                {
                    // SAVE1: Validate owned child tenant before INSERT to prevent cross-tenant contamination.
                    if (insertTenantCol != null && insertTenantId != null)
                    {
                        var childTenant = insertTenantCol.Getter(item);
                        if (childTenant == null)
                            throw new NormConfigurationException(
                                $"Tenant ID is required on owned child entity before saving but was null. " +
                                "Explicitly set the tenant ID on the child entity.");
                        if (!TenantIdsEqual(childTenant, insertTenantId))
                            throw new NormConfigurationException(
                                $"Owned child tenant '{childTenant}' does not match current tenant '{insertTenantId}'.");
                    }

                    await using var insScope = new CommandScope(RawConnection, transaction);
                    await using var insCmd = insScope.CreateCommand();
                    var valuePlaceholders = new StringBuilder("@ownerFk");
                    var fkp = insCmd.CreateParameter();
                    fkp.ParameterName = "@ownerFk";
                    fkp.Value = ownerKey;
                    insCmd.Parameters.Add(fkp);
                    int pj = 0;
                    foreach (var col in insertCols)
                    {
                        var pname = $"@op{pj}";
                        valuePlaceholders.Append($", {pname}");
                        var pp = insCmd.CreateParameter();
                        pp.ParameterName = pname;
                        var rawVal = col.Getter(item);
                        if (col.Converter != null) rawVal = col.Converter.ConvertToProvider(rawVal);
                        pp.Value = rawVal ?? DBNull.Value;
                        insCmd.Parameters.Add(pp);
                        pj++;
                    }
                    insCmd.CommandText = $"INSERT INTO {ownedMap.EscTable} ({colNames}) VALUES ({valuePlaceholders});";
                    await insCmd.ExecuteNonQueryWithInterceptionAsync(this, ct).ConfigureAwait(false);
                }
            }
        }

        /// <summary>
        /// Resolves which owner key column corresponds to the FK column on an owned table.
        /// For single-key owners this is the one key column. For composite-key owners the
        /// method tries: (1) exact column name match, (2) owner-type-name-prefixed match,
        /// then falls back to the first key column.
        /// </summary>
        private static Column ResolveOwnerKeyColumnForOwnedFk(Column[] ownerKeyColumns, string ownedFkColumnName, string ownerTypeName)
        {
            if (ownerKeyColumns.Length == 1) return ownerKeyColumns[0];

            // Try 1: exact FK column name matches a key column name (e.g. FK="OrderId", key="OrderId")
            var match = Array.Find(ownerKeyColumns, c =>
                string.Equals(c.Name, ownedFkColumnName, StringComparison.OrdinalIgnoreCase));
            if (match != null) return match;

            // Try 2: strip owner type name prefix (e.g. FK="OrderId", owner type="Order" ? "Id", key="Id")
            if (ownedFkColumnName.Length > ownerTypeName.Length &&
                ownedFkColumnName.StartsWith(ownerTypeName, StringComparison.OrdinalIgnoreCase))
            {
                var suffix = ownedFkColumnName.Substring(ownerTypeName.Length);
                match = Array.Find(ownerKeyColumns, c =>
                    string.Equals(c.Name, suffix, StringComparison.OrdinalIgnoreCase));
                if (match != null) return match;
            }

            // Fall back to first key column (preserves legacy single-key behavior)
            return ownerKeyColumns[0];
        }


        /// <summary>
        /// Loads owned collection items for all given owner entities using a single IN-query per collection.
        /// </summary>
        internal async Task LoadOwnedCollectionsAsync(System.Collections.IList owners, TableMapping ownerMap, CancellationToken ct)
        {
            if (ownerMap.KeyColumns.Length == 0 || owners.Count == 0) return;

            foreach (var ownedMap in ownerMap.OwnedCollections)
            {
                // Resolve which owner key column this FK references (composite-key aware).
                var pkCol = ResolveOwnerKeyColumnForOwnedFk(ownerMap.KeyColumns, ownedMap.ForeignKeyColumn, ownerMap.Type.Name);

                // Build PK ? owner lookup keyed by the FK-referenced key column value.
                var ownerByPk = new Dictionary<object, object>(owners.Count);
                foreach (var owner in owners)
                {
                    if (owner == null) continue;
                    var pk = pkCol.Getter(owner);
                    if (pk != null && !ownerByPk.ContainsKey(pk))
                        ownerByPk[pk] = owner;
                }
                if (ownerByPk.Count == 0) continue;
                // SELECT owned cols + fk_col FROM child_table WHERE fk_col IN (@p0, @p1, ...)
                var pks = ownerByPk.Keys.ToArray();
                var sqlBuilder = new StringBuilder();
                sqlBuilder.Append("SELECT ");
                for (int ci = 0; ci < ownedMap.Columns.Length; ci++)
                {
                    if (ci > 0) sqlBuilder.Append(", ");
                    sqlBuilder.Append(ownedMap.Columns[ci].EscCol);
                }
                if (ownedMap.Columns.Length > 0) sqlBuilder.Append(", ");
                sqlBuilder.Append(ownedMap.EscForeignKeyColumn);
                sqlBuilder.Append(" FROM ").Append(ownedMap.EscTable)
                          .Append(" WHERE ").Append(ownedMap.EscForeignKeyColumn).Append(" IN (");
                for (int pi = 0; pi < pks.Length; pi++)
                {
                    if (pi > 0) sqlBuilder.Append(", ");
                    sqlBuilder.Append(_p.ParamPrefix).Append("lpk").Append(pi);
                }
                sqlBuilder.Append(')');

                // X1: Scope SELECT to current tenant when multi-tenancy is configured
                // on the owned child table, preventing cross-tenant data leakage.
                Column? ownedTenantColLoad = null;
                if (Options.TenantProvider != null)
                    ownedTenantColLoad = Array.Find(ownedMap.Columns, c => c.PropName == Options.TenantColumnName)
                        ?? throw new NormConfigurationException(
                            $"TenantProvider is configured, but owned collection '{ownedMap.OwnedType.Name}' " +
                            $"does not map tenant column '{Options.TenantColumnName}'. nORM fails closed for tenant-scoped owned loads.");
                if (ownedTenantColLoad != null)
                    sqlBuilder.Append(" AND ").Append(ownedTenantColLoad.EscCol)
                              .Append(" = ").Append(_p.ParamPrefix).Append("tenantId");
                var querySql = sqlBuilder.ToString();

                await using var cmd = CreateCommand();
                cmd.CommandText = querySql;
                cmd.CommandTimeout = ToSecondsClamped(Options.TimeoutConfiguration.BaseTimeout);
                for (int i = 0; i < pks.Length; i++)
                {
                    var p = cmd.CreateParameter();
                    p.ParameterName = _p.ParamPrefix + "lpk" + i;
                    p.Value = pks[i];
                    cmd.Parameters.Add(p);
                }
                if (ownedTenantColLoad != null)
                {
                    var tp = cmd.CreateParameter();
                    tp.ParameterName = _p.ParamPrefix + "tenantId";
                    tp.Value = GetRequiredTenantId(ownedTenantColLoad, ownedMap.OwnedType, "owned collection load");
                    cmd.Parameters.Add(tp);
                }

                // Initialize empty collections on all owners first
                foreach (var owner in owners)
                {
                    if (owner == null) continue;
                    var existing = ownedMap.CollectionGetter(owner);
                    if (existing == null)
                        ownedMap.CollectionSetter(owner, Activator.CreateInstance(typeof(List<>).MakeGenericType(ownedMap.OwnedType)));
                }

                int fkOrdinal = ownedMap.Columns.Length; // FK is the last column in our SELECT
                await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    // Materialize owned item
                    var item = Activator.CreateInstance(ownedMap.OwnedType)!;
                    for (int ci = 0; ci < ownedMap.Columns.Length; ci++)
                    {
                        var col = ownedMap.Columns[ci];
                        if (reader.IsDBNull(ci)) continue;
                        var raw = reader.GetValue(ci);
                        object? converted;
                        if (col.Converter != null)
                            converted = col.Converter.ConvertFromProvider(raw);
                        else
                            converted = ConvertSimple(raw, col.Prop.PropertyType);
                        col.Setter(item, converted);
                    }

                    // Read FK and assign to owner.
                    // Type coercion fallback: ADO.NET providers may return the FK value in a different
                    // numeric type than the CLR PK property (e.g. SQLite returns Int64 for all integers
                    // while the PK property may be Int32). The initial TryGetValue uses the raw provider
                    // type for a zero-allocation fast path; on miss, ConvertSimple coerces to the PK
                    // property type so the dictionary lookup succeeds across type-width mismatches.
                    if (reader.IsDBNull(fkOrdinal)) continue;
                    var fkVal = reader.GetValue(fkOrdinal);
                    if (!ownerByPk.TryGetValue(fkVal, out var ownerEntity))
                    {
                        fkVal = ConvertSimple(fkVal, pkCol.Prop.PropertyType)!;
                        if (fkVal == null || !ownerByPk.TryGetValue(fkVal, out ownerEntity)) continue;
                    }

                    var col2 = ownedMap.CollectionGetter(ownerEntity);
                    if (col2 is System.Collections.IList list)
                        list.Add(item);
                }
            }
        }

        /// <summary>Converts a DB value to the target CLR type using safe fallback logic.</summary>
        private static object? ConvertSimple(object raw, Type targetType)
        {
            if (raw == null || raw == DBNull.Value) return null;
            var underlying = Nullable.GetUnderlyingType(targetType) ?? targetType;
            if (raw.GetType() == underlying) return raw;
            try { return Convert.ChangeType(raw, underlying); }
            catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException) { return raw; }
        }
    }
}
