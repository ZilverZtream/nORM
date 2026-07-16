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
        internal async Task LoadOwnedCollectionsAsync(System.Collections.IList owners, TableMapping ownerMap, CancellationToken ct, DateTime? asOf = null)
        {
            if (ownerMap.KeyColumns.Length == 0 || owners.Count == 0) return;

            foreach (var ownedMap in ownerMap.OwnedCollections)
            {
                var prep = PrepareOwnedCollectionLoad(ownedMap, owners, ownerMap, asOf);
                if (prep == null) continue;
                var (cmd, ownerByPk, pkCol, fkOrdinal) = prep.Value;
                try
                {
                    await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                    while (await reader.ReadAsync(ct).ConfigureAwait(false))
                        MaterializeAndAssignOwnedRow(reader, ownedMap, pkCol, ownerByPk, fkOrdinal);
                }
                finally
                {
                    await cmd.DisposeAsync().ConfigureAwait(false);
                }
            }
            CaptureOwnedSnapshotsAfterLoad(owners);
        }

        /// <summary>
        /// After owned collections are loaded, capture each tracked owner's owned-collection
        /// content snapshot so a later collection edit (add / remove / child-scalar change)
        /// is detected as a change. Owners are tracked BEFORE this load populated the
        /// collections, so their snapshot would otherwise reflect the empty pre-load state
        /// and edits would silently drop.
        /// </summary>
        private void CaptureOwnedSnapshotsAfterLoad(System.Collections.IList owners)
        {
            foreach (var owner in owners)
            {
                if (owner == null) continue;
                ChangeTracker.GetEntryOrDefault(owner)?.CaptureOwnedCollectionSnapshots();
            }
        }

        /// <summary>
        /// Synchronous twin of <see cref="LoadOwnedCollectionsAsync"/>. The sync query materialize path is
        /// deliberately free of <c>GetAwaiter().GetResult()</c>, so owned-collection loading needs its own
        /// truly synchronous path. Without loading owned collections on the sync path, a sync-loaded owner
        /// has an empty owned navigation and a later scalar edit + SaveChanges deletes all its owned
        /// children (SaveOwnedCollectionsAsync delete-then-reinserts from that empty nav).
        /// </summary>
        internal void LoadOwnedCollections(System.Collections.IList owners, TableMapping ownerMap, DateTime? asOf = null)
        {
            if (ownerMap.KeyColumns.Length == 0 || owners.Count == 0) return;

            foreach (var ownedMap in ownerMap.OwnedCollections)
            {
                var prep = PrepareOwnedCollectionLoad(ownedMap, owners, ownerMap, asOf);
                if (prep == null) continue;
                var (cmd, ownerByPk, pkCol, fkOrdinal) = prep.Value;
                using (cmd)
                {
                    using var reader = cmd.ExecuteReader();
                    while (reader.Read())
                        MaterializeAndAssignOwnedRow(reader, ownedMap, pkCol, ownerByPk, fkOrdinal);
                }
            }
            CaptureOwnedSnapshotsAfterLoad(owners);
        }

        /// <summary>
        /// Builds the IN-query command that loads one owned collection for the given owners (tenant-scoped
        /// when multi-tenancy is configured), seeds each owner's collection to an empty list, and returns
        /// the command plus the lookup needed to assign rows. Returns <c>null</c> when no owner has a
        /// resolvable key. Shared by the sync and async loaders.
        /// </summary>
        private (DbCommand Cmd, Dictionary<object, object> OwnerByPk, Column PkCol, int FkOrdinal)? PrepareOwnedCollectionLoad(
            OwnedCollectionMapping ownedMap, System.Collections.IList owners, TableMapping ownerMap, DateTime? asOf = null)
        {
            // Resolve which owner key column this FK references (composite-key aware).
            var pkCol = ResolveOwnerKeyColumnForOwnedFk(ownerMap.KeyColumns, ownedMap.ForeignKeyColumn, ownerMap.Type.Name);

            // Build PK -> owner lookup keyed by the FK-referenced key column value.
            var ownerByPk = new Dictionary<object, object>(owners.Count);
            foreach (var owner in owners)
            {
                if (owner == null) continue;
                var pk = pkCol.Getter(owner);
                if (pk != null && !ownerByPk.ContainsKey(pk))
                    ownerByPk[pk] = owner;
            }
            if (ownerByPk.Count == 0) return null;

            // SELECT owned cols + fk_col FROM child_table WHERE fk_col IN (@p0, @p1, ...)
            // Under AsOf the owned rows are part of the reconstructed entity, so they read
            // through the SAME history window the root query used — the live table would
            // silently mix eras (or, for rows deleted since, drop history that exists).
            var fromSource = ownedMap.EscTable;
            string? asOfParamName = null;
            if (asOf != null)
            {
                asOfParamName = _p.ParamPrefix + "asof";
                var ownedTypeMap = GetMapping(ownedMap.OwnedType);
                if (Options.TemporalStorageMode == nORM.Configuration.TemporalStorageMode.ProviderNative)
                {
                    fromSource = _p.GetProviderNativeTemporalAsOfFromClause(ownedTypeMap, asOfParamName);
                }
                else
                {
                    var history = _p.Escape(ownedTypeMap.TableName + "_History");
                    var w = _p.Escape("__ownw");
                    var windowCols = new StringBuilder();
                    for (int ci = 0; ci < ownedMap.Columns.Length; ci++)
                    {
                        if (ci > 0) windowCols.Append(", ");
                        windowCols.Append(ownedMap.Columns[ci].EscCol);
                    }
                    if (ownedMap.Columns.Length > 0) windowCols.Append(", ");
                    windowCols.Append(ownedMap.EscForeignKeyColumn);
                    fromSource = $"(SELECT {windowCols} FROM {history} {w} " +
                        $"WHERE {asOfParamName} >= {w}.{_p.Escape("__ValidFrom")} AND {asOfParamName} < {w}.{_p.Escape("__ValidTo")})";
                }
            }

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
            sqlBuilder.Append(" FROM ").Append(fromSource).Append(' ').Append(_p.Escape("__ownt"))
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

            var cmd = CreateCommand();
            cmd.CommandText = sqlBuilder.ToString();
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
            if (asOfParamName != null)
            {
                var ap = cmd.CreateParameter();
                ap.ParameterName = asOfParamName;
                ap.Value = _p.FormatTemporalAsOfParameterValue(asOf!.Value);
                cmd.Parameters.Add(ap);
            }

            // Reset each owner's collection to a fresh empty list before loading so the load is
            // idempotent — calling it again (e.g. the query pipeline auto-loads and the caller then
            // explicitly reloads) REPLACES the children instead of appending duplicates.
            foreach (var owner in owners)
            {
                if (owner == null) continue;
                ownedMap.CollectionSetter(owner, Activator.CreateInstance(typeof(List<>).MakeGenericType(ownedMap.OwnedType)));
            }

            int fkOrdinal = ownedMap.Columns.Length; // FK is the last column in our SELECT
            return (cmd, ownerByPk, pkCol, fkOrdinal);
        }

        /// <summary>
        /// Materializes one owned-collection row from the reader and appends it to its owner's collection.
        /// Shared by the sync and async loaders; skips a row whose FK does not resolve to a loaded owner.
        /// </summary>
        private void MaterializeAndAssignOwnedRow(
            DbDataReader reader, OwnedCollectionMapping ownedMap, Column pkCol, Dictionary<object, object> ownerByPk, int fkOrdinal)
        {
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

            // Read FK and assign to owner. Type coercion fallback: ADO.NET providers may return the FK
            // value in a different numeric type than the CLR PK property (e.g. SQLite returns Int64 for
            // all integers while the PK property may be Int32); coerce on the dictionary-lookup miss.
            if (reader.IsDBNull(fkOrdinal)) return;
            var fkVal = reader.GetValue(fkOrdinal);
            if (!ownerByPk.TryGetValue(fkVal, out var ownerEntity))
            {
                fkVal = ConvertSimple(fkVal, pkCol.Prop.PropertyType)!;
                if (fkVal == null || !ownerByPk.TryGetValue(fkVal, out ownerEntity)) return;
            }

            var col2 = ownedMap.CollectionGetter(ownerEntity);
            if (col2 is System.Collections.IList list)
                list.Add(item);
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
