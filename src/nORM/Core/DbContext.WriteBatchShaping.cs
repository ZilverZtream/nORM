using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        /// <summary>
        /// Orders the rows of a single self-referential mapping group so a parent row is written
        /// before the child row that references it (<paramref name="childrenFirst"/> = false, for
        /// inserts) or after it (<paramref name="childrenFirst"/> = true, for deletes). A
        /// self-referential table is one mapping, so <see cref="TopologicalSortMappings"/> cannot
        /// separate parents from children — the dependency lives between rows, not types, and an
        /// in-batch child insert that precedes its parent would violate the self-foreign-key.
        /// Returns the input list unchanged when the mapping has no self-reference or has fewer
        /// than two rows, so the common non-self-referential path pays no reordering cost.
        /// </summary>
        private static List<EntityEntry> OrderSelfReferentialRows(List<EntityEntry> entries, TableMapping map, bool childrenFirst)
        {
            if (entries.Count < 2)
                return entries;

            var selfRelation = map.Relations.Values.FirstOrDefault(r => r.DependentType == map.Type);
            if (selfRelation == null)
                return entries;

            var principalKeys = selfRelation.PrincipalKeys;
            var foreignKeys = selfRelation.ForeignKeys;

            // Index rows by their principal-key value so a child row can locate its parent row
            // within the same batch. Keys are formatted invariantly and length-delimited so that
            // single- and multi-column (composite) self-references compare consistently.
            static string FormatKey(IReadOnlyList<Column> cols, object entity)
            {
                // Length-prefixed, fully-printable segments (mirrors AppendSegment): a value can
                // never forge a segment boundary, and the -1 length is an unambiguous null marker
                // that no real value can produce.
                var sb = new StringBuilder();
                foreach (var c in cols)
                {
                    var v = c.Getter(entity);
                    if (v == null)
                    {
                        sb.Append("-1:|");
                    }
                    else
                    {
                        var s = Convert.ToString(v, CultureInfo.InvariantCulture) ?? string.Empty;
                        sb.Append(s.Length).Append(':').Append(s).Append('|');
                    }
                }
                return sb.ToString();
            }

            static bool AllKeyPartsNull(IReadOnlyList<Column> cols, object entity)
            {
                foreach (var c in cols)
                    if (c.Getter(entity) != null)
                        return false;
                return true;
            }

            var byPrincipal = new Dictionary<string, EntityEntry>(StringComparer.Ordinal);
            foreach (var e in entries)
            {
                if (e.Entity != null)
                    byPrincipal[FormatKey(principalKeys, e.Entity)] = e;
            }

            // Key-value indexing (above) cannot order a generated-key self-reference: every row's key is
            // still its type default at insert time, so parent and child collide on the same default key.
            // Fall back to object-graph edges (identity, not value): a parent's collection navigation lists
            // its children, and a child's reference navigation points at its parent. Either populated nav
            // gives the dependency, independent of the not-yet-generated key values.
            var parentByChildRef = new Dictionary<object, EntityEntry>(ReferenceEqualityComparer.Instance);
            var entryByEntity = new Dictionary<object, EntityEntry>(ReferenceEqualityComparer.Instance);
            foreach (var e in entries)
                if (e.Entity != null)
                    entryByEntity[e.Entity] = e;

            // (a) principal collection navigation: parent.NavProp enumerates its children.
            foreach (var e in entries)
            {
                if (e.Entity != null
                    && selfRelation.NavProp.GetValue(e.Entity) is System.Collections.IEnumerable children
                    && children is not string)
                {
                    foreach (var child in children)
                        if (child != null && !parentByChildRef.ContainsKey(child))
                            parentByChildRef[child] = e;
                }
            }

            // (b) dependent reference navigation: child.<refNav> points at the parent. Covers the graph
            // where only the reference side was set (the inverse collection may be empty).
            var referenceNavToParent = map.ReferenceNavigations.FirstOrDefault(nav =>
                ReferenceEquals(
                    global::nORM.Query.ExpressionToSqlVisitor.FindReferenceNavForeignKey(map, nav.Name, nav.PropertyType, map),
                    foreignKeys[0]));
            if (referenceNavToParent != null)
            {
                foreach (var e in entries)
                {
                    if (e.Entity == null || parentByChildRef.ContainsKey(e.Entity))
                        continue;
                    if (referenceNavToParent.GetValue(e.Entity) is { } parentObj
                        && entryByEntity.TryGetValue(parentObj, out var parentEntry)
                        && !ReferenceEquals(parentEntry, e))
                        parentByChildRef[e.Entity] = parentEntry;
                }
            }

            var ordered = new List<EntityEntry>(entries.Count);
            // 1 = on the current DFS path (cycle guard), 2 = emitted.
            var state = new Dictionary<EntityEntry, int>();

            void Visit(EntityEntry node)
            {
                if (state.TryGetValue(node, out _))
                    return; // already emitted, or on the current path (row-level cycle) — leave order to the DB.

                state[node] = 1;
                EntityEntry? parent = null;
                // Object-graph edges (identity) are authoritative and are consulted FIRST: with generated
                // keys every row's key is the same type default, so the key-value index below collides and
                // would resolve a child to an arbitrary same-default row (not its real parent). Only when no
                // navigation edge exists (the explicit-key case, where navs are typically unset) do we fall
                // back to matching the FK value against principal keys.
                if (node.Entity != null
                    && parentByChildRef.TryGetValue(node.Entity, out var graphParent)
                    && !ReferenceEquals(graphParent, node))
                {
                    parent = graphParent;
                }
                else if (node.Entity != null && !AllKeyPartsNull(foreignKeys, node.Entity))
                {
                    var parentKey = FormatKey(foreignKeys, node.Entity);
                    if (byPrincipal.TryGetValue(parentKey, out var keyParent) && !ReferenceEquals(keyParent, node))
                        parent = keyParent;
                }

                if (parent != null)
                    Visit(parent); // emit the parent first (post-order places it ahead of this node)

                state[node] = 2;
                ordered.Add(node);
            }

            foreach (var e in entries)
                Visit(e);

            if (childrenFirst)
                ordered.Reverse();
            return ordered;
        }

        /// <summary>
        /// True when a self-referential Added group must insert ONE row at a time. With a generated key
        /// (DB-generated or convention store-generated), a child row's FK parameter is bound before the
        /// multi-row INSERT command executes, so packing a parent and its self-referencing child into one
        /// command binds the child's FK to the parent's still-default key (0/NULL) — a silently wrong
        /// persisted FK, or an outright FK violation under enforcement. Per-row inserts let each parent's
        /// INSERT hydrate its key and <c>PropagateGeneratedKeyToChildren</c> update its in-memory children
        /// before the child row binds its parameters. <see cref="OrderSelfReferentialRows"/> places the
        /// parent first. Explicit-key self-references need no per-row split (the FK is known up front).
        /// </summary>
        private static bool ShouldInsertSelfReferentialRowsIndividually(TableMapping map, EntityState state)
            => state == EntityState.Added
               && (map.ConventionGeneratedKeyColumn != null || map.KeyColumns.Any(k => k.IsDbGenerated))
               && map.Relations.Values.Any(r => r.DependentType == map.Type);

        private int CalculateBatchSize(int totalEntries, int paramsPerEntity)
        {
            var batchSize = totalEntries;
            if (_p.MaxParameters != int.MaxValue)
            {
                var maxParams = Math.Max(1, _p.MaxParameters - ParameterBudgetReserve);
                var capacity = Math.Max(1, maxParams / Math.Max(1, paramsPerEntity));
                // Cap at the actual entity count — never batch (or size the SQL StringBuilder for) more rows
                // than exist. Without the Min, a single-row save sized its StringBuilder for the provider's
                // full parameter capacity (~83 rows on SQLite), allocating tens of KB per write for nothing.
                batchSize = Math.Min(totalEntries, capacity);
            }
            return batchSize;
        }

        private int EstimateTemplateLength(EntityState state, TableMapping map)
            => state switch
            {
                EntityState.Added => BuildInsertBatch(map, 0).Length + 1,
                EntityState.Modified => BuildUpdateBatch(map, 0).Length + 1,
                EntityState.Deleted => BuildDeleteBatch(map, 0).Length + 1,
                _ => 0
            };
    }
}
