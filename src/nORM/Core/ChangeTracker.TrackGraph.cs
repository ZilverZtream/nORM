using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public sealed partial class ChangeTracker
    {
        /// <summary>
        /// Begins tracking <paramref name="rootEntity"/> and every entity reachable from it through
        /// navigation properties, invoking <paramref name="callback"/> once for each entity discovered that
        /// is not already tracked (Entity Framework Core parity). The callback assigns
        /// <see cref="EntityEntry.State"/> on the node's <see cref="EntityEntryGraphNode.Entry"/> to choose
        /// how that entity is tracked — a common pattern is
        /// <c>node =&gt; node.Entry.State = node.Entry.IsKeySet ? EntityState.Modified : EntityState.Added</c>.
        /// An entity the callback leaves <see cref="EntityState.Detached"/> is not tracked and the traversal
        /// does not descend into its navigations; an entity already tracked by the context is skipped and not
        /// traversed through, so a cyclic graph always terminates. No lazy loading is enabled during the walk,
        /// so traversing the graph never triggers a database query — only the navigations the caller populated
        /// on the (typically disconnected) graph are followed.
        /// </summary>
        /// <param name="rootEntity">The root of the entity graph to track.</param>
        /// <param name="callback">Invoked for each untracked entity to assign its <see cref="EntityEntry.State"/>.</param>
        /// <exception cref="ArgumentNullException"><paramref name="rootEntity"/> or <paramref name="callback"/> is null.</exception>
        /// <exception cref="InvalidOperationException">The change tracker is not bound to a <see cref="DbContext"/>.</exception>
        [RequiresDynamicCode("TrackGraph reads navigation properties and resolves mappings via reflection; not NativeAOT-compatible.")]
        [RequiresUnreferencedCode("TrackGraph reflects over navigation properties; trimming may remove the required members.")]
        public void TrackGraph(object rootEntity, Action<EntityEntryGraphNode> callback)
        {
            ArgumentNullException.ThrowIfNull(rootEntity);
            ArgumentNullException.ThrowIfNull(callback);
            var ctx = Context ?? throw new InvalidOperationException(
                "TrackGraph requires the change tracker to be bound to a DbContext.");

            var visited = new HashSet<object>(ReferenceEqualityComparer.Instance);
            var stack = new Stack<(object Entity, EntityEntry? Source, string? Nav)>();
            stack.Push((rootEntity, null, null));

            while (stack.Count > 0)
            {
                var (entity, source, nav) = stack.Pop();
                if (entity is null || !visited.Add(entity))
                    continue;

                // An entity already tracked — before this call, or shared with an earlier node this
                // traversal — is not re-processed and its navigations are not followed. This matches EF and
                // (together with the visited set) guarantees a cyclic graph terminates.
                var existing = GetEntryOrDefault(entity);
                if (existing != null && existing.State != EntityState.Detached)
                    continue;

                var mapping = ctx.GetMapping(entity.GetType());

                // Present the callback a Detached, tracker-less PROBE entry: assigning its State records the
                // caller's intent with no change-tracker side effect (MarkDirty/Remove are all null-guarded on
                // the tracker). If the caller leaves it Detached we neither track it nor descend; otherwise we
                // track the real entity in the chosen state via the normal Track path. IsKeySet/CurrentValues
                // read straight off the entity, so the callback sees accurate values on the probe.
                var probe = new EntityEntry(entity, EntityState.Detached, mapping, _options, tracker: null);
                callback(new EntityEntryGraphNode(probe, source, nav));

                var chosen = probe.State;
                if (chosen == EntityState.Detached)
                    continue;

                var tracked = Track(entity, chosen, mapping);

                foreach (var (childNav, child) in EnumerateNavigationChildren(entity, mapping))
                    stack.Push((child, tracked, childNav));
            }
        }

        /// <summary>
        /// Yields each entity referenced by <paramref name="entity"/>'s navigation properties — reference
        /// navigations, relationship collections (one-to-many, or a one-to-one reference), and many-to-many
        /// collections — paired with the navigation name. Owned collections are excluded: owned rows are part
        /// of their owner's aggregate, not independently tracked entities whose state the caller would set.
        /// </summary>
        [RequiresDynamicCode("Reads navigation properties via reflection.")]
        [RequiresUnreferencedCode("Reflects over navigation properties.")]
        private static IEnumerable<(string Nav, object Child)> EnumerateNavigationChildren(object entity, TableMapping mapping)
        {
            foreach (var navProp in mapping.ReferenceNavigations)
            {
                var value = navProp.GetValue(entity);
                if (value != null)
                    yield return (navProp.Name, value);
            }

            foreach (var relation in mapping.Relations.Values)
            {
                var value = relation.NavProp.GetValue(entity);
                if (value is IEnumerable collection and not string)
                {
                    foreach (var child in collection)
                        if (child != null)
                            yield return (relation.NavProp.Name, child);
                }
                else if (value != null)
                {
                    yield return (relation.NavProp.Name, value);
                }
            }

            foreach (var join in mapping.ManyToManyJoins)
            {
                var navProp = mapping.Type.GetProperty(join.LeftNavPropertyName);
                if (navProp?.GetValue(entity) is IEnumerable collection and not string)
                {
                    foreach (var child in collection)
                        if (child != null)
                            yield return (join.LeftNavPropertyName, child);
                }
            }
        }
    }
}
