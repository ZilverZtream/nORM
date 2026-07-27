using System;
using System.Collections.Generic;
using System.Linq;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        /// <summary>
        /// Discovers dependents reachable through the collection navigations of tracked, non-deleted
        /// entities that are not yet tracked themselves, tracks each as <see cref="EntityState.Added"/>,
        /// and populates its foreign key from the principal's primary key — the documented
        /// <c>principal.Children.Add(child)</c> relationship-fixup contract. A work queue visits a newly
        /// added child's own collections too, so a deep object graph is added in one SaveChanges. When
        /// the principal's key is DB-generated (still default here), the foreign key is re-propagated
        /// after the principal is inserted (see <see cref="ChangeTracker.PropagateGeneratedKeyToChildren"/>).
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Relationship fixup reads navigation properties and resolves dependent mappings via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Relationship fixup reflects over navigation properties; trimming may remove the required members.")]
        private void FixupNavigationChildren()
        {
            ChangeTracker.ClearPendingReferenceKeyFixups();
            var queue = new Queue<EntityEntry>();
            foreach (var e in ChangeTracker.Entries)
                if (e.Entity != null && e.State != EntityState.Deleted
                    && (e.Mapping.Relations.Count > 0 || e.Mapping.ReferenceNavigations.Length > 0
                        || e.Mapping.ManyToManyJoins.Count > 0))
                    queue.Enqueue(e);
            if (queue.Count == 0)
                return;

            // PASS 1 — associate: track any untracked child found in a collection as Added
            // and set its FK from the principal (the documented principal.Children.Add(child)
            // contract). Deep graphs are walked via the queue. Reconciliation of already-
            // tracked children (reparent / severance) is handled in pass 2, gated on a
            // load-time snapshot so graph-only writes are untouched.
            while (queue.Count > 0)
            {
                var entry = queue.Dequeue();
                var principal = entry.Entity;
                if (principal == null)
                    continue;

                foreach (var relation in entry.Mapping.Relations.Values)
                {
                    if (relation.NavProp.GetValue(principal) is not System.Collections.IEnumerable collection || collection is string)
                        continue;

                    TableMapping? childMapping = null;
                    foreach (var child in collection)
                    {
                        if (child == null)
                            continue;
                        if (ChangeTracker.GetEntryOrDefault(child) != null)
                        {
                            // Already tracked — e.g. ctx.Add(child) then principal.Children.Add(child) without
                            // setting the child's FK/reference nav. Fixup still owes it the principal's key:
                            // fill an UNSET FK from the principal's already-assigned key (a deliberately-set
                            // FK is left alone — FK edit outranks stale nav). A default/DB-generated principal
                            // key is skipped here and propagated post-insert by PropagateGeneratedKeyToChildren.
                            PropagateAssignedPrincipalKeyToUnsetFk(relation, principal, child);
                            continue;
                        }

                        childMapping ??= GetMapping(relation.DependentType);
                        var childEntry = ChangeTracker.Track(child, EntityState.Added, childMapping);

                        // Set the FK from the principal's PK. Correct immediately when the principal's key
                        // is already assigned; re-propagated after insert for DB-generated principal keys.
                        for (int i = 0; i < relation.ForeignKeys.Count && i < relation.PrincipalKeys.Count; i++)
                            relation.ForeignKeys[i].Setter(child, relation.PrincipalKeys[i].Getter(principal));

                        if (childEntry.Mapping.Relations.Count > 0 || childEntry.Mapping.ReferenceNavigations.Length > 0)
                            queue.Enqueue(childEntry);
                    }
                }

                // Many-to-many (skip) navigations: a NEW related entity added to the collection WITHOUT
                // ctx.Add — post.Tags.Add(new Tag{...}) — must be tracked (and thus inserted) so the deferred
                // join-table sync links it by its hydrated key instead of a default 0 (a lost entity + broken
                // join row). Only untracked entities are tracked here; already-tracked ones flow through the
                // association sync. Walk whichever side this entry owns the collection nav for.
                foreach (var join in entry.Mapping.ManyToManyJoins)
                {
                    if (join.LeftType == entry.Mapping.Type)
                        TrackUntrackedManyToManyRelated(join.LeftCollectionGetter(principal), join.RightType, queue);
                    if (join.RightCollectionGetter != null && join.RightType == entry.Mapping.Type)
                        TrackUntrackedManyToManyRelated(join.RightCollectionGetter(principal), join.LeftType, queue);
                }

                FixupReferenceNavigations(entry, queue);
            }

            // PASS 2 — reconcile loaded collections: a child that was present when a
            // collection was loaded but is now absent is either reparented (moved into
            // another loaded collection) or disassociated. Gated on a load-time snapshot.
            ReconcileLoadedCollections();
        }

        /// <summary>
        /// Tracks (as Added) each untracked entity in a many-to-many collection navigation so it is inserted
        /// and its key hydrated before the deferred join-table sync flushes — otherwise a new related entity
        /// added straight to the skip navigation is silently lost and its join row written with a default key.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Relationship fixup resolves dependent mappings via reflection; trimming may remove the required members.")]
        private void TrackUntrackedManyToManyRelated(System.Collections.IList? related, Type relatedType, Queue<EntityEntry> queue)
        {
            if (related == null)
                return;
            TableMapping? relatedMapping = null;
            foreach (var item in related)
            {
                if (item == null || ChangeTracker.GetEntryOrDefault(item) != null)
                    continue;
                relatedMapping ??= GetMapping(relatedType);
                // Only a NEW entity (all key components default/unset) is inserted. An untracked entity with a
                // SET key is an EXISTING row that the join sync links by key — inserting it would duplicate
                // (and a client-keyed new entity must be ctx.Add'd explicitly, since a set key is ambiguous).
                if (!HasAllDefaultKeyValues(item, relatedMapping))
                    continue;
                var itemEntry = ChangeTracker.Track(item, EntityState.Added, relatedMapping);
                if (itemEntry.Mapping.Relations.Count > 0
                    || itemEntry.Mapping.ReferenceNavigations.Length > 0
                    || itemEntry.Mapping.ManyToManyJoins.Count > 0)
                    queue.Enqueue(itemEntry);
            }
        }

        /// <summary>True when every key column of <paramref name="entity"/> holds its default/unset value —
        /// i.e. the entity is new (never persisted) rather than an existing row referenced by key.</summary>
        private static bool HasAllDefaultKeyValues(object entity, TableMapping mapping)
        {
            if (mapping.KeyColumns.Length == 0)
                return false;
            foreach (var key in mapping.KeyColumns)
                if (!ChangeTracker.IsDefaultKeyValue(key.Getter(entity), key.Prop.PropertyType))
                    return false;
            return true;
        }

        /// <summary>
        /// Reference-navigation fixup for a COMPOSITE-key principal: when a dependent's reference nav points
        /// at a composite-key principal, copy every principal key component into the matching FK columns —
        /// the collection direction already does this, so the two directions agree. Only acts on an
        /// unambiguous composite relation from the principal to this dependent type (a dual composite FK is
        /// skipped to avoid guessing), a fully-assigned principal key (a still-default DB-generated component
        /// defers), and never rewrites a key or an already-correct FK. Non-null principals only; composite
        /// disassociation keeps the prior (conservative) no-op.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Relationship fixup reflects over navigation properties; trimming may remove the required members.")]
        private void TryFixupCompositeReferenceNav(EntityEntry entry, object? principal, TableMapping principalMap, Queue<EntityEntry> queue)
        {
            if (principal == null)
                return;

            TableMapping.Relation? match = null;
            foreach (var rel in principalMap.Relations.Values)
            {
                if (rel.DependentType == entry.Mapping.Type && rel.IsComposite)
                {
                    if (match != null)
                        return; // ambiguous (dual composite FK to the same dependent) — stay safe
                    match = rel;
                }
            }
            if (match == null)
                return;

            var principalEntry = ChangeTracker.GetEntryOrDefault(principal);
            if (principalEntry == null)
            {
                principalEntry = ChangeTracker.Track(principal, EntityState.Added, principalMap);
                if (principalEntry.Mapping.Relations.Count > 0 || principalEntry.Mapping.ReferenceNavigations.Length > 0)
                    queue.Enqueue(principalEntry);
            }
            else if (principalEntry.State == EntityState.Deleted)
            {
                return;
            }

            var count = Math.Min(match.ForeignKeys.Count, match.PrincipalKeys.Count);
            // Defer until every principal key component is assigned (a still-default DB-generated component
            // is propagated post-insert by PropagateGeneratedKeyToChildren).
            for (int i = 0; i < count; i++)
            {
                var pk = match.PrincipalKeys[i];
                if (ChangeTracker.IsDefaultKeyValue(pk.Getter(principal), pk.Prop.PropertyType))
                    return;
            }

            var dependent = entry.Entity!;
            var changed = false;
            for (int i = 0; i < count; i++)
            {
                var fkCol = match.ForeignKeys[i];
                if (fkCol.IsKey)
                    continue; // never rewrite a key column
                var pkValue = match.PrincipalKeys[i].Getter(principal);
                if (!Equals(fkCol.Getter(dependent), pkValue))
                {
                    fkCol.Setter(dependent, pkValue);
                    changed = true;
                }
            }
            if (changed && entry.State is EntityState.Unchanged or EntityState.Modified)
            {
                entry.SetStateInternal(EntityState.Modified);
                entry.MarkExplicitlyModified();
            }
        }

        /// <summary>
        /// Fills an already-tracked child's UNSET foreign key from the principal's ASSIGNED key when the
        /// child sits in the principal's collection navigation. Skips a principal whose key is still default
        /// (DB-generated, propagated post-insert instead) and a child whose FK is already non-default (set
        /// deliberately — FK edit outranks stale nav). Composite keys are handled per component.
        /// </summary>
        private static void PropagateAssignedPrincipalKeyToUnsetFk(TableMapping.Relation relation, object principal, object child)
        {
            for (int i = 0; i < relation.ForeignKeys.Count && i < relation.PrincipalKeys.Count; i++)
            {
                var principalKey = relation.PrincipalKeys[i].Getter(principal);
                if (ChangeTracker.IsDefaultKeyValue(principalKey, relation.PrincipalKeys[i].Prop.PropertyType))
                    continue;
                var fkCol = relation.ForeignKeys[i];
                if (!ChangeTracker.IsDefaultKeyValue(fkCol.Getter(child), fkCol.Prop.PropertyType))
                    continue;
                fkCol.Setter(child, principalKey);
            }
        }

        /// <summary>
        /// Reconciles every collection that carries a load-time snapshot against its current
        /// contents. A child removed from a loaded collection is reparented when it now sits
        /// in another tracked principal's collection, or otherwise disassociated (optional
        /// relationship → FK nulled; required → orphan deleted). Runs only when at least one
        /// collection was Include-loaded, so graph-only writes pay no cost and see no change.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Relationship fixup reads navigation properties and resolves dependent mappings via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Relationship fixup reflects over navigation properties; trimming may remove the required members.")]
        private void ReconcileLoadedCollections()
        {
            List<EntityEntry>? snapshotOwners = null;
            foreach (var e in ChangeTracker.Entries)
                if (e.Entity != null && e.CollectionNavSnapshots is { Count: > 0 })
                    (snapshotOwners ??= new List<EntityEntry>()).Add(e);
            if (snapshotOwners == null)
                return;

            // Reverse index: which tracked principal currently holds each child, per
            // relation. Lets a reparent (child moved into another loaded collection) be told
            // apart from a true removal. Keyed by the shared Relation instance, then by child
            // reference. Read-only over the graph — never mutates.
            var membership = new Dictionary<TableMapping.Relation, Dictionary<object, object>>();
            foreach (var e in ChangeTracker.Entries)
            {
                var owner = e.Entity;
                if (owner == null || e.State == EntityState.Deleted)
                    continue;
                foreach (var relation in e.Mapping.Relations.Values)
                {
                    if (relation.NavProp.GetValue(owner) is not System.Collections.IEnumerable coll || coll is string)
                        continue;
                    Dictionary<object, object>? map = null;
                    foreach (var child in coll)
                    {
                        if (child == null)
                            continue;
                        map ??= membership.TryGetValue(relation, out var m)
                            ? m
                            : membership[relation] = new Dictionary<object, object>(ReferenceEqualityComparer.Instance);
                        map[child] = owner; // last write wins for an (invalid) aliased child
                    }
                }
            }

            foreach (var entry in snapshotOwners)
                foreach (var relation in entry.Mapping.Relations.Values)
                    if (entry.CollectionNavSnapshots!.ContainsKey(relation.NavProp.Name))
                        ReconcileRemovedChildren(entry, relation, membership);
        }

        /// <summary>
        /// For one loaded collection, disassociates or reparents each child that was in the
        /// load-time snapshot but is no longer in the collection. A deliberately edited FK
        /// outranks the stale membership (neither action). Severed/reparented children are
        /// dropped from the snapshot so a later save does not repeat the action — the
        /// principal owning the snapshot is often itself Unchanged, so its AcceptChanges
        /// (which would refresh the baseline) never runs.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Relationship fixup reads navigation properties and resolves dependent mappings via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Relationship fixup reflects over navigation properties; trimming may remove the required members.")]
        private void ReconcileRemovedChildren(EntityEntry entry, TableMapping.Relation relation,
            Dictionary<TableMapping.Relation, Dictionary<object, object>> membership)
        {
            if (entry.CollectionNavSnapshots == null
                || !entry.CollectionNavSnapshots.TryGetValue(relation.NavProp.Name, out var loaded)
                || loaded.Count == 0)
                return;
            var owner = entry.Entity;
            if (owner == null)
                return;

            var current = new HashSet<object>(ReferenceEqualityComparer.Instance);
            if (relation.NavProp.GetValue(owner) is System.Collections.IEnumerable coll && coll is not string)
                foreach (var item in coll)
                    if (item != null) current.Add(item);

            var isOptional = true;
            for (int i = 0; i < relation.ForeignKeys.Count; i++)
                if (!relation.ForeignKeys[i].IsNullable) { isOptional = false; break; }

            membership.TryGetValue(relation, out var holders);

            List<object>? done = null;
            foreach (var removed in loaded)
            {
                if (current.Contains(removed))
                    continue;
                var removedEntry = ChangeTracker.GetEntryOrDefault(removed);
                if (removedEntry == null || removedEntry.State is not (EntityState.Unchanged or EntityState.Modified))
                    continue;

                // A deliberately edited FK outranks the (now stale) collection membership —
                // honor the edit and do nothing else.
                var fkEdited = false;
                for (int i = 0; i < relation.ForeignKeys.Count; i++)
                    if (removedEntry.HasColumnValueChanged(relation.ForeignKeys[i])) { fkEdited = true; break; }
                if (fkEdited)
                {
                    (done ??= new List<object>()).Add(removed);
                    continue;
                }

                // Reparented into another tracked principal's collection?
                object? newPrincipal = null;
                if (holders != null && holders.TryGetValue(removed, out var h) && !ReferenceEquals(h, owner))
                    newPrincipal = h;

                if (newPrincipal != null)
                {
                    for (int i = 0; i < relation.ForeignKeys.Count && i < relation.PrincipalKeys.Count; i++)
                        relation.ForeignKeys[i].Setter(removed, relation.PrincipalKeys[i].Getter(newPrincipal));
                    removedEntry.SetStateInternal(EntityState.Modified);
                    removedEntry.MarkExplicitlyModified();
                }
                else if (isOptional)
                {
                    for (int i = 0; i < relation.ForeignKeys.Count; i++)
                        relation.ForeignKeys[i].Setter(removed, null);
                    removedEntry.SetStateInternal(EntityState.Modified);
                    removedEntry.MarkExplicitlyModified();
                }
                else
                {
                    removedEntry.SetStateInternal(EntityState.Deleted);
                }
                (done ??= new List<object>()).Add(removed);
            }

            if (done != null)
                foreach (var d in done)
                    loaded.Remove(d);
        }

        /// <summary>
        /// The reference direction of relationship fixup: <c>dependent.Principal = entity</c>.
        /// An untracked principal is discovered and tracked as Added, and the dependent's FK
        /// scalar is aligned with the principal's primary key so the assignment persists. A
        /// null navigation is a no-op — a plain query leaves navigations null while the FK
        /// holds a value, so null cannot mean "sever" without a navigation snapshot; clearing
        /// the FK scalar is the severing gesture. A deliberately edited FK scalar outranks a
        /// stale navigation reference (the navigation may still point at the previously
        /// included principal).
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Relationship fixup reads navigation properties and resolves principal mappings via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Relationship fixup reflects over navigation properties; trimming may remove the required members.")]
        private void FixupReferenceNavigations(EntityEntry entry, Queue<EntityEntry> queue)
        {
            var navProps = entry.Mapping.ReferenceNavigations;
            if (navProps.Length == 0)
                return;
            var dependent = entry.Entity!;

            foreach (var navProp in navProps)
            {
                object? principal;
                try
                {
                    principal = navProp.GetValue(dependent);
                }
                catch
                {
                    continue;
                }

                TableMapping principalMap;
                try
                {
                    principalMap = GetMapping(navProp.PropertyType);
                }
                catch
                {
                    continue;
                }
                if (principalMap.KeyColumns.Length != 1)
                {
                    // Composite-key principal: the single-column fixup below can't express it. Propagate the
                    // full composite FK from an assigned composite key, mirroring the collection direction.
                    TryFixupCompositeReferenceNav(entry, principal, principalMap, queue);
                    continue;
                }
                var fk = global::nORM.Query.ExpressionToSqlVisitor.FindReferenceNavForeignKey(
                    entry.Mapping, navProp.Name, navProp.PropertyType, principalMap);
                if (fk == null)
                    continue;

                if (principal == null)
                {
                    // The user cleared a reference nav that was loaded non-null →
                    // disassociate by nulling the FK. A never-loaded null nav is not
                    // recorded, so its still-valid FK is left intact. Never touch a
                    // key, an already-null FK, or a REQUIRED (non-nullable) FK — a
                    // required relationship cannot be severed by nulling its FK (that
                    // would unbox null into a value-type column and throw), so its FK
                    // is left intact for the same reason a never-loaded nav's is.
                    if (entry.LoadedReferenceNavs?.Contains(navProp.Name) == true
                        && !fk.IsKey
                        && fk.IsNullable
                        && fk.Getter(dependent) != null
                        && entry.State != EntityState.Added)
                    {
                        fk.Setter(dependent, null);
                        if (entry.State is EntityState.Unchanged or EntityState.Modified)
                        {
                            entry.SetStateInternal(EntityState.Modified);
                            entry.MarkExplicitlyModified();
                        }
                    }
                    continue;
                }

                var principalEntry = ChangeTracker.GetEntryOrDefault(principal);
                if (principalEntry == null)
                {
                    principalEntry = ChangeTracker.Track(principal, EntityState.Added, principalMap);
                    if (principalEntry.Mapping.Relations.Count > 0 || principalEntry.Mapping.ReferenceNavigations.Length > 0)
                        queue.Enqueue(principalEntry);
                }
                else if (principalEntry.State == EntityState.Deleted)
                {
                    continue;
                }

                var pkColumn = principalMap.KeyColumns[0];
                if (pkColumn.IsDbGenerated && principalEntry.State == EntityState.Added
                    && IsDefaultDbGeneratedKey(principal, principalMap))
                {
                    // The key does not exist yet; link after the principal's INSERT hydrates it.
                    ChangeTracker.RegisterPendingReferenceKeyFixup(principal, dependent, fk, pkColumn);
                    if (entry.State is EntityState.Unchanged or EntityState.Modified)
                    {
                        // The FK write lands during SaveChanges itself (after the principal's
                        // insert), which is after change detection — mark the row now or its
                        // UPDATE is skipped and the link is silently lost.
                        entry.SetStateInternal(EntityState.Modified);
                        entry.MarkExplicitlyModified();
                    }
                    continue;
                }

                var pkValue = pkColumn.Getter(principal);
                var fkValue = fk.Getter(dependent);
                if (Equals(fkValue, pkValue))
                    continue;
                if (entry.State != EntityState.Added)
                {
                    if (fk.IsKey)
                        continue;
                    if (entry.HasColumnValueChanged(fk))
                    {
                        // The deliberately edited FK outranks the stale navigation —
                        // and the navigation must be reconciled NOW: left pointing at
                        // the old principal, it would silently re-assert itself on
                        // the NEXT save, where the accepted baseline equals the
                        // edited FK and the edit is no longer visible. Point it at
                        // the tracked principal the FK now references, or null it.
                        var editedFk = fk.Getter(dependent);
                        object? editedPrincipal = editedFk != null
                            ? ChangeTracker.GetEntryByKey(principalMap.Type, editedFk)?.Entity
                            : null;
                        try { navProp.SetValue(dependent, editedPrincipal); }
                        catch { /* read-only navigation — leave the stale reference */ }
                        continue;
                    }
                    entry.SetStateInternal(EntityState.Modified);
                    entry.MarkExplicitlyModified();
                }
                fk.Setter(dependent, pkValue);
            }
        }
    }
}
