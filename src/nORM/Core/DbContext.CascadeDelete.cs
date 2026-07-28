using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Globalization;
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
        /// Client-side cascade delete: marks TRACKED dependents of Deleted
        /// principals as Deleted, transitively, for relations configured with
        /// <c>CascadeDelete</c>. Dependents are matched by foreign key value, so
        /// loaded children cascade whether or not they sit in the navigation
        /// collection. Added dependents were never persisted and detach instead.
        /// Unloaded dependents are the database referential action's
        /// responsibility (migrations emit ON DELETE CASCADE for these relations).
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Cascade marking reads relation metadata via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Cascade marking reflects over relations; trimming may remove the required members.")]
        private void CascadeMarkDeletedDependents()
        {
            Queue<EntityEntry>? queue = null;
            foreach (var e in ChangeTracker.Entries)
            {
                if (e.State == EntityState.Deleted && e.Mapping.Relations.Count > 0)
                    (queue ??= new Queue<EntityEntry>()).Enqueue(e);
            }
            if (queue == null)
                return;

            var trackedByType = ChangeTracker.Entries
                .GroupBy(e => e.Mapping.Type)
                .ToDictionary(g => g.Key, g => g.ToList());

            // Added dependents reached by the cascade are detached (never persisted), but only AFTER the
            // walk so their own entries stay valid while their subtrees are traversed (see below).
            HashSet<object>? addedToDetach = null;

            while (queue.Count > 0)
            {
                var principalEntry = queue.Dequeue();
                var principal = principalEntry.Entity;
                if (principal == null)
                    continue;

                foreach (var relation in principalEntry.Mapping.Relations.Values)
                {
                    // nORM applies referential actions to loaded/tracked dependents client-side (that is how
                    // cascade works even with no DB FK constraint). NoAction leaves them to the database; every
                    // other action (Cascade / SetNull / SetDefault / Restrict) must be applied to the tracked
                    // dependents below — omitting SetNull/SetDefault/Restrict left dangling-FK orphans and let
                    // a Restrict delete silently succeed.
                    if (relation.OnDelete == ReferentialAction.NoAction)
                        continue;
                    if (!trackedByType.TryGetValue(relation.DependentType, out var dependents))
                        continue;

                    var principalKeyValues = new object?[relation.PrincipalKeys.Count];
                    for (var i = 0; i < relation.PrincipalKeys.Count; i++)
                        principalKeyValues[i] = relation.PrincipalKeys[i].Getter(principal);

                    // A dependent can be linked to the principal by navigation rather
                    // than by its current FK value. An Added dependent's FK is still
                    // default until fixup runs; a persisted dependent re-parented via a
                    // reference navigation keeps its stale FK — in both cases because
                    // fixup skips deleted principals. Honor the navigation for both so an
                    // Added child does not INSERT with a dangling FK and a persisted
                    // re-parented child cascades with the principal it now points at.
                    HashSet<object>? collectionMembers = null;
                    if (relation.NavProp.GetValue(principal) is System.Collections.IEnumerable membersEnumerable
                        && membersEnumerable is not string)
                    {
                        foreach (var member in membersEnumerable)
                        {
                            if (member != null)
                                (collectionMembers ??= new HashSet<object>(ReferenceEqualityComparer.Instance)).Add(member);
                        }
                    }

                    foreach (var dependentEntry in dependents)
                    {
                        if (dependentEntry.State is EntityState.Deleted or EntityState.Detached)
                            continue;
                        var dependent = dependentEntry.Entity;
                        if (dependent == null)
                            continue;
                        var matches = true;
                        for (var i = 0; i < relation.ForeignKeys.Count && matches; i++)
                            matches = Equals(relation.ForeignKeys[i].Getter(dependent), principalKeyValues[i]);

                        if (!matches)
                        {
                            var added = dependentEntry.State == EntityState.Added;

                            // Collection membership is honored for Added dependents only: a
                            // persisted child moved out by a deliberate FK edit can linger in
                            // its former parent's collection (a stale membership that fixup does
                            // not scrub), so honoring it here would over-cascade the child with
                            // a principal it no longer belongs to.
                            if (added && collectionMembers != null && collectionMembers.Contains(dependent))
                            {
                                matches = true;
                            }
                            else
                            {
                                // Reference navigation is honored when the FK cannot speak for
                                // the relationship: an Added dependent has no persisted FK yet,
                                // and a persisted dependent re-parented via reference navigation
                                // keeps a stale FK because fixup skips deleted principals. A
                                // deliberately edited FK outranks a stale navigation (fixup
                                // reconciles the nav to it), so a persisted dependent whose FK
                                // changed this save is not matched by navigation.
                                var honorNav = added;
                                if (!honorNav)
                                {
                                    honorNav = true;
                                    for (var i = 0; i < relation.ForeignKeys.Count; i++)
                                    {
                                        if (dependentEntry.HasColumnValueChanged(relation.ForeignKeys[i]))
                                        {
                                            honorNav = false;
                                            break;
                                        }
                                    }
                                }

                                if (honorNav)
                                {
                                    foreach (var navProp in dependentEntry.Mapping.ReferenceNavigations)
                                    {
                                        if (navProp.PropertyType != principalEntry.Mapping.Type)
                                            continue;
                                        // Only the navigation that actually BACKS this cascade relation
                                        // (its foreign key is one of relation.ForeignKeys) may pull the
                                        // dependent in. A second, unrelated navigation to the same
                                        // principal CLR type — e.g. a non-cascading Editor alongside the
                                        // cascading Author — must not: honoring it would silently delete a
                                        // dependent that belongs to a DIFFERENT, surviving principal via
                                        // its cascade foreign key.
                                        var navFk = nORM.Query.ExpressionToSqlVisitor.FindReferenceNavForeignKey(
                                            dependentEntry.Mapping, navProp.Name, navProp.PropertyType, principalEntry.Mapping);
                                        if (navFk == null ||
                                            !relation.ForeignKeys.Any(fk => string.Equals(fk.Name, navFk.Name, StringComparison.Ordinal)))
                                            continue;
                                        object? navValue;
                                        try { navValue = navProp.GetValue(dependent); }
                                        catch { continue; }
                                        if (ReferenceEquals(navValue, principal))
                                        {
                                            matches = true;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        if (!matches)
                            continue;

                        if (relation.OnDelete == ReferentialAction.Restrict)
                            throw new InvalidOperationException(
                                $"Cannot delete an instance of '{principalEntry.Mapping.Type.Name}': relationship " +
                                $"'{relation.NavProp.Name}' to '{dependentEntry.Mapping.Type.Name}' is configured with " +
                                "Restrict and a tracked dependent still references it. Delete or reassign the dependent first.");

                        if (relation.OnDelete is ReferentialAction.SetNull or ReferentialAction.SetDefault)
                        {
                            // The dependent is NOT deleted — sever it by writing NULL (SetNull) or the FK column
                            // type default (SetDefault) and marking it Modified so the UPDATE is emitted. Mirrors
                            // the reference-nav-clear sever path. An Added dependent stays Added and inserts with
                            // the severed FK. A required (non-nullable) FK cannot be nulled — left for the DB to
                            // reject, matching a misconfigured SetNull.
                            var setNull = relation.OnDelete == ReferentialAction.SetNull;
                            for (var i = 0; i < relation.ForeignKeys.Count; i++)
                            {
                                var fk = relation.ForeignKeys[i];
                                if (setNull)
                                {
                                    if (fk.IsNullable)
                                        fk.Setter(dependent, null);
                                }
                                else
                                {
                                    var fkType = fk.Prop.PropertyType;
                                    fk.Setter(dependent, fkType.IsValueType && Nullable.GetUnderlyingType(fkType) == null
                                        ? Activator.CreateInstance(fkType)
                                        : null);
                                }
                            }
                            if (dependentEntry.State is EntityState.Unchanged or EntityState.Modified)
                            {
                                dependentEntry.SetStateInternal(EntityState.Modified);
                                dependentEntry.MarkExplicitlyModified();
                            }
                            continue;
                        }

                        // ReferentialAction.Cascade
                        if (dependentEntry.State == EntityState.Added)
                        {
                            // Never persisted — nothing to DELETE. But its OWN Added descendants must also be
                            // detached, or they would be inserted as orphans (their FK points at this
                            // now-untracked parent, still default 0). Enqueue it so the walk reaches its
                            // grandchildren, and DEFER the untrack until the walk completes — removing it now
                            // would clear the entry's Entity and the dequeue would skip its subtree.
                            addedToDetach ??= new HashSet<object>(ReferenceEqualityComparer.Instance);
                            // Add returns false if already scheduled — avoids re-walking a cyclic Added graph.
                            if (addedToDetach.Add(dependent) && dependentEntry.Mapping.Relations.Count > 0)
                                queue.Enqueue(dependentEntry);
                            continue;
                        }

                        dependentEntry.SetStateInternal(EntityState.Deleted);
                        if (dependentEntry.Mapping.Relations.Count > 0)
                            queue.Enqueue(dependentEntry);
                    }
                }
            }

            // Untrack the Added dependents the cascade reached, now that the whole subtree has been walked.
            if (addedToDetach != null)
                foreach (var added in addedToDetach)
                    ChangeTracker.Remove(added);
        }

        /// <summary>
        /// Strips just-deleted instances out of tracked entities' navigations. A
        /// deleted instance left sitting in a navigation would be re-discovered by
        /// relationship fixup on the NEXT SaveChanges — its tracker entry is gone
        /// by then — and silently re-inserted: a deleted child through a principal's
        /// collection, or a deleted principal through a dependent's reference.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Navigation cleanup reads navigation properties via reflection; not NativeAOT-compatible.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Navigation cleanup reflects over navigation properties; trimming may remove the required members.")]
        private void RemoveDeletedInstancesFromTrackedNavigations(IReadOnlyList<object> deleted)
        {
            foreach (var entry in ChangeTracker.Entries)
            {
                var entity = entry.Entity;
                if (entity == null)
                    continue;

                foreach (var relation in entry.Mapping.Relations.Values)
                {
                    var navValue = relation.NavProp.GetValue(entity);
                    if (navValue == null)
                        continue;
                    if (navValue is System.Collections.IList list)
                    {
                        if (list.IsReadOnly)
                            continue;
                        foreach (var gone in deleted)
                        {
                            if (relation.DependentType.IsInstanceOfType(gone))
                                list.Remove(gone);
                        }
                    }
                    else
                    {
                        // A collection navigation whose runtime type is NOT IList (HashSet<T> / ISet<T> /
                        // any non-IList ICollection<T>) must also have the deleted instances stripped, via
                        // the generic ICollection<T>.Remove — otherwise the just-deleted child stays in the
                        // collection and the next SaveChanges' fixup rediscovers the now-untracked instance
                        // and re-inserts it (resurrection).
                        RemoveDeletedFromNonListCollection(navValue, relation.DependentType, deleted);
                    }
                }

                foreach (var navProp in entry.Mapping.ReferenceNavigations)
                {
                    object? current;
                    try { current = navProp.GetValue(entity); }
                    catch { continue; }
                    if (current == null)
                        continue;
                    foreach (var gone in deleted)
                    {
                        if (ReferenceEquals(current, gone))
                        {
                            try { navProp.SetValue(entity, null); }
                            catch { /* read-only navigation — leave it */ }
                            break;
                        }
                    }
                }
            }
        }

        // Cache of the closed ICollection<T> Remove method + IsReadOnly property per dependent element type,
        // used to strip deleted instances from non-IList collection navigations.
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<Type,
            (Type CollType, System.Reflection.MethodInfo Remove, System.Reflection.PropertyInfo IsReadOnly)?> _nonListCollectionRemoveCache = new();

        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Resolves ICollection<T>.Remove via reflection; trimming may remove the member.")]
        private static void RemoveDeletedFromNonListCollection(object navValue, Type dependentType, IReadOnlyList<object> deleted)
        {
            var info = _nonListCollectionRemoveCache.GetOrAdd(dependentType, static dt =>
            {
                var collType = typeof(System.Collections.Generic.ICollection<>).MakeGenericType(dt);
                var remove = collType.GetMethod("Remove");
                var isReadOnly = collType.GetProperty("IsReadOnly");
                return remove != null && isReadOnly != null
                    ? (collType, remove, isReadOnly)
                    : ((Type, System.Reflection.MethodInfo, System.Reflection.PropertyInfo)?)null;
            });
            if (info == null)
                return;
            var (collType, removeMethod, isReadOnlyProp) = info.Value;
            if (!collType.IsInstanceOfType(navValue) || isReadOnlyProp.GetValue(navValue) is true)
                return;
            var args = new object[1];
            foreach (var gone in deleted)
            {
                if (dependentType.IsInstanceOfType(gone))
                {
                    args[0] = gone;
                    removeMethod.Invoke(navValue, args);
                }
            }
        }

        /// <summary>
        /// Materialized grouping used by SaveChanges ordering when a group must be
        /// split (replace-in-place delete hoisting) while keeping the IGrouping
        /// pipeline shape.
        /// </summary>
        /// <summary>
        /// True when every changed entry shares the same <see cref="EntityEntry.State"/> and mapping — the
        /// common single-type/single-state save that can skip the grouping/topological-sort/replacement
        /// pipeline (see SaveChangesAsync). O(n), allocation-free.
        /// </summary>
        private static bool AllSameStateAndMapping(List<EntityEntry> entries)
        {
            var state = entries[0].State;
            var mapping = entries[0].Mapping;
            for (int i = 1; i < entries.Count; i++)
                if (entries[i].State != state || !ReferenceEquals(entries[i].Mapping, mapping))
                    return false;
            return true;
        }

        private sealed class SaveChangesEntryGroup : List<EntityEntry>, IGrouping<(EntityState State, TableMapping Mapping), EntityEntry>
        {
            public SaveChangesEntryGroup((EntityState State, TableMapping Mapping) key, IEnumerable<EntityEntry> entries)
                : base(entries) => Key = key;

            public (EntityState State, TableMapping Mapping) Key { get; }
        }
    }
}
