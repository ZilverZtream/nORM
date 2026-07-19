using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Configuration;
using nORM.Mapping;
using nORM.Navigation;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Represents the change tracking information for a single entity instance.
    /// An <see cref="EntityEntry"/> keeps the original values and state required to
    /// compute database updates when <c>SaveChanges</c> is invoked.
    /// </summary>
    /// <remarks>
    /// PERFORMANCE OPTIMIZATION: Deferred initialization of tracking arrays.
    /// Arrays are null until <see cref="InitializeTracking"/> is called, reducing
    /// memory overhead for read-only or short-lived entities (approximately 200-500
    /// bytes per entity depending on column count).
    /// </remarks>
    public class EntityEntry
    {
        /// <summary>Initial seed for the polynomial byte-array hash in <see cref="ContentHashCode"/>.</summary>
        private const int ByteArrayHashSeed = 17;

        /// <summary>Multiplier for the polynomial byte-array hash in <see cref="ContentHashCode"/>.</summary>
        private const int ByteArrayHashMultiplier = 31;

        private static long _attachCounter;

        // Monotonic creation sequence so change-tracker enumeration (and therefore SaveChanges
        // batch order) is deterministic in Add/Attach order. The backing stores are
        // ConcurrentDictionaries whose enumeration order is identity-hash based and varies from
        // run to run — without this, batched INSERT order (and thus autoincrement key
        // assignment) was nondeterministic.
        internal long AttachSequence { get; } = System.Threading.Interlocked.Increment(ref _attachCounter);

        private readonly TableMapping _mapping;
        // PERFORMANCE: Use null instead of Array.Empty to truly defer allocation
        private Column[]? _nonKeyColumns;
        private int[]? _originalHashes;
        private object?[]? _originalValues;
        private BitArray? _changedProperties;
        private Func<object, int>[]? _getHashCodes;
        private Func<object, object?>[]? _getValues;
        private Dictionary<string, int>? _propertyIndex;
        private readonly DbContextOptions _options;
        private bool _hasNotifiedChange;
        private bool _isInitialized;

        /// <summary>
        /// Gets the entity instance being tracked. May be <c>null</c> after the entity
        /// has been detached from the context.
        /// </summary>
        public object? Entity { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the entity's primary key has been assigned a non-default value
        /// (Entity Framework Core parity). Any key column still at its CLR default (0, <see cref="Guid.Empty"/>,
        /// or <c>null</c>) makes the key unset — so a store-generated key reads as unset until the database
        /// assigns it. A keyless entity, or a detached entry that has released its entity, is always unset.
        /// </summary>
        public bool IsKeySet
        {
            get
            {
                var entity = Entity;
                if (entity == null) return false;
                var keys = _mapping.KeyColumns;
                if (keys.Length == 0) return false;
                foreach (var col in keys)
                    if (ChangeTracker.IsDefaultKeyValue(col.Getter(entity), col.Prop.PropertyType))
                        return false;
                return true;
            }
        }

        private EntityState _state;

        /// <summary>
        /// Gets or sets the current <see cref="EntityState"/> of the tracked entity within the context.
        /// Assigning a new state performs the corresponding tracker transition — setting
        /// <see cref="EntityState.Modified"/> flags the entity for a full UPDATE, <see cref="EntityState.Deleted"/>
        /// for a DELETE (or detaches a never-inserted <see cref="EntityState.Added"/> entity),
        /// <see cref="EntityState.Unchanged"/> accepts the current values as the clean baseline, and
        /// <see cref="EntityState.Detached"/> stops tracking the entity — mirroring
        /// <c>DbContext.Update</c>/<c>Remove</c>/<c>Attach</c>. Internal bookkeeping uses
        /// <see cref="SetStateInternal"/>, which assigns the field without any transition side effects.
        /// </summary>
        public EntityState State
        {
            get => _state;
            set => SetEntityState(value);
        }

        /// <summary>
        /// Assigns the raw state field with no transition side effects. Used by the change tracker and
        /// the save pipeline, which manage identity-map membership and dirty registration themselves.
        /// </summary>
        internal void SetStateInternal(EntityState state) => _state = state;

        /// <summary>
        /// The change tracker that owns this entry, used by the public <see cref="State"/> setter to
        /// detach the entity (Detached, or Deleting a never-inserted Added entity). Null only for the
        /// rare entry constructed without a tracker.
        /// </summary>
        internal ChangeTracker? Tracker { get; }

        /// <summary>
        /// Applies the EF-style state transition for a user assignment to <see cref="State"/>.
        /// </summary>
        private void SetEntityState(EntityState newState)
        {
            // Added/Deleted/Detached are terminal enough that re-applying the same one is a no-op.
            // Modified and Unchanged always run: change detection is deferred, so a nominally-Unchanged
            // entry may hold pending edits that "= Unchanged" must accept, and "= Modified" force-marks.
            if (newState == _state && newState is EntityState.Added or EntityState.Deleted or EntityState.Detached)
                return;

            var entity = Entity;
            if (entity == null)
            {
                // A detached entry has released its entity; only re-detaching is meaningful.
                if (newState == EntityState.Detached)
                    return;
                throw new InvalidOperationException(
                    "Cannot change the state of an entry whose entity has already been detached.");
            }

            switch (newState)
            {
                case EntityState.Detached:
                    if (Tracker != null)
                        Tracker.Remove(entity);
                    else
                        DetachEntity();
                    return;

                case EntityState.Deleted:
                    // A never-inserted Added entity has no row to delete — deleting it just stops
                    // tracking it, matching EF. A persisted entity is marked for a DELETE by key.
                    if (_state == EntityState.Added)
                    {
                        if (Tracker != null)
                            Tracker.Remove(entity);
                        else
                            DetachEntity();
                        return;
                    }
                    _state = EntityState.Deleted;
                    return;

                case EntityState.Added:
                    _state = EntityState.Added;
                    return;

                case EntityState.Modified:
                    UpgradeToFullTracking();
                    _state = EntityState.Modified;
                    // Keep DetectChanges from reverting to Unchanged when no scalar diff is found,
                    // and flag every column so the entry reports a full modification.
                    _hasNotifiedChange = true;
                    MarkAllPropertiesModified();
                    Tracker?.MarkDirty(this);
                    return;

                case EntityState.Unchanged:
                    // Accept the current values (and collection/owned/token/key snapshots) as the new
                    // clean baseline so a subsequent SaveChanges does not re-detect and persist them.
                    AcceptChanges();
                    return;
            }
        }

        /// <summary>Flags every tracked column as modified. Requires <see cref="UpgradeToFullTracking"/> first.</summary>
        private void MarkAllPropertiesModified()
        {
            if (_changedProperties == null) return;
            for (int i = 0; i < _changedProperties.Length; i++)
                _changedProperties[i] = true;
        }

        /// <summary>
        /// Gets the entity's current property values as a mutable bag. Writing a value assigns it to the
        /// entity and marks the entry dirty, mirroring EF's <c>CurrentValues</c>.
        /// </summary>
        public PropertyValues CurrentValues => new PropertyValues(this, original: false);

        /// <summary>
        /// Gets the entity's original (as-loaded / last-saved) property values as a bag. Writing a value
        /// overrides the tracked baseline — e.g. resetting a concurrency token to the database value while
        /// resolving a conflict — mirroring EF's <c>OriginalValues</c>. Original key values are read-only.
        /// </summary>
        public PropertyValues OriginalValues => new PropertyValues(this, original: true);

        /// <summary>
        /// Overwrites the entity's column values from a fresh read of its database row and resets the
        /// entry to <see cref="EntityState.Unchanged"/>, discarding any pending edits — mirroring EF's
        /// <c>Reload</c>. If the row no longer exists the entity is detached. Navigation properties are
        /// not reloaded. Throws when the entry is detached or not associated with a context.
        /// </summary>
        [RequiresDynamicCode("Reload builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reload reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public void Reload() => RequireContext().ReloadEntry(this);

        /// <summary>
        /// Asynchronous <see cref="Reload"/>: overwrites the entity's column values from a fresh read of
        /// its database row and resets the entry to <see cref="EntityState.Unchanged"/> (or detaches it
        /// when the row is gone). Navigation properties are not reloaded.
        /// </summary>
        [RequiresDynamicCode("Reload builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reload reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public Task ReloadAsync(CancellationToken ct = default) => RequireContext().ReloadEntryAsync(this, ct);

        /// <summary>
        /// Reads the entity's current values straight from its database row as a detached
        /// <see cref="PropertyValues"/> snapshot, without disturbing the tracked entity or its state —
        /// mirroring EF's <c>GetDatabaseValues</c>. Returns <c>null</c> when the row no longer exists. The
        /// canonical use is resolving a <see cref="DbConcurrencyException"/>: compare the returned store
        /// values against <see cref="CurrentValues"/>/<see cref="OriginalValues"/>, then
        /// <see cref="PropertyValues.SetValues(object)"/> the chosen winner back before retrying the save.
        /// Throws when the entry is detached or not associated with a context.
        /// </summary>
        [RequiresDynamicCode("GetDatabaseValues builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("GetDatabaseValues reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public PropertyValues? GetDatabaseValues() => RequireContext().GetDatabaseValuesForEntry(this);

        /// <summary>
        /// Asynchronous <see cref="GetDatabaseValues"/>: reads the entity's current database row as a
        /// detached <see cref="PropertyValues"/> snapshot (or <c>null</c> when the row is gone) without
        /// affecting the tracked entity.
        /// </summary>
        [RequiresDynamicCode("GetDatabaseValues builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("GetDatabaseValues reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        public Task<PropertyValues?> GetDatabaseValuesAsync(CancellationToken ct = default)
            => RequireContext().GetDatabaseValuesForEntryAsync(this, ct);

        private DbContext RequireContext()
            => Tracker?.Context
               ?? throw new InvalidOperationException(
                   "This entry is not associated with a context. Obtain it from context.Entry(entity).");

        /// <summary>The owning context, or null when this entry is not tracker-bound.</summary>
        internal DbContext? Context => Tracker?.Context;

        /// <summary>
        /// Gets an entry for a navigation property by name (reference or collection), for inspecting
        /// its loaded state or explicitly loading it — mirroring EF's <c>Entry(e).Navigation(name)</c>.
        /// </summary>
        public NavigationEntry Navigation(string navigationName)
            => new NavigationEntry(this, ResolveNavigation(navigationName, requireCollection: null));

        /// <summary>
        /// Gets an entry for a reference (to-one) navigation property by name, e.g.
        /// <c>Entry(order).Reference("Customer").Load()</c>.
        /// </summary>
        public NavigationEntry Reference(string navigationName)
            => new NavigationEntry(this, ResolveNavigation(navigationName, requireCollection: false));

        /// <summary>
        /// Gets an entry for a collection (to-many) navigation property by name, e.g.
        /// <c>Entry(order).Collection("Lines").Load()</c>.
        /// </summary>
        public NavigationEntry Collection(string navigationName)
            => new NavigationEntry(this, ResolveNavigation(navigationName, requireCollection: true));

        private PropertyInfo ResolveNavigation(string navigationName, bool? requireCollection)
        {
            if (string.IsNullOrEmpty(navigationName))
                throw new ArgumentException("Navigation name cannot be null or empty.", nameof(navigationName));
            var prop = _mapping.Type.GetProperty(navigationName)
                ?? throw new ArgumentException(
                    $"'{_mapping.Type.Name}' has no property named '{navigationName}'.", nameof(navigationName));
            if (_mapping.ColumnsByName.ContainsKey(navigationName))
                throw new ArgumentException(
                    $"'{navigationName}' is a mapped scalar property, not a navigation.", nameof(navigationName));

            if (requireCollection.HasValue)
            {
                var isCollection = prop.PropertyType != typeof(string)
                    && typeof(IEnumerable).IsAssignableFrom(prop.PropertyType);
                if (requireCollection.Value && !isCollection)
                    throw new ArgumentException(
                        $"'{navigationName}' is a reference navigation; use Reference(\"{navigationName}\").", nameof(navigationName));
                if (!requireCollection.Value && isCollection)
                    throw new ArgumentException(
                        $"'{navigationName}' is a collection navigation; use Collection(\"{navigationName}\").", nameof(navigationName));
            }
            return prop;
        }

        /// <summary>The CLR type this entry maps.</summary>
        internal Type MappedType => _mapping.Type;

        /// <summary>All mapped columns (keys, timestamp, and regular) backing the property bags.</summary>
        internal IReadOnlyList<Column> MappedColumns => _mapping.Columns;

        /// <summary>Resolves a mapped column by its CLR property name, throwing when there is no match.</summary>
        internal Column RequireColumn(string propertyName)
        {
            foreach (var c in _mapping.Columns)
                if (string.Equals(c.PropName, propertyName, StringComparison.Ordinal))
                    return c;
            throw new ArgumentException(
                $"'{_mapping.Type.Name}' has no mapped property named '{propertyName}'.", nameof(propertyName));
        }

        /// <summary>
        /// Reads a column's current entity value, or its original (as-loaded) value when
        /// <paramref name="original"/> is set — resolving keys from <see cref="OriginalKey"/>, the
        /// concurrency token from <see cref="OriginalToken"/>, and everything else from the change snapshot.
        /// </summary>
        internal object? ReadValue(Column col, bool original)
        {
            if (!original)
                return Entity != null ? col.Getter(Entity) : null;

            UpgradeToFullTracking();
            if (col.IsKey)
                return ReadOriginalKeyComponent(col);
            if (col.IsTimestamp)
                return OriginalToken;
            if (_propertyIndex != null && _propertyIndex.TryGetValue(col.Prop.Name, out var idx))
                return _originalValues![idx];
            // No snapshot slot (e.g. a computed column not tracked for changes) — fall back to current.
            return Entity != null ? col.Getter(Entity) : null;
        }

        /// <summary>
        /// Writes a column's current entity value (marking the entry dirty), or its original baseline when
        /// <paramref name="original"/> is set. Original key values cannot be changed.
        /// </summary>
        internal void WriteValue(Column col, object? value, bool original)
        {
            var coerced = CoerceToColumnType(col, value);
            if (!original)
            {
                var entity = Entity ?? throw new InvalidOperationException("Cannot set values on a detached entry.");
                col.Setter(entity, coerced);
                Tracker?.MarkDirty(this);
                return;
            }

            UpgradeToFullTracking();
            if (col.IsTimestamp)
            {
                OriginalToken = coerced;
                return;
            }
            if (col.IsKey)
                throw new InvalidOperationException($"The original key value of '{col.PropName}' cannot be changed.");
            if (_propertyIndex != null && _propertyIndex.TryGetValue(col.Prop.Name, out var idx))
                _originalValues![idx] = coerced;
            else
                throw new InvalidOperationException($"'{col.PropName}' has no original-value slot to set.");
        }

        private object? ReadOriginalKeyComponent(Column col)
        {
            if (OriginalKey == null)
                return Entity != null ? col.Getter(Entity) : null;
            var keys = _mapping.KeyColumns;
            if (keys.Length == 1)
                return OriginalKey;
            var pos = Array.IndexOf(keys, col);
            return OriginalKey is object?[] arr && pos >= 0 && pos < arr.Length ? arr[pos] : OriginalKey;
        }

        /// <summary>Coerces a bag value to the column's CLR type; leaves it as-is (letting the setter surface
        /// a precise error) when no clean conversion applies.</summary>
        private static object? CoerceToColumnType(Column col, object? value)
        {
            if (value == null)
                return null;
            var target = Nullable.GetUnderlyingType(col.Prop.PropertyType) ?? col.Prop.PropertyType;
            if (target.IsInstanceOfType(value))
                return value;
            try
            {
                if (target.IsEnum)
                    return Enum.ToObject(target, value);
                return Convert.ChangeType(value, target, System.Globalization.CultureInfo.InvariantCulture);
            }
            catch (Exception ex) when (ex is InvalidCastException or FormatException or OverflowException)
            {
                return value;
            }
        }

        /// <summary>
        /// Marks this entry as explicitly modified (e.g. via ctx.Update), preventing
        /// DetectChanges from reverting the state to Unchanged when no scalar changes are found.
        /// </summary>
        internal void MarkExplicitlyModified()
        {
            _hasNotifiedChange = true;
        }

        /// <summary>
        /// Stores the original concurrency token (timestamp/rowversion) value captured at
        /// attach time. Used in UPDATE and DELETE WHERE clauses to ensure the correct snapshot
        /// value is compared, even if the entity's property has been mutated before SaveChanges.
        /// </summary>
        internal object? OriginalToken { get; set; }

        /// <summary>
        /// Stores the primary key value captured at attach/track time.
        /// Used by SaveChanges to detect PK mutations before issuing UPDATE statements.
        /// </summary>
        internal object? OriginalKey { get; set; }

        internal TableMapping Mapping => _mapping;

        /// <summary>
        /// Snapshots of many-to-many collections at attach/accept time.
        /// Key = nav property name; Value = set of related-entity PK values at last save.
        /// Used by SaveChanges to compute toAdd/toRemove deltas without re-querying the join table.
        /// </summary>
        internal Dictionary<string, HashSet<object>>? ManyToManySnapshots { get; private set; }

        /// <summary>
        /// Captures the current M2M collection state as the new snapshot baseline.
        /// Called after SaveChanges succeeds and after initial attach.
        /// </summary>
        internal void CaptureManyToManySnapshots()
        {
            if (_mapping.ManyToManyJoins == null || _mapping.ManyToManyJoins.Count == 0) return;
            var entity = Entity;
            if (entity == null) return;

            ManyToManySnapshots ??= new Dictionary<string, HashSet<object>>();
            foreach (var jtm in _mapping.ManyToManyJoins)
            {
                var collection = jtm.LeftCollectionGetter(entity);
                var snapshot = new HashSet<object>();
                if (collection != null)
                {
                    foreach (var item in collection)
                    {
                        if (item == null) continue;
                        var pk = jtm.GetRightKey(item);
                        if (pk != null) snapshot.Add(pk);
                    }
                }
                ManyToManySnapshots[jtm.LeftNavPropertyName] = snapshot;
            }
        }

        /// Names of reference navigations that held a non-null principal when the
        /// entity was loaded. Lets fixup distinguish "the user cleared a loaded
        /// reference nav" (so the FK must be nulled) from "the nav was never loaded"
        /// (so a valid FK must be preserved). Null until first captured.
        internal HashSet<string>? LoadedReferenceNavs { get; private set; }

        /// <summary>
        /// Records which reference navigations are currently non-null so a later
        /// clear (nav set to null) is detected as a real disassociation.
        /// </summary>
        internal void CaptureReferenceNavSnapshots()
        {
            var navs = _mapping.ReferenceNavigations;
            if (navs.Length == 0) return;
            var entity = Entity;
            if (entity == null) return;

            foreach (var nav in navs)
            {
                object? value;
                try { value = nav.GetValue(entity); }
                catch { continue; }
                if (value != null)
                    (LoadedReferenceNavs ??= new HashSet<string>(StringComparer.Ordinal)).Add(nav.Name);
            }
        }

        /// Per one-to-many collection navigation, the set of child instances present
        /// when the collection was loaded via Include. Lets fixup detect a child that
        /// the user removed from a loaded collection (in the snapshot, absent now) and
        /// sever it — the collection-side mirror of clearing a reference navigation.
        /// Reference identity is used: the loaded children are the tracked instances.
        /// Only navigations that were actually loaded get a snapshot, so an unloaded
        /// (empty) collection never triggers a spurious severance. Null until captured.
        internal Dictionary<string, HashSet<object>>? CollectionNavSnapshots { get; private set; }

        /// <summary>
        /// Records the instances currently held by a loaded collection navigation as
        /// the severance baseline. Called by the Include processor after it populates
        /// the collection, and refreshed on AcceptChanges for already-loaded navs.
        /// </summary>
        internal void CaptureCollectionNavSnapshot(string navName, IEnumerable? collection)
        {
            var set = new HashSet<object>(ReferenceEqualityComparer.Instance);
            if (collection != null && collection is not string)
            {
                foreach (var item in collection)
                    if (item != null) set.Add(item);
            }
            (CollectionNavSnapshots ??= new Dictionary<string, HashSet<object>>(StringComparer.Ordinal))[navName] = set;
        }

        /// <summary>
        /// Re-captures the baseline for every collection navigation that already has a
        /// snapshot (i.e. was loaded), reading the current post-save contents. Skips
        /// unloaded navs so they never begin tracking severances.
        /// </summary>
        private void RecaptureLoadedCollectionNavSnapshots()
        {
            if (CollectionNavSnapshots == null || CollectionNavSnapshots.Count == 0) return;
            var entity = Entity;
            if (entity == null) return;
            foreach (var relation in _mapping.Relations.Values)
            {
                if (!CollectionNavSnapshots.ContainsKey(relation.NavProp.Name)) continue;
                var value = relation.NavProp.GetValue(entity) as IEnumerable;
                CaptureCollectionNavSnapshot(relation.NavProp.Name, value);
            }
        }

        /// Owned-collection content snapshots: per nav property, a multiset of each
        /// child's column-value signature captured when the collection was loaded.
        /// Used to detect owned-collection edits (add / remove / child-scalar change),
        /// which are otherwise invisible to column-only change detection and would be
        /// silently dropped — the owned sync only runs for a Modified owner.
        internal Dictionary<string, List<string>>? OwnedCollectionSnapshots { get; private set; }

        /// <summary>
        /// Captures the current owned-collection contents as the snapshot baseline.
        /// Called after the owned collections are loaded and after SaveChanges succeeds.
        /// </summary>
        internal void CaptureOwnedCollectionSnapshots()
        {
            if (_mapping.OwnedCollections == null || _mapping.OwnedCollections.Count == 0) return;
            var entity = Entity;
            if (entity == null) return;

            OwnedCollectionSnapshots ??= new Dictionary<string, List<string>>();
            foreach (var ocm in _mapping.OwnedCollections)
                OwnedCollectionSnapshots[ocm.NavigationProperty.Name] = OwnedContentSignatures(ocm, entity);
        }

        private static List<string> OwnedContentSignatures(nORM.Mapping.OwnedCollectionMapping ocm, object owner)
        {
            var signatures = new List<string>();
            var collection = ocm.CollectionGetter(owner);
            if (collection is System.Collections.IEnumerable items)
            {
                foreach (var item in items)
                {
                    if (item == null) continue;
                    var sb = new System.Text.StringBuilder();
                    foreach (var col in ocm.Columns)
                    {
                        sb.Append(col.Getter(item)?.ToString() ?? "\0");
                        sb.Append('\x1f');
                    }
                    signatures.Add(sb.ToString());
                }
            }
            signatures.Sort(StringComparer.Ordinal); // multiset compare, order-insensitive
            return signatures;
        }

        /// <summary>
        /// Compares each owned collection's current content signature multiset against
        /// its snapshot and returns true when a child was added, removed, or edited.
        /// </summary>
        internal bool HasOwnedCollectionChanges()
        {
            if (_mapping.OwnedCollections == null || _mapping.OwnedCollections.Count == 0) return false;
            var entity = Entity;
            if (entity == null) return false;

            foreach (var ocm in _mapping.OwnedCollections)
            {
                var current = OwnedContentSignatures(ocm, entity);
                var snapshot = OwnedCollectionSnapshots != null
                    && OwnedCollectionSnapshots.TryGetValue(ocm.NavigationProperty.Name, out var snap)
                    ? snap
                    : null;
                if (snapshot == null)
                {
                    // No baseline captured (collection never loaded). A populated
                    // collection with no snapshot is a change; an empty one is not.
                    if (current.Count > 0) return true;
                    continue;
                }
                if (snapshot.Count != current.Count) return true;
                for (var i = 0; i < current.Count; i++)
                    if (!string.Equals(snapshot[i], current[i], StringComparison.Ordinal))
                        return true;
            }
            return false;
        }

        /// <summary>
        /// Compares each tracked many-to-many collection against its snapshot and returns
        /// true when the association set has changed (an item added or removed). Change
        /// detection is otherwise column-only, so without this a collection-only edit
        /// leaves the owner Unchanged and its join-table sync never runs — silently
        /// dropping the association write.
        /// </summary>
        internal bool HasManyToManyChanges()
        {
            if (_mapping.ManyToManyJoins == null || _mapping.ManyToManyJoins.Count == 0) return false;
            var entity = Entity;
            if (entity == null) return false;

            foreach (var jtm in _mapping.ManyToManyJoins)
            {
                var snapshot = ManyToManySnapshots != null
                    && ManyToManySnapshots.TryGetValue(jtm.LeftNavPropertyName, out var snap)
                    ? snap
                    : null;
                var collection = jtm.LeftCollectionGetter(entity);

                var currentCount = 0;
                if (collection != null)
                {
                    foreach (var item in collection)
                    {
                        if (item == null) continue;
                        var pk = jtm.GetRightKey(item);
                        if (pk == null) continue;
                        currentCount++;
                        if (snapshot == null || !snapshot.Contains(pk))
                            return true; // an item not in the snapshot → added
                    }
                }
                // Every current item was in the snapshot; a size mismatch means the
                // snapshot had extra items → one or more were removed.
                if ((snapshot?.Count ?? 0) != currentCount)
                    return true;
            }
            return false;
        }

        internal EntityEntry(object entity, EntityState state, TableMapping mapping, DbContextOptions options, ChangeTracker? tracker = null, bool lazy = false)
        {
            Entity = entity ?? throw new ArgumentNullException(nameof(entity));
            _state = state;
            _mapping = mapping ?? throw new ArgumentNullException(nameof(mapping));
            _options = options ?? throw new ArgumentNullException(nameof(options));
            Tracker = tracker;
            _isInitialized = false;

            // Capture original PK at attach time so mutations can be detected.
            OriginalKey = CaptureKey(entity, mapping);

            // PERFORMANCE: Only initialize immediately if not lazy.
            // This saves ~200-500 bytes per entity for read-only scenarios.
            // When lazy=true, OriginalToken capture is deferred until
            // UpgradeToFullTracking() / InitializeTracking() is called
            // (triggered by the first DetectChanges or AcceptChanges).
            // This means OriginalToken may be null for lazily-tracked entities
            // until they are first inspected for changes — callers that read
            // OriginalToken must handle null (see AddParametersOptimized).
            if (!lazy)
            {
                InitializeTracking();
            }
        }

        /// <summary>
        /// Returns a snapshot of <paramref name="value"/> that is safe to store as an
        /// original-value baseline. For <c>byte[]</c> a defensive clone is returned so
        /// that in-place mutations to the array cannot silently match the snapshot;
        /// all other values are returned as-is (they are either immutable value types
        /// or reference types compared by identity/Equals).
        /// </summary>
        private static object? SnapshotValue(object? value)
            => value is byte[] b ? (object)((byte[])b.Clone()) : value;

        /// <summary>
        /// Content-aware equality check used when comparing current vs. original values.
        /// Provides a fast <see cref="object.ReferenceEquals"/> short-circuit, then
        /// byte-by-byte <see cref="Enumerable.SequenceEqual{TSource}(IEnumerable{TSource},IEnumerable{TSource})"/> for <c>byte[]</c>,
        /// and falls back to <see cref="object.Equals(object,object)"/> for everything else.
        /// </summary>
        private static bool ValuesEqual(object? current, object? original)
        {
            if (ReferenceEquals(current, original)) return true;
            if (current is byte[] cb && original is byte[] ob) return cb.SequenceEqual(ob);
            return Equals(current, original);
        }

        /// <summary>
        /// Content-based hash code for use in hash-based dirty detection.
        /// For <c>byte[]</c> a simple polynomial hash over the bytes is computed so
        /// that two equal arrays produce the same hash regardless of instance identity.
        /// All other types delegate to <see cref="object.GetHashCode"/>.
        /// </summary>
        private static int ContentHashCode(object? value)
        {
            if (value is byte[] b)
            {
                unchecked
                {
                    int h = ByteArrayHashSeed;
                    foreach (var by in b) h = h * ByteArrayHashMultiplier + by;
                    return h;
                }
            }
            return value?.GetHashCode() ?? 0;
        }

        /// <summary>
        /// Reads the primary key value(s) from an entity using the provided mapping.
        /// Returns null when the entity has no key columns defined.
        /// </summary>
        private static object? CaptureKey(object entity, TableMapping mapping)
        {
            if (mapping.KeyColumns.Length == 0) return null;
            if (mapping.KeyColumns.Length == 1) return mapping.KeyColumns[0].Getter(entity);
            var values = new object?[mapping.KeyColumns.Length];
            for (int i = 0; i < mapping.KeyColumns.Length; i++)
                values[i] = mapping.KeyColumns[i].Getter(entity);
            return values;
        }

        /// <summary>
        /// Prepares the entry for change tracking by caching getters and hash-code
        /// delegates for all non-key, non-timestamp columns and capturing the
        /// entity's original values. If the entity implements
        /// <see cref="INotifyPropertyChanged"/>, the entry subscribes to change
        /// notifications to enable real-time tracking.
        /// </summary>
        /// <remarks>
        /// PERFORMANCE: Only called when tracking is actually needed, not on entity load.
        /// </remarks>
        private void InitializeTracking()
        {
            if (_isInitialized)
                return;

            _nonKeyColumns = _mapping.Columns.Where(c => !c.IsKey && !c.IsTimestamp).ToArray();

            // Capture original timestamp value so UPDATE/DELETE can use the snapshot value
            // rather than the potentially-mutated current value of the entity property.
            var tsCol = _mapping.TimestampColumn;
            if (tsCol != null && Entity != null)
                OriginalToken = SnapshotValue(tsCol.Getter(Entity));
            _getHashCodes = new Func<object, int>[_nonKeyColumns.Length];
            _getValues = new Func<object, object?>[_nonKeyColumns.Length];
            _propertyIndex = new Dictionary<string, int>(StringComparer.Ordinal);
            for (int i = 0; i < _nonKeyColumns.Length; i++)
            {
                var getter = _nonKeyColumns[i].Getter;
                _getValues[i] = getter;
                _getHashCodes[i] = e => ContentHashCode(getter(e));
                _propertyIndex[_nonKeyColumns[i].Prop.Name] = i;
            }
            _originalHashes = new int[_nonKeyColumns.Length];
            _originalValues = new object?[_nonKeyColumns.Length];
            _changedProperties = new BitArray(_nonKeyColumns.Length);
            _isInitialized = true;

            CaptureOriginalValues();
            // Only capture M2M snapshots if entity is already Unchanged (loaded from DB).
            // For Added entities, AcceptChanges() will capture after the INSERT succeeds.
            // Skip when a snapshot already exists: an Include may have re-captured the
            // loaded association set already, and this lazy init runs at DetectChanges
            // time — AFTER any user edit — so re-capturing here would overwrite the true
            // baseline with the post-edit state and lose removals.
            if (State == EntityState.Unchanged && ManyToManySnapshots == null)
                CaptureManyToManySnapshots();
            // Same guard for owned collections: the owned-collection loader may have
            // already captured the loaded contents; this lazy init runs at DetectChanges
            // time (after any user edit), so re-capturing would lose the baseline.
            if (State == EntityState.Unchanged && OwnedCollectionSnapshots == null)
                CaptureOwnedCollectionSnapshots();

            if (Entity is INotifyPropertyChanged notify)
            {
                notify.PropertyChanged += PropertyChangedHandler;
            }
        }

        /// <summary>
        /// Handles <see cref="INotifyPropertyChanged.PropertyChanged"/> events raised
        /// by the tracked entity and updates the internal change tracking state.
        /// </summary>
        /// <param name="_">Unused sender parameter.</param>
        /// <param name="e">Event arguments describing the property change.</param>
        private void PropertyChangedHandler(object? _, PropertyChangedEventArgs e)
        {
            if (State is EntityState.Added or EntityState.Deleted or EntityState.Detached) return;
            if (!_isInitialized) return; // PERFORMANCE: Skip if not yet initialized

            var currentEntity = Entity;
            if (currentEntity is null) return;

            if (_propertyIndex == null) return;
            if (e.PropertyName != null && _propertyIndex.TryGetValue(e.PropertyName, out var idx))
            {
                var currentValue = _getValues![idx](currentEntity);
                var changed = !ValuesEqual(currentValue, _originalValues![idx]);
                _changedProperties![idx] = changed;
            }
            else
            {
                for (int i = 0; i < _nonKeyColumns!.Length; i++)
                {
                    var currentValue = _getValues![i](currentEntity);
                    var changed = !ValuesEqual(currentValue, _originalValues![i]);
                    _changedProperties![i] = changed;
                }
            }

            var hasAnyChanges = false;
            for (int i = 0; i < _nonKeyColumns!.Length; i++)
            {
                if (_changedProperties![i])
                {
                    hasAnyChanges = true;
                    break;
                }
            }

            _hasNotifiedChange = true;
            // State transition is not under a lock — this is by design because DbContext
            // is single-threaded. Callers must not raise PropertyChanged from other threads.
            _state = hasAnyChanges ? EntityState.Modified : EntityState.Unchanged;
            Tracker?.MarkDirty(this);
        }

        /// <summary>
        /// Ensures the entry has been fully initialized for change tracking. When a
        /// lazily-initialized entry first detects changes, this method populates the
        /// required caches and subscribes to change notifications.
        /// </summary>
        internal void UpgradeToFullTracking()
        {
            if (_isInitialized)
                return;
            InitializeTracking();
        }

        /// <summary>
        /// Captures the current property values and hash codes as the baseline for
        /// detecting future modifications.
        /// </summary>
        private void CaptureOriginalValues()
        {
            var entity = Entity;
            if (entity is null)
            {
                DetachEntity();
                return;
            }
            for (int i = 0; i < _nonKeyColumns!.Length; i++)
            {
                var val = _getValues![i](entity);
                _originalValues![i] = SnapshotValue(val);
                _originalHashes![i] = ContentHashCode(val);
                _changedProperties![i] = false;
            }
        }

        /// <summary>
        /// True when the given column's current entity value differs from the value
        /// captured when the entity was attached. Relationship fixup uses this to give a
        /// deliberately edited FK scalar precedence over a stale navigation reference.
        /// Key columns are not snapshot-tracked and always report unchanged.
        /// </summary>
        internal bool HasColumnValueChanged(Column column)
        {
            UpgradeToFullTracking();
            var entity = Entity;
            if (entity is null || _propertyIndex is null)
                return false;
            if (!_propertyIndex.TryGetValue(column.Prop.Name, out var idx))
                return false;
            return !ValuesEqual(_getValues![idx](entity), _originalValues![idx]);
        }

        /// <summary>
        /// Compares the current entity values against the original snapshot to update
        /// the <see cref="EntityState"/>. This method is used for entities that do not
        /// notify when properties change.
        /// </summary>
        internal void DetectChanges()
        {
            UpgradeToFullTracking();
            if (State is EntityState.Added or EntityState.Deleted or EntityState.Detached) return;
            if (_hasNotifiedChange) return;

            var entity = Entity;
            if (entity is null)
            {
                DetachEntity();
                return;
            }

            var hasChanges = false;
            for (int i = 0; i < _nonKeyColumns!.Length; i++)
            {
                bool changed;
                if (_options.UsePreciseChangeTracking)
                {
                    var currentValue = _getValues![i](entity);
                    changed = !ValuesEqual(currentValue, _originalValues![i]);
                }
                else
                {
                    // Hash-first detection strategy: compare hash codes first as a
                    // cheap O(1) divergence check. When hashes differ, the value has
                    // definitely changed (true positive). When hashes MATCH, we still
                    // fall through to a precise value comparison because GetHashCode
                    // collisions can produce false negatives (two different values
                    // with the same hash would be incorrectly treated as unchanged).
                    // This gives us the performance of hash comparison in the common
                    // "unchanged" case while guaranteeing correctness via the fallback.
                    var currentHash = _getHashCodes![i](entity);
                    if (currentHash != _originalHashes![i])
                    {
                        changed = true;
                    }
                    else
                    {
                        // Hash collision guard - verify using precise comparison
                        var currentValue = _getValues![i](entity);
                        changed = !ValuesEqual(currentValue, _originalValues![i]);
                    }
                }
                _changedProperties![i] = changed;
                hasChanges |= changed;
            }

            // A many-to-many collection edit is not a column change, so it must be
            // detected separately or the owner stays Unchanged and its join-table
            // sync never runs (silent association-write loss).
            hasChanges |= HasManyToManyChanges();

            // Owned-collection edits (add / remove / child-scalar change) are likewise
            // invisible to column-only detection; without this the owner stays Unchanged
            // and its owned-collection sync never runs (silent owned-write loss).
            hasChanges |= HasOwnedCollectionChanges();

            if (hasChanges)
                _state = EntityState.Modified;
            else if (_state == EntityState.Modified)
                _state = EntityState.Unchanged;
        }

        /// <summary>
        /// Accepts the current property values as the new original values and resets
        /// the change tracking state to <see cref="EntityState.Unchanged"/>.
        /// </summary>
        internal void AcceptChanges()
        {
            UpgradeToFullTracking();
            CaptureOriginalValues();
            CaptureManyToManySnapshots();
            CaptureOwnedCollectionSnapshots();
            // Reset then re-capture the loaded-reference-nav set so a cleared nav is
            // not carried across a save as still-loaded.
            LoadedReferenceNavs = null;
            CaptureReferenceNavSnapshots();
            // Refresh loaded collection-nav baselines so a severed/removed child is not
            // re-severed on the next save, and a newly added child becomes the baseline.
            RecaptureLoadedCollectionNavSnapshots();
            _state = EntityState.Unchanged;
            _hasNotifiedChange = false;
            // Refresh the original token so future saves use the latest DB value.
            var tsCol = _mapping.TimestampColumn;
            if (tsCol != null && Entity != null)
                OriginalToken = SnapshotValue(tsCol.Getter(Entity));
            // Refresh the tracked original key so that a DB-generated key assigned after INSERT
            // is accepted as the new "original" and does not trigger a false-positive
            // PK-mutation error on the next UPDATE.
            if (Entity != null)
                OriginalKey = CaptureKey(Entity, _mapping);
        }

        /// <summary>
        /// Detaches the entity from the context and removes any navigation property
        /// references that were established for change tracking or lazy loading.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("ReflectionAnalysis", "IL2026:RequiresUnreferencedCode",
            Justification = "CleanupNavigationContext only accesses a ConditionalWeakTable and disposes a context — no reflection paths are exercised.")]
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("Aot", "IL3050:RequiresDynamicCode",
            Justification = "CleanupNavigationContext only accesses a ConditionalWeakTable and disposes a context — no dynamic code paths are exercised.")]
        internal void DetachEntity()
        {
            _state = EntityState.Detached;
            var entity = Entity;
            if (entity is INotifyPropertyChanged notify)
                notify.PropertyChanged -= PropertyChangedHandler;
            if (entity != null)
                NavigationPropertyExtensions.CleanupNavigationContext(entity);
            Entity = null;

            // Reset tracking state so a detached entry cannot leak stale snapshots.
            _isInitialized = false;
            _nonKeyColumns = null;
            _originalHashes = null;
            _originalValues = null;
            _changedProperties = null;
            _getHashCodes = null;
            _getValues = null;
            _propertyIndex = null;
            _hasNotifiedChange = false;
            ManyToManySnapshots = null;
            CollectionNavSnapshots = null;
        }
    }
}
