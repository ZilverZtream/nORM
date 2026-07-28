using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using nORM.Configuration;
using nORM.Enterprise;
using nORM.Execution;
using nORM.Internal;
using nORM.Mapping;
using nORM.Navigation;
using nORM.Providers;
using nORM.Query;
using nORM.Scaffolding;
using nORM.Versioning;
#nullable enable
namespace nORM.Core
{
    public partial class DbContext
    {
        #region Transaction Savepoints
        /// <summary>
        /// Creates a savepoint within the provided transaction. Savepoints allow portions of a
        /// transaction to be rolled back without affecting the entire transaction scope.
        /// </summary>
        /// <param name="transaction">The active database transaction.</param>
        /// <param name="name">Name of the savepoint to create.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task that completes when the savepoint has been created.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <paramref name="transaction"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or empty.</exception>
        public Task CreateSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(CreateSavepointAsync));
            return CreateSavepointCoreAsync(transaction, name, ct);
        }

        // Per-savepoint snapshot of the DB-generated key values of Added entities at the moment the
        // savepoint was created, keyed by entity reference. Used to reset keys stamped by inserts that
        // happen after the savepoint so a rollback-to-savepoint leaves those entities re-insertable.
        private Dictionary<string, Dictionary<object, object?[]>>? _savepointKeySnapshots;

        // Snapshot of Added-entity DB-generated keys captured when a caller-owned transaction begins,
        // used to reset keys stamped during the transaction if it is fully rolled back (the same
        // silent-drop class as the savepoint fix, via a different rollback path).
        private Dictionary<object, object?[]>? _transactionKeySnapshot;

        // Client-key counterpart of the key snapshots: the set of entities whose INSERT had already run
        // (EntityEntry.InsertedInUncommittedTransaction == true) at snapshot time. Restored alongside the
        // keys so a rollback leaves entities inserted AFTER the snapshot re-insertable and those inserted
        // before it untouched — a client-assigned key carries no key value to signal this on its own.
        private HashSet<object>? _transactionInsertedSnapshot;
        private Dictionary<string, HashSet<object>>? _savepointInsertedSnapshots;
        private HashSet<object>? _ambientInsertedSnapshot;

        // Delete/update counterparts of the inserted snapshots: the set of entities whose DELETE / UPDATE had
        // already run in the current uncommitted transaction (the DeletedInUncommittedTransaction /
        // ModifiedInUncommittedTransaction flags) at snapshot time. Restored alongside the inserted flags at
        // every rollback site so a rolled-back delete stays Deleted (re-deletable) and a rolled-back update stays
        // Modified (its pending edit re-applies), instead of being wrongly reconciled at the eventual commit.
        private HashSet<object>? _transactionDeletedSnapshot;
        private Dictionary<string, HashSet<object>>? _savepointDeletedSnapshots;
        private HashSet<object>? _ambientDeletedSnapshot;
        private HashSet<object>? _transactionModifiedSnapshot;
        private Dictionary<string, HashSet<object>>? _savepointModifiedSnapshots;
        private HashSet<object>? _ambientModifiedSnapshot;

        // Snapshot of each tracked OCC entity's original concurrency token when a caller-owned transaction
        // begins. A save inside the transaction advances the token snapshot (so a second update matches the
        // uncommitted row), so a full rollback must restore the pre-transaction token — otherwise re-updating
        // the same tracked entity after the rollback compares an advanced token against the reverted row and
        // false-conflicts.
        private Dictionary<object, object?>? _transactionTokenSnapshot;

        // Pre-transaction change-tracking baseline (original non-key values) of each Modified entity that a
        // caller-owned-transaction save advances, captured lazily on the FIRST such advance. A full rollback
        // restores it so a still-pending edit re-applies on the next save instead of being lost — the advanced
        // baseline would otherwise equal the reverted-but-still-edited entity and read as "no change".
        private Dictionary<object, object?[]>? _transactionValuesSnapshot;

        // Per-savepoint original-value baselines, captured at CreateSavepoint and restored at
        // RollbackToSavepoint — the analogue of _transactionValuesSnapshot for the full-rollback path. Without
        // this, a Modified entity whose baseline advanced during a save BETWEEN the savepoint and the rollback
        // keeps the advanced baseline, so the re-save after RollbackToSavepoint compares the current value against
        // that advanced baseline, finds no diff, emits no UPDATE, and silently drops the pending update.
        private Dictionary<string, Dictionary<object, object?[]>>? _savepointValuesSnapshots;
        // Per-savepoint OCC ([Timestamp]) original-token snapshots — the analogue of _transactionTokenSnapshot.
        // Without restoring these, an OCC entity updated in-tx then rolled back to a savepoint keeps its advanced
        // OriginalToken while the DB row's token reverts, so the next write matches 0 rows and throws a spurious
        // DbConcurrencyException (a false conflict).
        private Dictionary<string, Dictionary<object, object?>>? _savepointTokenSnapshots;

        // Pre-advance m2m / owned-collection snapshot baselines, captured lazily the FIRST time a caller-owned
        // save advances them (the collection twin of _transactionValuesSnapshot). A full rollback / ambient abort
        // restores them so a still-pending collection edit re-applies on the next save instead of being silently
        // dropped — the advanced snapshot would otherwise equal the reverted-but-still-edited collection and read
        // as "no change".
        private Dictionary<object, CollectionSnapshotBaseline>? _transactionCollectionSnapshot;
        // Per-savepoint m2m/owned snapshot baselines — the analogue for RollbackToSavepoint.
        private Dictionary<string, Dictionary<object, CollectionSnapshotBaseline>>? _savepointCollectionSnapshots;

        private Dictionary<object, CollectionSnapshotBaseline> SnapshotAllTrackedCollectionBaselines()
        {
            var dict = new Dictionary<object, CollectionSnapshotBaseline>(ReferenceEqualityComparer.Instance);
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.Entity is { } e
                    && (entry.Mapping.ManyToManyJoins.Count > 0 || entry.Mapping.OwnedCollections.Count > 0))
                    dict[e] = entry.CaptureCollectionSnapshotBaseline();
            }
            return dict;
        }

        /// <summary>
        /// Records the pre-advance m2m/owned snapshot baseline of an entity the first time a caller-owned save
        /// advances it, so a full rollback can restore it. Idempotent per entity; only for entities that have
        /// m2m or owned-collection navigations.
        /// </summary>
        internal void RememberPreTransactionCollectionBaseline(object entity, EntityEntry entry)
        {
            if (entry.Mapping.ManyToManyJoins.Count == 0 && entry.Mapping.OwnedCollections.Count == 0)
                return;
            _transactionCollectionSnapshot ??= new Dictionary<object, CollectionSnapshotBaseline>(ReferenceEqualityComparer.Instance);
            if (_transactionCollectionSnapshot.ContainsKey(entity))
                return;
            _transactionCollectionSnapshot[entity] = entry.CaptureCollectionSnapshotBaseline();
        }

        private Dictionary<object, object?[]> SnapshotAllTrackedOriginalValues()
        {
            var dict = new Dictionary<object, object?[]>(ReferenceEqualityComparer.Instance);
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.Entity is { } e)
                {
                    var snap = entry.SnapshotOriginalValues();
                    if (snap != null)
                        dict[e] = snap;
                }
            }
            return dict;
        }

        private void RestoreSavepointOriginalValues(Dictionary<object, object?[]> snapshot)
        {
            foreach (var (entity, values) in snapshot)
            {
                var entry = ChangeTracker.GetEntryOrDefault(entity);
                if (entry != null && ReferenceEquals(entry.Entity, entity))
                    entry.RestoreOriginalValues(values);
            }
        }

        /// <summary>
        /// Records the pre-advance baseline of a Modified entity the first time a caller-owned-transaction save
        /// advances it, so a full rollback can restore it. Idempotent per entity within a transaction.
        /// </summary>
        internal void RememberPreTransactionValuesBaseline(object entity, EntityEntry entry)
        {
            _transactionValuesSnapshot ??= new Dictionary<object, object?[]>(ReferenceEqualityComparer.Instance);
            if (_transactionValuesSnapshot.ContainsKey(entity))
                return;
            var snap = entry.SnapshotOriginalValues();
            if (snap != null)
                _transactionValuesSnapshot[entity] = snap;
        }

        internal void CaptureTransactionKeySnapshot()
        {
            _transactionKeySnapshot = SnapshotAddedGeneratedKeys();
            _transactionInsertedSnapshot = SnapshotInsertedEntities();
            _transactionDeletedSnapshot = SnapshotDeletedEntities();
            _transactionModifiedSnapshot = SnapshotModifiedEntities();
            _transactionTokenSnapshot = SnapshotOccOriginalTokens();
        }

        /// <summary>
        /// Records an OCC entity's pre-advance concurrency token the FIRST time a save inside a
        /// caller-owned/enlisted transaction advances it, so a full rollback (or ambient abort) restores it.
        /// The begin-time snapshot (<see cref="SnapshotOccOriginalTokens"/>) only covers entities already
        /// tracked when the transaction began; an OCC entity LOADED or attached AFTER begin would otherwise
        /// keep its advanced token past the rollback and false-conflict on the next write of the reverted row.
        /// Only augments an already-armed snapshot (a non-nORM-managed external transaction has no restore
        /// hook); idempotent per entity so an earlier advance's baseline is never overwritten by a later one.
        /// </summary>
        internal void RememberPreTransactionTokenBaseline(object entity, EntityEntry entry)
        {
            if (_transactionTokenSnapshot == null || entry.Mapping.TimestampColumn == null)
                return;
            if (_transactionTokenSnapshot.ContainsKey(entity))
                return;
            var tok = entry.OriginalToken;
            _transactionTokenSnapshot[entity] = tok is byte[] bytes ? bytes.Clone() : tok;
        }

        /// <summary>
        /// After a caller-owned transaction commits DURABLY, reconcile the ChangeTracker with the now-durable
        /// writes — matching EF Core, where a committed change is fully accepted. Only entities PROVEN flushed in
        /// this transaction (the per-state <c>*InUncommittedTransaction</c> flags) are reconciled:
        /// <list type="bullet">
        /// <item>Inserted (<see cref="EntityState.Added"/>) and Modified entities become
        /// <see cref="EntityState.Unchanged"/> — so a later update emits a normal UPDATE and a
        /// <c>detectChanges:false</c> save does not re-issue a stale UPDATE. A still-dirty entity (edited again
        /// after its flush) is left as-is because AcceptChanges would capture the unsaved edit as the committed
        /// baseline (silent loss); its pending edit re-applies on the next DetectChanges.</item>
        /// <item>Deleted entities are detached and removed from tracked navigations — otherwise the next
        /// SaveChanges silently re-issues the committed DELETE (dropping a row re-created at the same key by any
        /// other path) or, for an OCC entity, throws a spurious concurrency conflict that poisons the context.</item>
        /// </list>
        /// A pending change not yet flushed carries no flag and is untouched. Called only on the success path of
        /// commit, so a rollback never accepts.
        /// </summary>
        internal void AcceptSavedChangesAfterCommit()
        {
            List<EntityEntry>? toAccept = null;
            List<object>? deletedInstances = null;
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.Entity is not { } entity)
                    continue;
                if (entry.State == EntityState.Added
                    && entry.InsertedInUncommittedTransaction
                    && !entry.HasChangedSinceInsertedBaseline())
                {
                    (toAccept ??= new List<EntityEntry>()).Add(entry);
                }
                else if (entry.State == EntityState.Modified
                    && entry.ModifiedInUncommittedTransaction
                    && !entry.HasChangedSinceInsertedBaseline())
                {
                    (toAccept ??= new List<EntityEntry>()).Add(entry);
                }
                else if (entry.State == EntityState.Deleted
                    && entry.DeletedInUncommittedTransaction)
                {
                    (deletedInstances ??= new List<object>()).Add(entity);
                }
            }
            // Act AFTER enumeration completes: ChangeTracker.Remove mutates the entry dictionary that
            // ChangeTracker.Entries lazily enumerates, so removing mid-iteration would throw.
            if (toAccept != null)
                foreach (var entry in toAccept)
                    entry.AcceptChanges();
            if (deletedInstances != null)
            {
                foreach (var entity in deletedInstances)
                    ChangeTracker.Remove(entity, true);
                RemoveDeletedInstancesFromTrackedNavigations(deletedInstances);
            }
        }

        /// <summary>
        /// Resets, after a full transaction rollback, the DB-generated keys stamped during the
        /// transaction so the still-Added entities are re-inserted on the next SaveChanges instead of
        /// being silently dropped by the "skip already-inserted" guard. Invoked by
        /// <see cref="DbContextTransaction"/> before the transaction is cleared.
        /// </summary>
        internal void ResetGeneratedKeysAfterFullRollback()
        {
            if (_transactionKeySnapshot != null)
                RestoreRolledBackGeneratedKeys(_transactionKeySnapshot);
            RestoreInsertedFlags(_transactionInsertedSnapshot);
            RestoreDeletedFlags(_transactionDeletedSnapshot);
            RestoreModifiedFlags(_transactionModifiedSnapshot);
            RestoreTransactionTokenAndValueBaselines();
        }

        /// <summary>
        /// Restores the OCC token snapshot and change-tracking value baselines advanced by saves under a
        /// caller-owned or enlisted scope, so a rollback leaves a pending edit re-applicable and an OCC entity
        /// re-updatable instead of false-conflicting. Shared by the explicit full-rollback path and the ambient
        /// scope-abort path; a no-op when neither snapshot was captured. Clears the values snapshot so it cannot
        /// leak into a later scope's rollback.
        /// </summary>
        private void RestoreTransactionTokenAndValueBaselines()
        {
            if (_transactionTokenSnapshot != null)
                RestoreOccOriginalTokens(_transactionTokenSnapshot);
            if (_transactionValuesSnapshot != null)
            {
                foreach (var (entity, values) in _transactionValuesSnapshot)
                {
                    var entry = ChangeTracker.GetEntryOrDefault(entity);
                    if (entry != null && ReferenceEquals(entry.Entity, entity))
                        entry.RestoreOriginalValues(values);
                }
                _transactionValuesSnapshot.Clear();
            }
            if (_transactionCollectionSnapshot != null)
            {
                foreach (var (entity, baseline) in _transactionCollectionSnapshot)
                {
                    var entry = ChangeTracker.GetEntryOrDefault(entity);
                    if (entry != null && ReferenceEquals(entry.Entity, entity))
                        entry.RestoreCollectionSnapshotBaseline(baseline);
                }
                _transactionCollectionSnapshot.Clear();
            }
        }

        // The ambient System.Transactions.Transaction nORM enlisted in, plus the Added-entity key
        // snapshot taken at enlistment. When the scope aborts (disposed without Complete) the DB rolls
        // back OUTSIDE any DbContextTransaction, so the completion event is the only reset hook.
        private System.Transactions.Transaction? _registeredAmbientTransaction;
        private Dictionary<object, object?[]>? _ambientKeySnapshot;

        /// <summary>
        /// Registers a reset for a successfully-enlisted ambient <see cref="System.Transactions.Transaction"/>
        /// so that, if the scope is disposed without Complete(), the DB-generated keys stamped while the
        /// scope was active are reset (the entities stay Added because durability is the scope's; without
        /// this the next SaveChanges silently drops them). Snapshots the current Added keys once per scope.
        /// </summary>
        internal void RegisterAmbientRollbackReset(System.Transactions.Transaction ambient)
        {
            if (ReferenceEquals(_registeredAmbientTransaction, ambient))
                return; // already registered for this scope (a later SaveChanges within it)
            _registeredAmbientTransaction = ambient;
            _ambientKeySnapshot = SnapshotAddedGeneratedKeys();
            _ambientInsertedSnapshot = SnapshotInsertedEntities();
            _ambientDeletedSnapshot = SnapshotDeletedEntities();
            _ambientModifiedSnapshot = SnapshotModifiedEntities();
            // Capture the pre-scope OCC tokens here (the caller-owned path captures them in
            // SetCurrentTransaction, which the ambient path never calls). A save under the scope advances
            // both these tokens and the Modified-entity value baselines (the latter into the shared, lazily
            // populated _transactionValuesSnapshot), so an abort must restore both — otherwise a re-updated
            // OCC entity false-conflicts and a pending edit is silently dropped after the scope reverts.
            _transactionTokenSnapshot = SnapshotOccOriginalTokens();
            ambient.TransactionCompleted += OnAmbientTransactionCompleted;
        }

        private void OnAmbientTransactionCompleted(object? sender, System.Transactions.TransactionEventArgs e)
        {
            try
            {
                if (e.Transaction?.TransactionInformation.Status == System.Transactions.TransactionStatus.Aborted)
                {
                    if (_ambientKeySnapshot != null)
                        RestoreRolledBackGeneratedKeys(_ambientKeySnapshot);
                    RestoreInsertedFlags(_ambientInsertedSnapshot);
                    RestoreDeletedFlags(_ambientDeletedSnapshot);
                    RestoreModifiedFlags(_ambientModifiedSnapshot);
                    RestoreTransactionTokenAndValueBaselines();
                }
            }
            finally
            {
                if (ReferenceEquals(sender as System.Transactions.Transaction, _registeredAmbientTransaction))
                {
                    _registeredAmbientTransaction = null;
                    _ambientKeySnapshot = null;
                    _ambientInsertedSnapshot = null;
                    _ambientDeletedSnapshot = null;
                    _ambientModifiedSnapshot = null;
                    // Clear on BOTH commit and abort so a committed scope's advanced baselines cannot leak
                    // into a later caller-owned transaction's full-rollback restore. (On abort they were
                    // already restored above; RestoreTransactionTokenAndValueBaselines cleared the values map.)
                    _transactionTokenSnapshot = null;
                    _transactionValuesSnapshot = null;
                }
            }
        }

        internal async Task CreateSavepointCoreAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction == null)
                throw new NormUsageException("No active transaction.");
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Savepoint name cannot be null or empty.", nameof(name));
            await _p.CreateSavepointAsync(transaction, name, ct).ConfigureAwait(false);
            // Snapshot AFTER the savepoint exists so a later rollback to it can restore the exact
            // in-memory key state. Overwrites any prior snapshot for the same name (matching the SQL
            // semantics of re-declaring a savepoint).
            (_savepointKeySnapshots ??= new Dictionary<string, Dictionary<object, object?[]>>(StringComparer.Ordinal))[name]
                = SnapshotAddedGeneratedKeys();
            (_savepointInsertedSnapshots ??= new Dictionary<string, HashSet<object>>(StringComparer.Ordinal))[name]
                = SnapshotInsertedEntities();
            (_savepointDeletedSnapshots ??= new Dictionary<string, HashSet<object>>(StringComparer.Ordinal))[name]
                = SnapshotDeletedEntities();
            (_savepointModifiedSnapshots ??= new Dictionary<string, HashSet<object>>(StringComparer.Ordinal))[name]
                = SnapshotModifiedEntities();
            (_savepointValuesSnapshots ??= new Dictionary<string, Dictionary<object, object?[]>>(StringComparer.Ordinal))[name]
                = SnapshotAllTrackedOriginalValues();
            (_savepointTokenSnapshots ??= new Dictionary<string, Dictionary<object, object?>>(StringComparer.Ordinal))[name]
                = SnapshotOccOriginalTokens();
            (_savepointCollectionSnapshots ??= new Dictionary<string, Dictionary<object, CollectionSnapshotBaseline>>(StringComparer.Ordinal))[name]
                = SnapshotAllTrackedCollectionBaselines();
        }

        /// <summary>
        /// Rolls back the specified transaction to a previously created savepoint.
        /// </summary>
        /// <param name="transaction">The active database transaction.</param>
        /// <param name="name">Name of the savepoint to roll back to.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task that completes when the transaction has been rolled back to the savepoint.</returns>
        /// <exception cref="InvalidOperationException">Thrown when <paramref name="transaction"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or empty.</exception>
        public Task RollbackToSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(RollbackToSavepointAsync));
            return RollbackToSavepointCoreAsync(transaction, name, ct);
        }

        internal async Task RollbackToSavepointCoreAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction == null)
                throw new NormUsageException("No active transaction.");
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Savepoint name cannot be null or empty.", nameof(name));
            await _p.RollbackToSavepointAsync(transaction, name, ct).ConfigureAwait(false);
            // The rollback discarded every row inserted since the savepoint. Reset the DB-generated
            // keys those inserts stamped so the entities (still Added, because a caller-owned
            // transaction skips AcceptChanges) are re-inserted on the next SaveChanges instead of being
            // silently dropped by the "skip already-inserted" guard.
            if (_savepointKeySnapshots != null && _savepointKeySnapshots.TryGetValue(name, out var snapshot))
                RestoreRolledBackGeneratedKeys(snapshot);
            // Mirror the key reset for client-assigned keys: an entity inserted after the savepoint has
            // lost its row, so clearing its flag makes the next SaveChanges re-insert it rather than
            // silently skip it; one inserted before the savepoint keeps its row and its flag.
            if (_savepointInsertedSnapshots != null && _savepointInsertedSnapshots.TryGetValue(name, out var insSnapshot))
                RestoreInsertedFlags(insSnapshot);
            // Same reconciliation-flag restore for deletes and updates flushed AFTER the savepoint: their
            // DELETE/UPDATE was undone, so the flag clears and the entity stays Deleted / Modified (its pending
            // change re-applies) rather than being wrongly detached / accepted at the eventual commit.
            if (_savepointDeletedSnapshots != null && _savepointDeletedSnapshots.TryGetValue(name, out var delSnapshot))
                RestoreDeletedFlags(delSnapshot);
            if (_savepointModifiedSnapshots != null && _savepointModifiedSnapshots.TryGetValue(name, out var modSnapshot))
                RestoreModifiedFlags(modSnapshot);
            // Restore the change-tracking baselines captured at the savepoint so a Modified entity whose baseline
            // advanced during a save AFTER the savepoint is re-detected and re-applied on the next SaveChanges,
            // rather than silently dropped (its current value would otherwise equal the advanced baseline).
            // Mirrors the full-rollback baseline restore in ResetGeneratedKeysAfterFullRollback.
            if (_savepointValuesSnapshots != null && _savepointValuesSnapshots.TryGetValue(name, out var valSnapshot))
                RestoreSavepointOriginalValues(valSnapshot);
            // Likewise restore the OCC tokens so a rolled-back [Timestamp] entity does not throw a spurious
            // concurrency conflict on its next write.
            if (_savepointTokenSnapshots != null && _savepointTokenSnapshots.TryGetValue(name, out var tokSnapshot))
                RestoreOccOriginalTokens(tokSnapshot);
            // Restore the m2m/owned snapshot baselines captured at the savepoint so a collection edit saved AFTER
            // the savepoint (whose snapshot was advanced by the deferred-accept branch) is re-detected and
            // re-applied on the next SaveChanges rather than silently dropped. Mirrors the full-rollback restore.
            if (_savepointCollectionSnapshots != null && _savepointCollectionSnapshots.TryGetValue(name, out var colSnapshot))
            {
                foreach (var (entity, baseline) in colSnapshot)
                {
                    var entry = ChangeTracker.GetEntryOrDefault(entity);
                    if (entry != null && ReferenceEquals(entry.Entity, entity))
                        entry.RestoreCollectionSnapshotBaseline(baseline);
                }
            }
        }

        /// <summary>
        /// Releases a previously created savepoint within the provided transaction. Unlike a rollback, the work
        /// done since the savepoint is KEPT — the savepoint simply stops being a rollback target. Savepoints
        /// are released automatically when the transaction commits, so an explicit release is only needed to
        /// free the name (or resources) earlier.
        /// </summary>
        /// <param name="transaction">The active database transaction.</param>
        /// <param name="name">Name of the savepoint to release.</param>
        /// <param name="ct">Token used to cancel the asynchronous operation.</param>
        /// <returns>A task that completes when the savepoint has been released.</returns>
        /// <exception cref="NormUsageException">Thrown when <paramref name="transaction"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="name"/> is null or empty.</exception>
        public Task ReleaseSavepointAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            ThrowIfDisposed();
            ThrowIfStrictProviderMobilityEscapeHatch(nameof(ReleaseSavepointAsync));
            return ReleaseSavepointCoreAsync(transaction, name, ct);
        }

        internal async Task ReleaseSavepointCoreAsync(DbTransaction transaction, string name, CancellationToken ct = default)
        {
            if (transaction == null)
                throw new NormUsageException("No active transaction.");
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Savepoint name cannot be null or empty.", nameof(name));
            await _p.ReleaseSavepointAsync(transaction, name, ct).ConfigureAwait(false);
            // The released savepoint is no longer a rollback target, so its key snapshot is obsolete; the rows
            // inserted since it are KEPT (unlike a rollback), so their stamped keys stay valid as-is.
            _savepointKeySnapshots?.Remove(name);
            _savepointInsertedSnapshots?.Remove(name);
            _savepointDeletedSnapshots?.Remove(name);
            _savepointModifiedSnapshots?.Remove(name);
        }

        /// <summary>
        /// Captures the current DB-generated key values of every Added entity, keyed by entity
        /// reference, so a subsequent rollback can tell which keys were stamped afterwards.
        /// </summary>
        /// <summary>
        /// Captures the set of Added entities whose INSERT has already run in the current uncommitted
        /// transaction (<see cref="EntityEntry.InsertedInUncommittedTransaction"/>), keyed by reference, so a
        /// later rollback can tell which entities were inserted before the snapshot from those inserted after.
        /// The client-key counterpart of <see cref="SnapshotAddedGeneratedKeys"/>.
        /// </summary>
        private HashSet<object> SnapshotInsertedEntities()
        {
            var snapshot = new HashSet<object>(ReferenceEqualityComparer.Instance);
            foreach (var entry in ChangeTracker.Entries)
                if (entry.State == EntityState.Added && entry.InsertedInUncommittedTransaction && entry.Entity is { } e)
                    snapshot.Add(e);
            return snapshot;
        }

        /// <summary>
        /// After a rollback that undid inserts, restores the "already inserted" flag on Added entities to the
        /// supplied snapshot: an entity present in it kept its row through the rollback and stays flagged
        /// (skipped by the next save); one absent from it was inserted afterwards, its row is gone, so the
        /// flag is cleared and the next SaveChanges re-inserts it instead of silently dropping it. A null
        /// snapshot (a rollback with no captured state) clears every flag — a full rollback discards all
        /// uncommitted inserts, so every one becomes re-insertable.
        /// </summary>
        private void RestoreInsertedFlags(HashSet<object>? snapshot)
        {
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.State != EntityState.Added || entry.Entity is not { } e)
                    continue;
                entry.InsertedInUncommittedTransaction = snapshot != null && snapshot.Contains(e);
            }
        }

        /// <summary>
        /// Captures the set of entities whose DELETE has already run in the current uncommitted transaction
        /// (<see cref="EntityEntry.DeletedInUncommittedTransaction"/>), keyed by reference — the delete-state
        /// counterpart of <see cref="SnapshotInsertedEntities"/>.
        /// </summary>
        private HashSet<object> SnapshotDeletedEntities()
        {
            var snapshot = new HashSet<object>(ReferenceEqualityComparer.Instance);
            foreach (var entry in ChangeTracker.Entries)
                if (entry.State == EntityState.Deleted && entry.DeletedInUncommittedTransaction && entry.Entity is { } e)
                    snapshot.Add(e);
            return snapshot;
        }

        /// <summary>
        /// After a rollback that undid deletes, restores the "already deleted" flag on Deleted entities to the
        /// supplied snapshot: one present in it kept its committed delete and stays flagged (detached at commit);
        /// one absent had its DELETE undone, so the flag clears and it stays Deleted / re-deletable. A null
        /// snapshot clears every flag (a full rollback undid all uncommitted deletes). Mirrors
        /// <see cref="RestoreInsertedFlags"/>.
        /// </summary>
        private void RestoreDeletedFlags(HashSet<object>? snapshot)
        {
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.State != EntityState.Deleted || entry.Entity is not { } e)
                    continue;
                entry.DeletedInUncommittedTransaction = snapshot != null && snapshot.Contains(e);
            }
        }

        /// <summary>
        /// Captures the set of entities whose UPDATE has already run in the current uncommitted transaction
        /// (<see cref="EntityEntry.ModifiedInUncommittedTransaction"/>), keyed by reference — the modified-state
        /// counterpart of <see cref="SnapshotInsertedEntities"/>.
        /// </summary>
        private HashSet<object> SnapshotModifiedEntities()
        {
            var snapshot = new HashSet<object>(ReferenceEqualityComparer.Instance);
            foreach (var entry in ChangeTracker.Entries)
                if (entry.State == EntityState.Modified && entry.ModifiedInUncommittedTransaction && entry.Entity is { } e)
                    snapshot.Add(e);
            return snapshot;
        }

        /// <summary>
        /// After a rollback that undid updates, restores the "already updated" flag on Modified entities to the
        /// supplied snapshot: one present in it kept its committed update and stays flagged (accepted at commit);
        /// one absent had its UPDATE undone, so the flag clears and it stays Modified so its pending edit
        /// re-applies. A null snapshot clears every flag. Mirrors <see cref="RestoreInsertedFlags"/>.
        /// </summary>
        private void RestoreModifiedFlags(HashSet<object>? snapshot)
        {
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.State != EntityState.Modified || entry.Entity is not { } e)
                    continue;
                entry.ModifiedInUncommittedTransaction = snapshot != null && snapshot.Contains(e);
            }
        }

        /// <summary>
        /// Captures the original concurrency token of every already-persisted tracked OCC entity (a
        /// [Timestamp]/rowversion column, entities not in the <see cref="EntityState.Added"/> state), keyed by
        /// reference. A save inside the transaction advances this snapshot (<c>ExecuteUpdateBatch</c>), so a
        /// full rollback restores it, keeping a re-update of the same tracked entity after the rollback
        /// comparing against the token the reverted row actually carries.
        /// </summary>
        private Dictionary<object, object?> SnapshotOccOriginalTokens()
        {
            var snapshot = new Dictionary<object, object?>(ReferenceEqualityComparer.Instance);
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.State == EntityState.Added || entry.Entity is not { } e || entry.Mapping.TimestampColumn == null)
                    continue;
                var tok = entry.OriginalToken;
                snapshot[e] = tok is byte[] bytes ? bytes.Clone() : tok;
            }
            return snapshot;
        }

        /// <summary>
        /// After a full rollback, restores each tracked OCC entity's original-token snapshot (and the entity's
        /// own token value, so it matches the reverted row) to the value captured before the transaction, undoing
        /// any advance a save inside the transaction made. Mirrors <see cref="RestoreRolledBackGeneratedKeys"/>.
        /// </summary>
        private void RestoreOccOriginalTokens(Dictionary<object, object?> snapshot)
        {
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.Entity is not { } e || entry.Mapping.TimestampColumn == null)
                    continue;
                if (!snapshot.TryGetValue(e, out var original))
                    continue;
                var restored = original is byte[] bytes ? bytes.Clone() : original;
                entry.OriginalToken = restored;
                entry.Mapping.TimestampColumn.Setter(e, restored);
            }
        }

        private Dictionary<object, object?[]> SnapshotAddedGeneratedKeys()
        {
            var snapshot = new Dictionary<object, object?[]>(ReferenceEqualityComparer.Instance);
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.State == EntityState.Added && entry.Entity is { } e && HasDbGeneratedKey(entry.Mapping.KeyColumns))
                {
                    var m = entry.Mapping;
                    var keys = new object?[m.KeyColumns.Length];
                    for (int i = 0; i < m.KeyColumns.Length; i++)
                        keys[i] = m.KeyColumns[i].Getter(e);
                    snapshot[e] = keys;
                }
            }
            return snapshot;
        }

        /// <summary>
        /// After a rollback that undid inserts, resets the DB-generated keys those inserts stamped:
        /// entities present at snapshot time are restored to their snapshot key; entities added after
        /// the snapshot have their generated key columns reset to default (non-generated composite-key
        /// parts are preserved). Uses <see cref="ChangeTracker.RollbackGeneratedKeyAssignment"/> so the
        /// stale key-based identity-map index is dropped too.
        /// </summary>
        private void RestoreRolledBackGeneratedKeys(Dictionary<object, object?[]> snapshot)
        {
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.State != EntityState.Added || entry.Entity is not { } e)
                    continue;
                var m = entry.Mapping;
                if (!HasDbGeneratedKey(m.KeyColumns))
                    continue;

                if (snapshot.TryGetValue(e, out var snapKeys))
                {
                    var changed = false;
                    for (int i = 0; i < m.KeyColumns.Length; i++)
                    {
                        if (!Equals(m.KeyColumns[i].Getter(e), snapKeys[i]))
                        {
                            changed = true;
                            break;
                        }
                    }
                    if (changed)
                        ChangeTracker.RollbackGeneratedKeyAssignment(e, m, snapKeys);
                }
                else if (!IsDefaultDbGeneratedKey(e, m))
                {
                    ChangeTracker.RollbackGeneratedKeyAssignment(e, m, DefaultDbGeneratedKeyValues(m, e));
                }
            }
        }

        private static object?[] DefaultDbGeneratedKeyValues(TableMapping map, object entity)
        {
            var vals = new object?[map.KeyColumns.Length];
            for (int i = 0; i < map.KeyColumns.Length; i++)
            {
                var col = map.KeyColumns[i];
                if (!col.IsDbGenerated)
                {
                    vals[i] = col.Getter(entity); // preserve a user-set non-generated composite-key part
                    continue;
                }
                var underlying = Nullable.GetUnderlyingType(col.Prop.PropertyType) ?? col.Prop.PropertyType;
                vals[i] = underlying == typeof(Guid)
                    ? Guid.Empty
                    : Type.GetTypeCode(underlying) switch
                    {
                        TypeCode.Int32 => 0,
                        TypeCode.Int64 => 0L,
                        TypeCode.Int16 => (short)0,
                        TypeCode.Byte => (byte)0,
                        TypeCode.SByte => (sbyte)0,
                        TypeCode.UInt16 => (ushort)0,
                        TypeCode.UInt32 => 0u,
                        TypeCode.UInt64 => 0ul,
                        _ => null
                    };
            }
            return vals;
        }
        #endregion
    }
}
