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
            _transactionTokenSnapshot = SnapshotOccOriginalTokens();
        }

        /// <summary>
        /// After a caller-owned transaction commits DURABLY, accept the entities it inserted and left in the
        /// <see cref="EntityState.Added"/> state so they become <see cref="EntityState.Unchanged"/> — matching
        /// EF Core, where an entity is updatable once its transaction commits. Without this an entity inserted
        /// inside a caller transaction stays Added after commit, so a later update of it cannot emit an UPDATE
        /// (the modify-after-insert guard rejects it). Only entities PROVEN saved in this transaction (the
        /// <see cref="EntityEntry.InsertedInUncommittedTransaction"/> flag) AND unmodified since their insert are
        /// accepted: a still-dirty inserted entity is left Added because AcceptChanges would capture its unsaved
        /// edit as the baseline over the committed row (silent loss), and a pending never-saved entity carries no
        /// flag and is untouched. Modified and Deleted entities keep their existing post-commit handling. Called
        /// only on the success path of commit, so a rollback never accepts.
        /// </summary>
        internal void AcceptSavedInsertsAfterCommit()
        {
            foreach (var entry in ChangeTracker.Entries)
            {
                if (entry.State == EntityState.Added
                    && entry.InsertedInUncommittedTransaction
                    && entry.Entity is not null
                    && !entry.HasChangedSinceInsertedBaseline())
                {
                    entry.AcceptChanges();
                }
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
                }
            }
            finally
            {
                if (ReferenceEquals(sender as System.Transactions.Transaction, _registeredAmbientTransaction))
                {
                    _registeredAmbientTransaction = null;
                    _ambientKeySnapshot = null;
                    _ambientInsertedSnapshot = null;
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
