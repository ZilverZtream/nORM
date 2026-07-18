using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        private static readonly MethodInfo s_reloadCoreAsyncMethod =
            typeof(DbContext).GetMethod(nameof(ReloadEntryCoreAsync), BindingFlags.Instance | BindingFlags.NonPublic)!;
        private static readonly MethodInfo s_reloadCoreSyncMethod =
            typeof(DbContext).GetMethod(nameof(ReloadEntryCore), BindingFlags.Instance | BindingFlags.NonPublic)!;

        /// <summary>
        /// Reloads a tracked entry from its current database row (see <see cref="EntityEntry.ReloadAsync"/>).
        /// Dispatches to the strongly-typed core so the no-tracking key query reuses the standard
        /// materializer (converters, provider coercion) before the fresh values are copied onto the
        /// tracked instance.
        /// </summary>
        [RequiresDynamicCode("Reload builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reload reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        internal Task<bool> ReloadEntryAsync(EntityEntry entry, CancellationToken ct)
        {
            var mapping = ValidateReloadable(entry);
            return (Task<bool>)s_reloadCoreAsyncMethod.MakeGenericMethod(mapping.Type)
                .Invoke(this, new object[] { entry, ct })!;
        }

        /// <summary>Synchronous <see cref="ReloadEntryAsync"/> (see <see cref="EntityEntry.Reload"/>).</summary>
        [RequiresDynamicCode("Reload builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reload reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        internal bool ReloadEntry(EntityEntry entry)
        {
            var mapping = ValidateReloadable(entry);
            try
            {
                return (bool)s_reloadCoreSyncMethod.MakeGenericMethod(mapping.Type)
                    .Invoke(this, new object[] { entry })!;
            }
            catch (TargetInvocationException tie) when (tie.InnerException != null)
            {
                // Surface the real query/materialization error, not the reflection wrapper.
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
                throw; // unreachable
            }
        }

        private TableMapping ValidateReloadable(EntityEntry entry)
        {
            ThrowIfDisposed();
            ArgumentNullException.ThrowIfNull(entry);
            if (entry.Entity == null)
                throw new InvalidOperationException("Cannot reload a detached entry.");
            var mapping = entry.Mapping;
            if (mapping.KeyColumns.Length == 0)
                throw new NormUsageException(
                    $"Reload requires a primary key, but entity type '{mapping.Type.Name}' has none.");
            return mapping;
        }

        [RequiresDynamicCode("Reload builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reload reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        private async Task<bool> ReloadEntryCoreAsync<T>(EntityEntry entry, CancellationToken ct) where T : class
        {
            var (mapping, entity, predicate) = PrepareReload<T>(entry);
            var fresh = await this.Query<T>().Where(predicate).AsNoTracking().FirstOrDefaultAsync(ct).ConfigureAwait(false);
            return ApplyReload(entry, mapping, entity, fresh);
        }

        [RequiresDynamicCode("Reload builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reload reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        private bool ReloadEntryCore<T>(EntityEntry entry) where T : class
        {
            var (mapping, entity, predicate) = PrepareReload<T>(entry);
            var fresh = this.Query<T>().Where(predicate).AsNoTracking().FirstOrDefault();
            return ApplyReload(entry, mapping, entity, fresh);
        }

        [RequiresDynamicCode("Reload builds a key predicate; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reload reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        private static (TableMapping Mapping, T Entity, Expression<Func<T, bool>> Predicate) PrepareReload<T>(EntityEntry entry) where T : class
        {
            var mapping = entry.Mapping;
            var entity = (T)entry.Entity!;
            var keyValues = new object?[mapping.KeyColumns.Length];
            for (var i = 0; i < keyValues.Length; i++)
                keyValues[i] = mapping.KeyColumns[i].Getter(entity);
            return (mapping, entity, BuildFindPredicate<T>(mapping, keyValues));
        }

        private bool ApplyReload<T>(EntityEntry entry, TableMapping mapping, T entity, T? fresh) where T : class
        {
            if (fresh == null)
            {
                // The row no longer exists — detach the entity, matching EF's Reload semantics.
                ChangeTracker.Remove(entity);
                return false;
            }

            // Overwrite the tracked instance's column values with the fresh (already-converted) values,
            // then accept them as the clean baseline. Navigation properties are intentionally untouched.
            foreach (var col in mapping.Columns)
                col.Setter(entity, col.Getter(fresh));
            entry.AcceptChanges();
            return true;
        }
    }
}
