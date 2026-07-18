using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        private static readonly MethodInfo s_getDatabaseValuesCoreAsyncMethod =
            typeof(DbContext).GetMethod(nameof(GetDatabaseValuesCoreAsync), BindingFlags.Instance | BindingFlags.NonPublic)!;
        private static readonly MethodInfo s_getDatabaseValuesCoreSyncMethod =
            typeof(DbContext).GetMethod(nameof(GetDatabaseValuesCore), BindingFlags.Instance | BindingFlags.NonPublic)!;

        /// <summary>
        /// Reads an entry's current database row as a detached <see cref="PropertyValues"/> snapshot
        /// (see <see cref="EntityEntry.GetDatabaseValues"/>). Dispatches to the strongly-typed core so the
        /// no-tracking key query reuses the standard materializer (converters, provider coercion); the
        /// values are then wrapped in a tracker-less holder entry so they stay detached from the graph.
        /// </summary>
        [RequiresDynamicCode("GetDatabaseValues builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("GetDatabaseValues reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        internal Task<PropertyValues?> GetDatabaseValuesForEntryAsync(EntityEntry entry, CancellationToken ct)
        {
            var mapping = ValidateReloadable(entry);
            return (Task<PropertyValues?>)s_getDatabaseValuesCoreAsyncMethod.MakeGenericMethod(mapping.Type)
                .Invoke(this, new object[] { entry, ct })!;
        }

        /// <summary>Synchronous <see cref="GetDatabaseValuesForEntryAsync"/> (see <see cref="EntityEntry.GetDatabaseValues"/>).</summary>
        [RequiresDynamicCode("GetDatabaseValues builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("GetDatabaseValues reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        internal PropertyValues? GetDatabaseValuesForEntry(EntityEntry entry)
        {
            var mapping = ValidateReloadable(entry);
            try
            {
                return (PropertyValues?)s_getDatabaseValuesCoreSyncMethod.MakeGenericMethod(mapping.Type)
                    .Invoke(this, new object[] { entry })!;
            }
            catch (TargetInvocationException tie) when (tie.InnerException != null)
            {
                // Surface the real query/materialization error, not the reflection wrapper.
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(tie.InnerException).Throw();
                throw; // unreachable
            }
        }

        [RequiresDynamicCode("GetDatabaseValues builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("GetDatabaseValues reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        private async Task<PropertyValues?> GetDatabaseValuesCoreAsync<T>(EntityEntry entry, CancellationToken ct) where T : class
        {
            var (mapping, _, predicate) = PrepareReload<T>(entry);
            var fresh = await this.Query<T>().Where(predicate).AsNoTracking().FirstOrDefaultAsync(ct).ConfigureAwait(false);
            return fresh == null ? null : BuildDetachedValues(fresh, mapping);
        }

        [RequiresDynamicCode("GetDatabaseValues builds a key predicate and lifts it onto the entity query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("GetDatabaseValues reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        private PropertyValues? GetDatabaseValuesCore<T>(EntityEntry entry) where T : class
        {
            var (mapping, _, predicate) = PrepareReload<T>(entry);
            var fresh = this.Query<T>().Where(predicate).AsNoTracking().FirstOrDefault();
            return fresh == null ? null : BuildDetachedValues(fresh, mapping);
        }

        /// <summary>
        /// Wraps a freshly-read (no-tracking) entity's column values in a tracker-less holder entry and
        /// returns its <see cref="EntityEntry.CurrentValues"/>. The holder is never registered with the
        /// change tracker, so the returned bag is a standalone database snapshot — reading it, mutating it,
        /// or copying it onto a tracked entry never disturbs the graph.
        /// </summary>
        private PropertyValues BuildDetachedValues(object fresh, TableMapping mapping)
            => new EntityEntry(fresh, EntityState.Detached, mapping, Options, tracker: null, lazy: true).CurrentValues;
    }
}
