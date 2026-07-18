using System;
using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using nORM.Mapping;

#nullable enable

namespace nORM.Core
{
    public partial class DbContext
    {
        private static readonly MethodInfo s_loadReferenceCoreMethod =
            typeof(DbContext).GetMethod(nameof(LoadReferenceCoreAsync), BindingFlags.Instance | BindingFlags.NonPublic)!;

        /// <summary>
        /// Loads a reference (dependent → principal) navigation for an entry, resolving the foreign key from
        /// the mapping and fetching the principal by key (identity-map aware). nORM's lazy-loading path is
        /// principal → dependent only, so this backs <see cref="NavigationEntry.LoadAsync"/> for the to-one
        /// direction directly.
        /// </summary>
        [RequiresDynamicCode("Reference loading builds a key predicate and lifts it onto the principal query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reference loading reflects over the relationship metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        internal Task LoadReferenceNavigationForEntryAsync(object entity, Type entityType, PropertyInfo property, CancellationToken ct)
        {
            ThrowIfDisposed();
            var ownerMap = GetMapping(entityType);
            var principalType = property.PropertyType;
            TableMapping principalMap;
            try
            {
                principalMap = GetMapping(principalType);
            }
            catch (Exception ex)
            {
                throw new NormUsageException(
                    $"Reference navigation '{property.Name}' targets '{principalType.Name}', which is not a mapped entity.", ex);
            }

            var fkColumn = nORM.Query.ExpressionToSqlVisitor.FindReferenceNavForeignKey(ownerMap, property.Name, principalType, principalMap);
            if (fkColumn == null)
                throw new NormUsageException(
                    $"Cannot resolve the foreign key for reference navigation '{property.Name}' on '{entityType.Name}'. " +
                    "Configure the relationship with HasOne/WithMany or a conventional '{Principal}Id' foreign key.");

            var fkValue = fkColumn.Getter(entity);
            if (fkValue == null)
            {
                // A null foreign key means no principal — clear the navigation to reflect that.
                property.SetValue(entity, null);
                return Task.CompletedTask;
            }

            return (Task)s_loadReferenceCoreMethod.MakeGenericMethod(principalType)
                .Invoke(this, new object[] { entity, property, fkValue, ct })!;
        }

        /// <summary>Synchronous <see cref="LoadReferenceNavigationForEntryAsync"/>.</summary>
        [RequiresDynamicCode("Reference loading builds a key predicate and lifts it onto the principal query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reference loading reflects over the relationship metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        internal void LoadReferenceNavigationForEntry(object entity, Type entityType, PropertyInfo property)
            => LoadReferenceNavigationForEntryAsync(entity, entityType, property, CancellationToken.None).GetAwaiter().GetResult();

        [RequiresDynamicCode("Reference loading builds a key predicate; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reference loading reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        private async Task LoadReferenceCoreAsync<TPrincipal>(object entity, PropertyInfo property, object fkValue, CancellationToken ct)
            where TPrincipal : class
        {
            // FindAsync resolves the identity map first (returning an already-tracked principal), then
            // queries by key — exactly the semantics EF's reference Load uses.
            var principal = await FindAsync<TPrincipal>(new object?[] { fkValue }, ct).ConfigureAwait(false);
            property.SetValue(entity, principal);
        }
    }
}
