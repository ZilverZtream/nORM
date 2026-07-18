using System;
using System.Collections;
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
        private static readonly MethodInfo s_loadReferenceCoreMethod =
            typeof(DbContext).GetMethod(nameof(LoadReferenceCoreAsync), BindingFlags.Instance | BindingFlags.NonPublic)!;

        /// <summary>
        /// Handles a dependent → principal reference navigation (this entity holds the foreign key, e.g.
        /// <c>Line.Order</c>) by loading the principal directly by that key — nORM's relationship loader
        /// reads the principal key from the wrong side for this direction and never populates it. Returns
        /// false (leaving <paramref name="task"/> unset) for collection navs, principal → dependent
        /// references, and composite-key principals, which the relationship loader handles.
        /// </summary>
        [RequiresDynamicCode("Reference loading builds a key predicate and lifts it onto the principal query; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reference loading reflects over the relationship metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        internal bool TryLoadDependentToPrincipalReference(object entity, Type entityType, PropertyInfo property, CancellationToken ct, out Task task)
        {
            task = Task.CompletedTask;

            // Collections are handled by the batched relationship loader, not here.
            if (property.PropertyType == typeof(string) || typeof(IEnumerable).IsAssignableFrom(property.PropertyType))
                return false;

            var ownerMap = GetMapping(entityType);
            var principalType = property.PropertyType;
            TableMapping principalMap;
            try { principalMap = GetMapping(principalType); }
            catch { return false; }

            // A non-null result means THIS entity carries the foreign key (dependent → principal). A null
            // result means the principal → dependent case (the other side holds the FK), which
            // LoadRelationshipAsync loads correctly, including composite keys.
            var fkColumn = nORM.Query.ExpressionToSqlVisitor.FindReferenceNavForeignKey(ownerMap, property.Name, principalType, principalMap);
            if (fkColumn == null || principalMap.KeyColumns.Length != 1)
                return false;

            task = LoadDependentToPrincipalReferenceAsync(entity, entityType, principalType, principalMap, property, fkColumn, ct);
            return true;
        }

        [RequiresDynamicCode("Reference loading builds a key predicate; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reference loading reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        private async Task LoadDependentToPrincipalReferenceAsync(
            object entity, Type entityType, Type principalType, TableMapping principalMap, PropertyInfo property, Column fkColumn, CancellationToken ct)
        {
            var fkValue = fkColumn.Getter(entity);
            if (fkValue == null)
            {
                property.SetValue(entity, null);
            }
            else
            {
                await (Task)s_loadReferenceCoreMethod.MakeGenericMethod(principalType)
                    .Invoke(this, new object[] { entity, property, principalMap, fkValue, ct })!;
            }
            nORM.Navigation.NavigationPropertyExtensions.SetNavigationLoaded(entity, property, true, entityType, this);
        }

        [RequiresDynamicCode("Reference loading builds a key predicate; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [RequiresUnreferencedCode("Reference loading reflects over the mapped key metadata; trimming may remove the required members. See docs/aot-trimming.md.")]
        private async Task LoadReferenceCoreAsync<TPrincipal>(object entity, PropertyInfo property, TableMapping principalMap, object fkValue, CancellationToken ct)
            where TPrincipal : class
        {
            // Tracking query so identity resolution returns an already-tracked principal, matching EF.
            var predicate = BuildFindPredicate<TPrincipal>(principalMap, new object?[] { fkValue });
            var principal = await this.Query<TPrincipal>().Where(predicate).FirstOrDefaultAsync(ct).ConfigureAwait(false);
            property.SetValue(entity, principal);
        }
    }
}
