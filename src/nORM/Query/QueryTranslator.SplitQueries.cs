using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.ObjectPool;
using System.Text;
using nORM.Core;
using nORM.Configuration;
using nORM.Internal;
using nORM.Mapping;
using nORM.Providers;
using nORM.SourceGeneration;
#nullable enable
namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        /// <summary>
        /// Builds dependent query definitions for detected navigation collections.
        /// This enables split query execution to avoid Cartesian explosion.
        /// </summary>
        private List<DependentQueryDefinition> BuildDependentQueryDefinitions()
        {
            var dependentQueries = new List<DependentQueryDefinition>();

            // Two shaped/bare bindings over the SAME navigation (new { A = o.Lines.Where(x).ToList(),
            // B = o.Lines.Where(y).ToList() }) collide: the per-navigation filter/projection/target maps keep
            // only the last binding, so the split query would run once and stitch a single member, leaving the
            // other SILENTLY EMPTY. Fail loud instead of returning wrong data. (_detectedCollections keeps both
            // entries even though the maps collided, so the duplicate is detectable here.) Supporting multiple
            // projections of one navigation is a follow-up (the maps would need to key by target member).
            var seenNavigations = new HashSet<string>(StringComparer.Ordinal);
            foreach (var c in _detectedCollections)
                if (!seenNavigations.Add(c.Name))
                    throw new NormUnsupportedFeatureException(
                        $"Projecting the navigation collection '{c.Name}' into more than one member of a single " +
                        "projection isn't supported yet — the shaped-collection loads would collide and silently " +
                        "drop one. Project each navigation at most once, or load them in separate queries.");

            // A collection PROJECTION under AsOf reconstructs its children at the timestamp through the child
            // load's as-of FROM source (see FetchChildrenBatch/BuildDependentFromSource): the {Table}_History
            // window for nORM-managed storage, or the provider's FOR SYSTEM_TIME AS OF clause for
            // provider-native (system-versioned) storage. Both keep the table's own name so the FK/tenant/
            // global/element filters resolve to the reconstructed rows, never the live era.

            foreach (var collectionProperty in _detectedCollections)
            {
                // Resolve the collection's kind — an ordinary relation, an owned (OwnsMany) collection, or a
                // many-to-many. Owned/m2m use separate mapping structures and are recorded on the
                // DependentQueryDefinition's Owned/M2M discriminant so the fetch/stitch can branch (they carry
                // no child FK property / correlate through the bridge table). The element type, filter,
                // projection, and target-member resolution below are shared across all three kinds.
                TableMapping targetMapping;
                IReadOnlyList<Column> fkColumns;
                IReadOnlyList<PropertyInfo> parentKeyProps;
                OwnedCollectionMapping? owned = null;
                JoinTableMapping? m2m = null;

                if (_mapping.Relations.TryGetValue(collectionProperty.Name, out var relation))
                {
                    targetMapping = _ctx.GetMapping(relation.DependentType);
                    fkColumns = relation.ForeignKeys;
                    parentKeyProps = relation.PrincipalKeys.Select(c => c.Prop).ToArray();
                }
                else if ((m2m = _mapping.ManyToManyJoins.FirstOrDefault(j => j.LeftNavPropertyName == collectionProperty.Name)) != null)
                {
                    targetMapping = _ctx.GetMapping(m2m.RightType);
                    fkColumns = Array.Empty<Column>();   // m2m correlates through the bridge table, not a child FK
                    parentKeyProps = m2m.LeftKeyColumns.Select(c => c.Prop).ToArray();
                }
                else if (_mapping.OwnedCollections.Any(o => o.NavigationProperty.Name == collectionProperty.Name))
                {
                    // Owned shaped projections are the next increment; still fail loud rather than build a
                    // definition the executor can't run.
                    throw new NormUnsupportedFeatureException(
                        $"Projecting the owned collection '{collectionProperty.Name}' into a shaped result isn't " +
                        "supported yet. Load it with Include(...), or fetch it in a separate query.");
                }
                else
                {
                    continue; // not a mapped collection
                }

                // Get the element type of the collection
                var collectionType = collectionProperty.PropertyType;
                Type elementType;

                if (collectionType.IsGenericType)
                {
                    elementType = collectionType.GetGenericArguments()[0];
                }
                else
                {
                    // Try to find IEnumerable<T>
                    var iEnumerable = collectionType.GetInterfaces()
                        .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IEnumerable<>));
                    if (iEnumerable == null)
                        continue;

                    elementType = iEnumerable.GetGenericArguments()[0];
                }

                // A shaped projection binding (Lines = o.Lines.Where(pred).ToList()) captured a
                // per-element filter, rendered to SQL at translation time so its closures flow through
                // the shared compiled-parameter channel; the child fetch ANDs it on and binds its params.
                _detectedCollectionFilters.TryGetValue(collectionProperty, out var filter);

                // A shaped projection binding that also projects each element
                // (Lines = o.Lines.Select(l => new LineDto{...}).ToList()) captured an element projection.
                // The collection then holds the PROJECTED type, and the child materializer applies the
                // projection client-side over each fetched child entity.
                _detectedCollectionProjections.TryGetValue(collectionProperty, out var elementProjection);
                var dependentElementType = elementProjection?.ReturnType ?? elementType;

                // Filtered/projected many-to-many shaped projections are a later sub-increment; only a bare
                // .ToList() is wired so far. Fail loud rather than silently drop the filter/projection.
                if (m2m != null && (filter.Sql != null || elementProjection != null))
                    throw new NormUnsupportedFeatureException(
                        $"Projecting the many-to-many collection '{collectionProperty.Name}' with a Where(...) filter " +
                        "or an element projection isn't supported yet — use a bare .ToList(), or Include(...).");

                // A closure-capturing element projection is applied client-side; baking it into a cached
                // delegate would replay the first execution's captured value. Mark the plan non-cacheable so
                // it re-translates (a fresh projection with the current capture) on every execution.
                if (elementProjection != null && SelectClauseVisitor.ProjectionCapturesClosures(elementProjection))
                    _closureFoldedIntoSql = true;

                // The projection's DTO property may be named differently from the navigation; the stitch
                // assigns to THAT member (falling back to the nav when the binding member wasn't captured,
                // e.g. a field-backed binding).
                var targetMember = _detectedCollectionTargetMembers.TryGetValue(collectionProperty, out var boundMember)
                    ? boundMember
                    : collectionProperty;

                // Create the dependent query definition
                var dependentQuery = new DependentQueryDefinition(
                    TargetMapping: targetMapping,
                    ForeignKeyColumns: fkColumns,
                    ParentKeyProperties: parentKeyProps,
                    TargetCollectionProperty: targetMember,
                    CollectionElementType: dependentElementType,
                    FilterSql: filter.Sql,
                    FilterParameters: filter.Parameters,
                    ElementProjection: elementProjection,
                    Owned: owned,
                    M2M: m2m
                );

                dependentQueries.Add(dependentQuery);
            }

            return dependentQueries;
        }
    }
}
