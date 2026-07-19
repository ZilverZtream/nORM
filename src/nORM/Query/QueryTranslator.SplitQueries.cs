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

            // A collection PROJECTION under AsOf (Select(o => new { Lines = o.Lines.Where(p).ToList() }).AsOf(t))
            // would silently mix eras: the dependent child query (FetchChildrenBatch) reads the LIVE table, not
            // the reconstructed history window the root reads at the timestamp — so it returns present-day child
            // rows and applies any element filter to live values. Filtered Include under AsOf IS handled
            // (IncludeProcessor reconstructs the era); the projection/split path is not yet. Fail loud rather
            // than return wrong data. Proper fix: build the child FROM as the {Table}_History window with the
            // @asof param, re-aliasing the tenant/global/element filters onto that derived table — the owned-
            // collection path (DbContext.OwnedCollections.cs) already does exactly this for owned children.
            if (_detectedCollections.Count > 0 && (_asOfTimestamp.HasValue || HasActiveTemporalScope))
                throw new NormUnsupportedFeatureException(
                    "Projecting a navigation collection under AsOf (temporal) isn't supported yet — the child " +
                    "load would read live rows and mix eras. Use a filtered Include(...) under AsOf, or " +
                    "materialise the query first (ToList) and project in memory.");

            foreach (var collectionProperty in _detectedCollections)
            {
                // Try to find the relation for this navigation property
                if (!_mapping.Relations.TryGetValue(collectionProperty.Name, out var relation))
                {
                    // Owned (OwnsMany) and many-to-many collections use separate mapping structures the
                    // split-query stitch doesn't cover. They are admitted as shaped-collection bindings
                    // upstream, so silently skipping here left the materializer expecting a stitched column
                    // that never arrived — an opaque ordinal error at read time. Fail loud with an actionable
                    // message instead of crashing (full shaped-projection support for these is a follow-up).
                    var isOwned = _mapping.OwnedCollections.Any(o => o.NavigationProperty.Name == collectionProperty.Name);
                    var isManyToMany = _mapping.ManyToManyJoins.Any(j => j.LeftNavPropertyName == collectionProperty.Name);
                    if (isOwned || isManyToMany)
                        throw new NormUnsupportedFeatureException(
                            $"Projecting the {(isOwned ? "owned" : "many-to-many")} collection '{collectionProperty.Name}' " +
                            "into a shaped result isn't supported yet. Load it with Include(...), or fetch it in a separate query.");
                    continue; // Skip if relation not found
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

                // Get the target mapping for the dependent type
                var targetMapping = _ctx.GetMapping(relation.DependentType);

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
                    ForeignKeyColumns: relation.ForeignKeys,
                    ParentKeyProperties: relation.PrincipalKeys.Select(c => c.Prop).ToArray(),
                    TargetCollectionProperty: targetMember,
                    CollectionElementType: dependentElementType,
                    FilterSql: filter.Sql,
                    FilterParameters: filter.Parameters,
                    ElementProjection: elementProjection
                );

                dependentQueries.Add(dependentQuery);
            }

            return dependentQueries;
        }
    }
}
