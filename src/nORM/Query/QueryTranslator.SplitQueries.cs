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

            foreach (var collectionProperty in _detectedCollections)
            {
                // Try to find the relation for this navigation property
                if (!_mapping.Relations.TryGetValue(collectionProperty.Name, out var relation))
                {
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

                // Create the dependent query definition
                var dependentQuery = new DependentQueryDefinition(
                    TargetMapping: targetMapping,
                    ForeignKeyColumns: relation.ForeignKeys,
                    ParentKeyProperties: relation.PrincipalKeys.Select(c => c.Prop).ToArray(),
                    TargetCollectionProperty: collectionProperty,
                    CollectionElementType: elementType,
                    FilterSql: filter.Sql,
                    FilterParameters: filter.Parameters
                );

                dependentQueries.Add(dependentQuery);
            }

            return dependentQueries;
        }
    }
}
