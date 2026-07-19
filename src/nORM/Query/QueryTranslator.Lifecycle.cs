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
using Microsoft.Extensions.Logging;
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
        private static readonly ObjectPool<QueryTranslator> _translatorPool =
            new DefaultObjectPool<QueryTranslator>(new QueryTranslatorPooledObjectPolicy());
        private static readonly ObjectPool<List<string>> _selectItemsPool =
            new DefaultObjectPool<List<string>>(new ListPooledObjectPolicy<string>());
        private static readonly AdaptiveQueryComplexityAnalyzer _complexityAnalyzer =
            new AdaptiveQueryComplexityAnalyzer(new SystemMemoryMonitor());
        private QueryTranslator()
        {
        }
        public QueryTranslator(DbContext ctx)
        {
            Reset(ctx);
        }
        private QueryTranslator(
            DbContext ctx,
            TableMapping mapping,
            IDictionary<string, object> parameters,
            int pIndex,
            Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> correlated,
            HashSet<string> tables,
            List<string> compiledParams,
            Dictionary<ParameterExpression, string> paramMap,
            int joinStart = 0,
            int recursionDepth = 0)
        {
            _ctx = ctx;
            _provider = ctx.RawProvider;
            _mapping = mapping;
            _rootType = mapping.Type;
            _params = parameters;
            _parameterManager.Index = pIndex;
            _correlatedParams = correlated;
            _tables = tables;
            _compiledParams = compiledParams;
            _paramMap = paramMap;
            _tables.Add(mapping.TableName);
            RecordReferencedTable(mapping.TableName);
            _joinCounter = joinStart;
            _recursionDepth = recursionDepth;
            _maxRecursionDepth = ctx.Options.MaxRecursionDepth;
        }
        internal static QueryTranslator Create(
            DbContext ctx,
            TableMapping mapping,
            IDictionary<string, object> parameters,
            int pIndex,
            Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)> correlated,
            HashSet<string> tables,
            List<string> compiledParams,
            Dictionary<ParameterExpression, string> paramMap,
            int joinStart = 0,
            int recursionDepth = 0)
            => new QueryTranslator(ctx, mapping, parameters, pIndex, correlated, tables, compiledParams, paramMap, joinStart, recursionDepth);
        internal static QueryTranslator Rent(DbContext ctx)
        {
            var t = _translatorPool.Get();
            t.Reset(ctx);
            return t;
        }
        private void Reset(DbContext ctx)
        {
            lock (_syncRoot)
            {
                SqlBuilder? oldClauses = Interlocked.Exchange(ref _clauses, null!);
                oldClauses?.Dispose();
                _ctx = ctx;
                _provider = ctx.RawProvider;
                _mapping = null!;
                _rootType = null;
                _parameterManager.Reset();
                _includes = new List<IncludePlan>();
                _m2mIncludes = new List<M2MIncludePlan>();
                _projection = null;
                _clientProjection = null;
                _clientProjectionResultType = null;
                _compositeKeyMemberSql.Clear();
                _windowedGroupBySubSql = null;
                _windowedGroupByAlias = null;
                _outerDerivedAlias = null;
                _isAggregate = false;
                _methodName = string.Empty;
                _correlatedParams = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
                _groupJoinInfo = null;
                _groupJoinResultSelector = null;
                _groupJoinExpansionSelector = null;
                _groupByElementSelector = null;
                _groupByKeySelector = null;
                _groupOrderedFirstSourceWheres = null;
                _closureFoldedIntoSql = false;
                _selfRootAlias = null;
                _joinCounter = 0;
                _recursionDepth = 0;
                _singleResult = false;
                _noTracking = false;
                _forceTracking = false;
                _identityResolution = false;
                _trackingDecided = false;
                _splitQuery = false;
                _queryTags = null;
                _tables = new HashSet<string>();
                _clauses = new SqlBuilder();
                _contextStack.Clear();
                _estimatedTimeout = ctx.Options.TimeoutConfiguration.BaseTimeout;
                _isCacheable = false;
                _cacheExpiration = null;
                _asOfTimestamp = null;
                _detectedCollections = new List<PropertyInfo>();
                _detectedCollectionFilters = new Dictionary<PropertyInfo, SelectClauseVisitor.RenderedCollectionFilter>();
                _detectedCollectionProjections = new Dictionary<PropertyInfo, System.Linq.Expressions.LambdaExpression>();
                _detectedCollectionTargetMembers = new Dictionary<PropertyInfo, PropertyInfo>();
                _complexityMetrics = default;
                _postMaterializeTransform = null;
                _postMaterializeOrderPrefixTransform = null;
                _postMaterializeOrderings = null;
                _postMaterializeElementType = null;
                _postReverseResult = false;
                _clientScalarResult = false;
                _flattenedLeftJoinEntityResult = false;
                // Capture the configured recursion depth limit at Reset time.
                _maxRecursionDepth = ctx.Options.MaxRecursionDepth;
            }
        }
        private void Clear()
        {
            lock (_syncRoot)
            {
                SqlBuilder? oldClauses = Interlocked.Exchange(ref _clauses, null!);
                oldClauses?.Dispose();
                _clauses = new SqlBuilder();
                _includes = new List<IncludePlan>();
                _m2mIncludes = new List<M2MIncludePlan>();
                _correlatedParams = new Dictionary<ParameterExpression, (TableMapping Mapping, string Alias)>();
                _tables = new HashSet<string>();
                _ctx = null!;
                _provider = null!;
                _mapping = null!;
                _rootType = null;
                _parameterManager.Reset();
                _projection = null;
                _clientProjection = null;
                _clientProjectionResultType = null;
                _compositeKeyMemberSql.Clear();
                _windowedGroupBySubSql = null;
                _windowedGroupByAlias = null;
                _outerDerivedAlias = null;
                _streamingGroupByKeySelector = null;
                _isAggregate = false;
                _methodName = string.Empty;
                _groupJoinInfo = null;
                _groupJoinResultSelector = null;
                _groupJoinExpansionSelector = null;
                _groupByElementSelector = null;
                _groupByKeySelector = null;
                _groupOrderedFirstSourceWheres = null;
                _closureFoldedIntoSql = false;
                _selfRootAlias = null;
                _joinCounter = 0;
                _recursionDepth = 0;
                _contextStack.Clear();
                _singleResult = false;
                _noTracking = false;
                _forceTracking = false;
                _identityResolution = false;
                _trackingDecided = false;
                _splitQuery = false;
                _queryTags = null;
                _estimatedTimeout = default;
                _isCacheable = false;
                _cacheExpiration = null;
                _asOfTimestamp = null;
                _detectedCollections = new List<PropertyInfo>();
                _detectedCollectionFilters = new Dictionary<PropertyInfo, SelectClauseVisitor.RenderedCollectionFilter>();
                _detectedCollectionProjections = new Dictionary<PropertyInfo, System.Linq.Expressions.LambdaExpression>();
                _detectedCollectionTargetMembers = new Dictionary<PropertyInfo, PropertyInfo>();
                _complexityMetrics = default;
                _postMaterializeTransform = null;
                _postMaterializeOrderPrefixTransform = null;
                _postMaterializeOrderings = null;
                _postMaterializeElementType = null;
                _postReverseResult = false;
                _clientScalarResult = false;
                _flattenedLeftJoinEntityResult = false;
            }
        }
        /// <summary>
        /// Creates a delegate that materializes rows from a reader into the specified type.
        /// </summary>
        /// <param name="mapping">Mapping describing the table schema.</param>
        /// <param name="targetType">Type to materialize into.</param>
        /// <param name="projection">Optional projection expression.</param>
        /// <returns>A materializer delegate.</returns>
        public Func<DbDataReader, CancellationToken, Task<object>> CreateMaterializer(TableMapping mapping, Type targetType, LambdaExpression? projection = null)
            => _materializerFactory.CreateMaterializer(mapping, targetType, projection);
        private static Type GetElementType(Expression queryExpression)
        {
            var type = queryExpression.Type;
            if (type.IsGenericType)
            {
                var args = type.GetGenericArguments();
                if (args.Length > 0) return args[0];
            }
            var iface = type.GetInterfaces()
                .FirstOrDefault(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IQueryable<>));
            if (iface != null) return iface.GetGenericArguments()[0];
            throw new NormQueryException(string.Format(ErrorMessages.QueryTranslationFailed, $"Cannot determine element type from expression of type {type}"));
        }

        /// <summary>
        /// Releases resources used by the translator and, if pooled, returns it to the shared pool.
        /// </summary>
        public void Dispose()
        {
            // Clear() already performs Interlocked.Exchange + Dispose on _clauses,
            // so delegate directly to avoid a redundant SqlBuilder allocation.
            Clear();
        }
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class QueryTranslatorPooledObjectPolicy : PooledObjectPolicy<QueryTranslator>
        {
            /// <summary>
            /// Creates a new <see cref="QueryTranslator"/> for inclusion in the pool.
            /// </summary>
            /// <returns>A newly constructed translator instance.</returns>
            public override QueryTranslator Create() => new QueryTranslator();

            /// <summary>
            /// Resets a translator before returning it to the pool for reuse.
            /// </summary>
            /// <param name="obj">The translator to recycle.</param>
            /// <returns>Always <c>true</c> to indicate pooling should continue.</returns>
            public override bool Return(QueryTranslator obj)
            {
                obj.Clear();
                return true;
            }
        }

        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Runtime LINQ translation can build generic types and delegates at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Runtime LINQ translation reflects over entity types; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ListPooledObjectPolicy<T> : PooledObjectPolicy<List<T>>
        {
            /// <summary>
            /// Creates a new list instance for pooling.
            /// </summary>
            public override List<T> Create() => new List<T>();

            /// <summary>
            /// Clears the list prior to returning it to the pool.
            /// </summary>
            /// <param name="obj">The list to reset.</param>
            /// <returns>Always <c>true</c>, indicating the list can be reused.</returns>
            public override bool Return(List<T> obj)
            {
                obj.Clear();
                return true;
            }
        }
    }
}
