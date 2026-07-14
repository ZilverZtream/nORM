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
        /// <summary>
        /// Translates a sub-expression using a nested translation context.
        /// </summary>
        /// <remarks>
        /// Uses a stack-based context swap instead of allocating a new <see cref="QueryTranslator"/>,
        /// reducing allocations for nested subqueries such as unions or correlated WHERE clauses.
        /// </remarks>
        private string TranslateSubExpression(Expression e)
        {
            if (_recursionDepth >= _maxRecursionDepth)
                throw new NormQueryException(
                    $"Query exceeds maximum translation depth of {_maxRecursionDepth}. " +
                    $"This typically indicates overly complex nested subqueries. " +
                    $"Consider simplifying the query by breaking it into multiple queries or using CTEs. " +
                    $"You can also increase the limit via DbContextOptions.MaxRecursionDepth (current: {_maxRecursionDepth}, max: 200).");

            // Log deep recursion for monitoring.
            if (_recursionDepth > Math.Min(DeepRecursionWarningAbsolute, _maxRecursionDepth / 2))
            {
                _ctx.Options.Logger?.LogWarning(
                    "Query translation depth is {Depth} (max: {MaxDepth}). " +
                    "Deep nesting causes O(depth) allocations (~2KB per level). " +
                    "Consider query simplification or increase DbContextOptions.MaxRecursionDepth.",
                    _recursionDepth + 1, _maxRecursionDepth);
            }

            _complexityMetrics.SubqueryDepth++;
            var subPlan = TranslateInSubContext(e, _mapping, _parameterManager.Index, _joinCounter, _recursionDepth + 1, out _);
            return subPlan.Sql;
        }

        private TranslationContextSnapshot CaptureContext()
        {
            return new TranslationContextSnapshot(
                _clauses,
                _includes,
                _projection,
                _isAggregate,
                _methodName,
                _groupJoinInfo,
                _groupJoinExpansionSelector,
                _joinCounter,
                _singleResult,
                _noTracking,
                _splitQuery,
                _mapping,
                _rootType,
                _estimatedTimeout,
                _isCacheable,
                _cacheExpiration,
                _asOfTimestamp,
                _recursionDepth,
                _clientProjection,
                _clientProjectionResultType,
                _groupJoinResultSelector,
                _groupByElementSelector,
                _streamingGroupByKeySelector,
                _postMaterializeTransform,
                _postMaterializeElementType,
                _postMaterializeOrderPrefixTransform,
                _postMaterializeOrderings,
                _postReverseResult,
                _clientScalarResult);
        }
        private void RestoreContext(TranslationContextSnapshot snapshot)
        {
            _clauses = snapshot.Clauses;
            _includes = snapshot.Includes;
            _projection = snapshot.Projection;
            _isAggregate = snapshot.IsAggregate;
            _methodName = snapshot.MethodName;
            _groupJoinInfo = snapshot.GroupJoinInfo;
            _groupJoinExpansionSelector = snapshot.GroupJoinExpansionSelector;
            _joinCounter = snapshot.JoinCounter;
            _singleResult = snapshot.SingleResult;
            _noTracking = snapshot.NoTracking;
            _splitQuery = snapshot.SplitQuery;
            _mapping = snapshot.Mapping;
            _rootType = snapshot.RootType;
            _estimatedTimeout = snapshot.EstimatedTimeout;
            _isCacheable = snapshot.IsCacheable;
            _cacheExpiration = snapshot.CacheExpiration;
            _asOfTimestamp = snapshot.AsOfTimestamp;
            _recursionDepth = snapshot.RecursionDepth;
            _clientProjection = snapshot.ClientProjection;
            _clientProjectionResultType = snapshot.ClientProjectionResultType;
            _groupJoinResultSelector = snapshot.GroupJoinResultSelector;
            _groupByElementSelector = snapshot.GroupByElementSelector;
            _streamingGroupByKeySelector = snapshot.StreamingGroupByKeySelector;
            _postMaterializeTransform = snapshot.PostMaterializeTransform;
            _postMaterializeElementType = snapshot.PostMaterializeElementType;
            _postMaterializeOrderPrefixTransform = snapshot.PostMaterializeOrderPrefixTransform;
            _postMaterializeOrderings = snapshot.PostMaterializeOrderings;
            _postReverseResult = snapshot.PostReverseResult;
            _clientScalarResult = snapshot.ClientScalarResult;
        }
        private QueryPlan TranslateInSubContext(Expression e, TableMapping mapping, int parameterIndex, int joinStart, int recursionDepth, out TableMapping resultingMapping)
        {
            var snapshot = CaptureContext();
            _contextStack.Push(snapshot);
            // The sub-context binds its own root parameter; its alias must neither
            // inherit the outer scope's nor leak back to it (Build's FROM clause
            // prefers this over the ambiguous mapping-based lookup).
            var outerSelfRootAlias = _selfRootAlias;
            _selfRootAlias = null;
            try
            {
                _clauses = new SqlBuilder();
                _includes = new List<IncludePlan>();
                _projection = null;
                _clientProjection = null;
                _clientProjectionResultType = null;
                _isAggregate = false;
                _methodName = string.Empty;
                _groupJoinInfo = null;
                _groupJoinResultSelector = null;
                _groupJoinExpansionSelector = null;
                _groupByElementSelector = null;
                _streamingGroupByKeySelector = null;
                // Client-tail reshape state must NOT leak into a sub-context: a nested
                // subquery or set-op arm translates against its own clean tail-state,
                // and the outer's transform is reinstated by RestoreContext on exit.
                _postMaterializeTransform = null;
                _postMaterializeElementType = null;
                _postMaterializeOrderPrefixTransform = null;
                _postMaterializeOrderings = null;
                _postReverseResult = false;
                _clientScalarResult = false;
                _joinCounter = joinStart;
                _singleResult = false;
                _noTracking = false;
                _splitQuery = false;
                _mapping = mapping;
                _rootType = mapping.Type;
                _estimatedTimeout = _ctx.Options.TimeoutConfiguration.BaseTimeout;
                _isCacheable = false;
                _cacheExpiration = null;
                _asOfTimestamp = null;
                _parameterManager.Index = parameterIndex;
                _recursionDepth = recursionDepth;
                _tables.Add(mapping.TableName);
                RecordReferencedTable(mapping.TableName);
                var plan = Translate(e);
                resultingMapping = _mapping;
                return plan;
            }
            finally
            {
                var subClauses = _clauses;
                var contextToRestore = _contextStack.Pop();
                RestoreContext(contextToRestore);
                _selfRootAlias = outerSelfRootAlias;
                subClauses.Dispose();
            }
        }
        private readonly struct TranslationContextSnapshot
        {
            public TranslationContextSnapshot(
                SqlBuilder clauses,
                List<IncludePlan> includes,
                LambdaExpression? projection,
                bool isAggregate,
                string methodName,
                GroupJoinInfo? groupJoinInfo,
                LambdaExpression? groupJoinExpansionSelector,
                int joinCounter,
                bool singleResult,
                bool noTracking,
                bool splitQuery,
                TableMapping mapping,
                Type? rootType,
                TimeSpan estimatedTimeout,
                bool isCacheable,
                TimeSpan? cacheExpiration,
                DateTime? asOfTimestamp,
                int recursionDepth,
                Func<object, object>? clientProjection,
                Type? clientProjectionResultType,
                LambdaExpression? groupJoinResultSelector,
                LambdaExpression? groupByElementSelector,
                LambdaExpression? streamingGroupByKeySelector,
                Func<DbContext, System.Collections.IList, System.Collections.IList>? postMaterializeTransform,
                Type? postMaterializeElementType,
                Func<DbContext, System.Collections.IList, System.Collections.IList>? postMaterializeOrderPrefixTransform,
                List<(Func<object, object> KeyReader, bool Ascending)>? postMaterializeOrderings,
                bool postReverseResult,
                bool clientScalarResult)
            {
                Clauses = clauses;
                Includes = includes;
                Projection = projection;
                IsAggregate = isAggregate;
                MethodName = methodName;
                GroupJoinInfo = groupJoinInfo;
                GroupJoinExpansionSelector = groupJoinExpansionSelector;
                JoinCounter = joinCounter;
                SingleResult = singleResult;
                NoTracking = noTracking;
                SplitQuery = splitQuery;
                Mapping = mapping;
                RootType = rootType;
                EstimatedTimeout = estimatedTimeout;
                IsCacheable = isCacheable;
                CacheExpiration = cacheExpiration;
                AsOfTimestamp = asOfTimestamp;
                RecursionDepth = recursionDepth;
                ClientProjection = clientProjection;
                ClientProjectionResultType = clientProjectionResultType;
                GroupJoinResultSelector = groupJoinResultSelector;
                GroupByElementSelector = groupByElementSelector;
                StreamingGroupByKeySelector = streamingGroupByKeySelector;
                PostMaterializeTransform = postMaterializeTransform;
                PostMaterializeElementType = postMaterializeElementType;
                PostMaterializeOrderPrefixTransform = postMaterializeOrderPrefixTransform;
                PostMaterializeOrderings = postMaterializeOrderings;
                PostReverseResult = postReverseResult;
                ClientScalarResult = clientScalarResult;
            }
            public SqlBuilder Clauses { get; }
            public List<IncludePlan> Includes { get; }
            public LambdaExpression? Projection { get; }
            public bool IsAggregate { get; }
            public string MethodName { get; }
            public GroupJoinInfo? GroupJoinInfo { get; }
            public LambdaExpression? GroupJoinExpansionSelector { get; }
            public int JoinCounter { get; }
            public bool SingleResult { get; }
            public bool NoTracking { get; }
            public bool SplitQuery { get; }
            public TableMapping Mapping { get; }
            public Type? RootType { get; }
            public TimeSpan EstimatedTimeout { get; }
            public bool IsCacheable { get; }
            public TimeSpan? CacheExpiration { get; }
            public DateTime? AsOfTimestamp { get; }
            public int RecursionDepth { get; }
            // Client-tail / post-materialize state. Omitting these let a reshape or
            // client projection on the outer query leak into (or be lost across) the
            // sub-context translation of a set-op arm or nested subquery.
            public Func<object, object>? ClientProjection { get; }
            public Type? ClientProjectionResultType { get; }
            public LambdaExpression? GroupJoinResultSelector { get; }
            public LambdaExpression? GroupByElementSelector { get; }
            public LambdaExpression? StreamingGroupByKeySelector { get; }
            public Func<DbContext, System.Collections.IList, System.Collections.IList>? PostMaterializeTransform { get; }
            public Type? PostMaterializeElementType { get; }
            public Func<DbContext, System.Collections.IList, System.Collections.IList>? PostMaterializeOrderPrefixTransform { get; }
            public List<(Func<object, object> KeyReader, bool Ascending)>? PostMaterializeOrderings { get; }
            public bool PostReverseResult { get; }
            public bool ClientScalarResult { get; }
        }
    }
}
