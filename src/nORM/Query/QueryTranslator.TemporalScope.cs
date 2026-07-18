using System;
using System.Linq;
using System.Linq.Expressions;
using nORM.Configuration;
using nORM.Core;
using nORM.Mapping;
using nORM.Providers;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        // ── Temporal AsOf table-source substitution ─────────────────────────────
        //
        // AsOf must window EVERY mapped table the statement reads, not just the
        // root FROM: navigation-scalar projections and predicates, SelectMany
        // flattens, explicit joins, and correlated navigation subqueries all
        // reference additional tables inside the same statement. Each reference
        // reads through the same history window as the root, or historical roots
        // silently mix with live relations (wrong values AND wrong membership).
        // Ambient (thread-static) like the closure-ordinal and referenced-table
        // scopes: translation is fully synchronous, and nested sub-translations
        // deliberately share the top-level window.
        [ThreadStatic] private static TemporalTableSourceScope? t_temporalScope;

        private sealed class TemporalTableSourceScope
        {
            private readonly DatabaseProvider _provider;
            private readonly string _timeParamName;
            private readonly bool _providerNative;
            public readonly DateTime Timestamp;

            public TemporalTableSourceScope(DatabaseProvider provider, string timeParamName, bool providerNative, DateTime timestamp)
            {
                _provider = provider;
                _timeParamName = timeParamName;
                _providerNative = providerNative;
                Timestamp = timestamp;
            }

            public string TableSourceFor(TableMapping map)
            {
                if (_providerNative)
                    return _provider.GetProviderNativeTemporalAsOfFromClause(map, _timeParamName);

                var history = _provider.Escape(map.TableName + "_History");
                var cols = string.Join(", ", map.Columns.Select(c => c.EscCol));
                var w = _provider.Escape("__asofw");
                return $"(SELECT {cols} FROM {history} {w} WHERE {_timeParamName} >= {w}.{_provider.Escape("__ValidFrom")} AND {_timeParamName} < {w}.{_provider.Escape("__ValidTo")})";
            }
        }

        /// <summary>
        /// Opens the ambient temporal window for an AsOf timestamp. The timestamp
        /// parameter registers ONCE on the opening translator (it is a fixed constant
        /// of the plan — the timestamp participates in the plan fingerprint); every
        /// table reference rendered while the scope is active reads through the
        /// window. Two different timestamps in one statement fail loud instead of
        /// silently mixing eras.
        /// </summary>
        private void BeginTemporalTableSourceScope(DateTime ts)
        {
            var active = t_temporalScope;
            if (active != null)
            {
                if (active.Timestamp != ts)
                    throw new NormUnsupportedFeatureException(
                        "A query cannot combine two different AsOf timestamps in one statement.");
                return;
            }
            var timeParamName = _provider.ParamPrefix + "p" + _parameterManager.GetNextIndex();
            AddLiteralParameter(timeParamName, _provider.FormatTemporalAsOfParameterValue(ts));
            t_temporalScope = new TemporalTableSourceScope(
                _provider, timeParamName,
                _ctx.Options.TemporalStorageMode == TemporalStorageMode.ProviderNative, ts);
        }

        private static void EndTemporalTableSourceScope() => t_temporalScope = null;

        /// <summary>Local-to-Unspecified UTC normalization shared by the pre-scan and the AsOf translator.</summary>
        private static DateTime NormalizeAsOfTimestamp(DateTime dt)
            => DateTime.SpecifyKind(dt.Kind == DateTimeKind.Local ? dt.ToUniversalTime() : dt, DateTimeKind.Unspecified);

        // A tag resolved by the pre-scan is reused by the AsOf translator: resolving
        // the tag twice could observe a concurrent tag rewrite and produce two
        // different timestamps for one statement (a spurious mixed-timestamp throw).
        private (string Tag, DateTime Ts)? _asOfTagResolution;

        /// <summary>
        /// Opens the temporal window BEFORE any part of the tree translates when the
        /// tree contains an AsOf node. Translation order must not decide which arms
        /// of a composite statement (set operations, subquery sources) get windowed:
        /// with the scope open from the start, every mapped table in the statement
        /// reads the same era regardless of where the AsOf node sits.
        /// </summary>
        private void TryOpenTemporalScopeForTranslation(Expression root)
        {
            if (!_ctx.Options.IsTemporalVersioningEnabled || t_temporalScope != null)
                return;
            var finder = new AsOfNodeFinder();
            finder.Visit(root);
            if (finder.Found is not { } asOfNode)
                return;

            if (!TryGetConstantValue(asOfNode.Arguments[1], out var value))
                return; // the AsOf translator raises the canonical error for non-constant arguments
            DateTime ts;
            if (value is DateTime dt)
            {
                ts = NormalizeAsOfTimestamp(dt);
            }
            else if (value is string tagName)
            {
                ts = GetTimestampForTagAsync(tagName).GetAwaiter().GetResult();
                _asOfTagResolution = (tagName, ts);
            }
            else
            {
                return;
            }
            BeginTemporalTableSourceScope(ts);
            // The OUTERMOST plan carries the temporal semantics even when the AsOf
            // node sits inside a sub-translated arm (whose own flags are reset and
            // restored): the plan must record the timestamp (owned-collection loads,
            // the write-path guard) and must NOT track — a historical row sharing a
            // primary key with an already-tracked current entity would otherwise
            // alias to the tracked instance and silently return CURRENT state.
            _asOfTimestamp = ts;
            _noTracking = true;
        }

        private sealed class AsOfNodeFinder : ExpressionVisitor
        {
            public MethodCallExpression? Found;

            public override Expression? Visit(Expression? node)
                => Found != null ? node : base.Visit(node);

            protected override Expression VisitMethodCall(MethodCallExpression node)
            {
                if (node.Method.Name == nameof(TemporalExtensions.AsOf)
                    && node.Method.DeclaringType == typeof(TemporalExtensions))
                {
                    Found = node;
                    return node;
                }
                return base.VisitMethodCall(node);
            }

            // Closure constants can hold whole object graphs; AsOf only ever sits in
            // the query's method-call spine, so constants never need descending.
            protected override Expression VisitConstant(ConstantExpression node) => node;
        }

        /// <summary>
        /// The FROM/JOIN source for a mapped read: the live escaped table normally,
        /// the AsOf history-window derived table while a temporal scope is active.
        /// </summary>
        internal static string TemporalTableSource(TableMapping map)
            => t_temporalScope?.TableSourceFor(map) ?? map.EscTable;

        /// <summary>
        /// The root FROM for a scalar terminal that emits its own SELECT/FROM (direct aggregates, ALL): the
        /// <c>FromSqlRaw</c>/<c>FromSqlInterpolated</c> derived table when the query started from raw SQL,
        /// otherwise <see cref="TemporalTableSource"/> for the mapped table. Raw and temporal are mutually
        /// exclusive — AsOf is not a composable raw operator, so it fails loud before reaching here.
        /// </summary>
        private string RootTableSource()
            => _rawSqlSource != null ? "(" + _rawSqlSource + ")" : TemporalTableSource(_mapping);

        /// <summary>Whether an AsOf window is active for the current translation.</summary>
        internal static bool HasActiveTemporalScope => t_temporalScope != null;
    }
}
