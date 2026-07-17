using System;
using System.Linq;
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
        /// Opens the ambient temporal window when an AsOf timestamp is parsed. The
        /// timestamp parameter registers ONCE on the parsing translator (it is a
        /// fixed constant of the plan — the timestamp participates in the plan
        /// fingerprint); every table reference rendered while the scope is active
        /// reads through the window. Two different timestamps in one statement
        /// fail loud instead of silently mixing eras.
        /// </summary>
        private void BeginTemporalTableSourceScope()
        {
            var ts = _asOfTimestamp!.Value;
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

        /// <summary>
        /// The FROM/JOIN source for a mapped read: the live escaped table normally,
        /// the AsOf history-window derived table while a temporal scope is active.
        /// </summary>
        internal static string TemporalTableSource(TableMapping map)
            => t_temporalScope?.TableSourceFor(map) ?? map.EscTable;

        /// <summary>Whether an AsOf window is active for the current translation.</summary>
        internal static bool HasActiveTemporalScope => t_temporalScope != null;
    }
}
