using System;
using System.Collections.Generic;
using System.Linq.Expressions;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class QueryTranslator
    {
        // ── Closure-occurrence ordinals ─────────────────────────────────────────
        //
        // The per-execution ParameterValueExtractor produces one value per closure
        // MemberExpression in expression DOCUMENT ORDER. Compiled-parameter slots,
        // however, register in TRANSLATION order — and the projection renders at
        // plan-Build time, after every clause-translated slot, so a closure that a
        // clause re-expands (an ORDER BY key over a projected computed member) can
        // mint out of document order, or mint TWICE for one tree occurrence.
        //
        // The pre-pass below numbers every closure occurrence in document order
        // (mirroring the extractor's walk exactly). Mint sites consult the ambient
        // state to (a) REUSE the slot already minted for the same tree node instead
        // of minting a duplicate, and (b) record which ordinal a slot binds so the
        // binder can pair by ordinal. Slots without a recorded ordinal consume the
        // unclaimed ordinals in ascending order — exactly the legacy positional
        // stream. Ambient (thread-static) state avoids threading the maps through
        // every sub-translator and pooled-visitor signature; translation is fully
        // synchronous, and nested sub-translations deliberately share the
        // top-level translation's maps.

        [ThreadStatic] private static Dictionary<Expression, int>? t_closureOrdinals;
        [ThreadStatic] private static Dictionary<Expression, string>? t_closureSlots;
        [ThreadStatic] private static Dictionary<string, int>? t_slotOrdinals;

        // ── Referenced-table accumulation ───────────────────────────────────────
        //
        // The result-cache tags a Cacheable query with every table it reads so a
        // write to ANY of them invalidates the entry. A correlated subquery is
        // translated by a SEPARATE sub-translator whose local _tables set is
        // discarded, so its table (e.g. the child queried by a projection subquery)
        // never reached the outer plan's Tables — a write to that table then left a
        // stale cached result. This ambient set, shared across the top-level
        // translation and every nested sub-translation (like the closure ordinals
        // above), collects every table any translator touches; the plan merges it.
        [ThreadStatic] private static HashSet<string>? t_referencedTables;

        /// <summary>
        /// Starts referenced-table accumulation for a top-level translation. Returns
        /// false when already ambient (a nested sub-translation must not end the scope).
        /// </summary>
        private static bool BeginReferencedTableScope()
        {
            if (t_referencedTables != null)
                return false;
            t_referencedTables = new HashSet<string>(StringComparer.Ordinal);
            return true;
        }

        private static void EndReferencedTableScope() => t_referencedTables = null;

        /// <summary>Records a table read during translation into the ambient scope, if active.</summary>
        internal static void RecordReferencedTable(string tableName) => t_referencedTables?.Add(tableName);

        /// <summary>The tables accumulated for the current translation, or null when no scope is active.</summary>
        internal static IReadOnlyCollection<string>? CurrentReferencedTables => t_referencedTables;

        /// <summary>
        /// Starts ordinal tracking for a top-level translation. Returns false when a
        /// translation is already ambient (a nested sub-translation), in which case
        /// the caller must not end the scope.
        /// </summary>
        private static bool BeginClosureOrdinalScope(Expression root)
        {
            if (t_closureOrdinals != null)
                return false;

            var ordinals = new Dictionary<Expression, int>(ReferenceEqualityComparer.Instance);
            new ClosureOccurrenceNumberer(ordinals).Visit(root);
            t_closureOrdinals = ordinals;
            t_closureSlots = new Dictionary<Expression, string>(ReferenceEqualityComparer.Instance);
            t_slotOrdinals = new Dictionary<string, int>(StringComparer.Ordinal);
            return true;
        }

        private static void EndClosureOrdinalScope()
        {
            t_closureOrdinals = null;
            t_closureSlots = null;
            t_slotOrdinals = null;
        }

        /// <summary>
        /// Returns the compiled-parameter name already minted for this exact closure
        /// node, or null. Re-rendering the same node (ORDER BY expansion plus the
        /// Build-time projection render) must share one slot: the extractor produces
        /// exactly one value per tree occurrence.
        /// </summary>
        internal static string? TryReuseClosureSlot(Expression node)
            => t_closureSlots != null && t_closureSlots.TryGetValue(node, out var name) ? name : null;

        /// <summary>
        /// Records the slot minted for a closure node and, when the node belongs to
        /// the translated tree, the document ordinal that slot binds.
        /// </summary>
        internal static void RecordClosureSlot(Expression node, string parameterName)
        {
            if (t_closureSlots == null)
                return;
            t_closureSlots.TryAdd(node, parameterName);
            if (t_closureOrdinals!.TryGetValue(node, out var ordinal))
                t_slotOrdinals!.TryAdd(parameterName, ordinal);
        }

        /// <summary>
        /// Snapshot of the name → document-ordinal records for the given plan's
        /// compiled parameters. Null when nothing was recorded.
        /// </summary>
        private static IReadOnlyDictionary<string, int>? SnapshotSlotOrdinals(List<string> compiledParams)
        {
            var recorded = t_slotOrdinals;
            if (recorded == null || recorded.Count == 0)
                return null;

            Dictionary<string, int>? result = null;
            foreach (var name in compiledParams)
            {
                if (recorded.TryGetValue(name, out var ordinal))
                    (result ??= new Dictionary<string, int>(StringComparer.Ordinal))[name] = ordinal;
            }
            return result;
        }

        /// <summary>
        /// Numbers closure occurrences in document order with EXACTLY the walk the
        /// ParameterValueExtractor uses: constants are skipped wholesale, and a
        /// closure member match does not descend into its children.
        /// </summary>
        [System.Diagnostics.CodeAnalysis.RequiresDynamicCode("Closure lifting evaluates expression trees at runtime; not NativeAOT-compatible. See docs/aot-trimming.md.")]
        [System.Diagnostics.CodeAnalysis.RequiresUnreferencedCode("Closure lifting reflects over closure members; trimming may remove the required members. See docs/aot-trimming.md.")]
        private sealed class ClosureOccurrenceNumberer : ExpressionVisitor
        {
            private readonly Dictionary<Expression, int> _ordinals;
            private int _next;

            public ClosureOccurrenceNumberer(Dictionary<Expression, int> ordinals) => _ordinals = ordinals;

            protected override Expression VisitConstant(ConstantExpression node) => node;

            protected override Expression VisitMember(MemberExpression node)
            {
                if (TryGetConstantValue(node, out _))
                {
                    _ordinals[node] = _next++;
                    return node;
                }
                return base.VisitMember(node);
            }
        }
    }
}
