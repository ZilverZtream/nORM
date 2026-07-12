using System;
using System.Collections.Generic;

namespace nORM.Query
{
    /// <summary>
    /// Container for the various SQL clause builders used by <see cref="QueryTranslator"/>.
    /// Renamed from <c>SqlClauseBuilder</c> to emphasize its role as a focused
    /// SQL construction utility.
    /// </summary>
    internal sealed class SqlBuilder : IDisposable
    {
        public OptimizedSqlBuilder Sql { get; } = new();
        public OptimizedSqlBuilder Where { get; } = new();
        public OptimizedSqlBuilder Having { get; } = new();
        public List<(string col, bool asc)> OrderBy { get; } = new();
        public List<string> GroupBy { get; } = new();
        // Extra grouping expressions appended ONLY to the emitted GROUP BY clause — never to the
        // SELECT-side key resolution that also reads GroupBy (RegisterGroupingKey). Used for
        // ordinal string grouping on CI-collation providers: GROUP BY key, BINARY key groups
        // byte-wise while the projection keeps selecting the plain key (a selected BINARY key
        // would materialize as raw bytes and shift the positional materializer).
        public List<string> GroupByOrdinalExtras { get; } = new();
        public List<WindowFunctionInfo> WindowFunctions { get; } = new();
        public int? Take { get; set; }
        public int? Skip { get; set; }
        public string? TakeParam { get; set; }
        public string? SkipParam { get; set; }
        // True when Take was set by a terminal operator (First/Single/Last/ElementAt)
        // rather than a user-facing .Take() / .Skip(). Lets the post-Take/Skip pin
        // family suppress false-positives on `q.OrderBy(k).First()`-style chains.
        public bool TakeSetByTerminal { get; set; }
        public bool IsDistinct { get; set; }

        /// <summary>
        /// Releases the clause builders and any resources they consume.
        /// </summary>
        public void Dispose()
        {
            Sql.Dispose();
            Where.Dispose();
            Having.Dispose();
        }
    }
}
