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
        public List<WindowFunctionInfo> WindowFunctions { get; } = new();
        public int? Take { get; set; }
        public int? Skip { get; set; }
        public string? TakeParam { get; set; }
        public string? SkipParam { get; set; }
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
