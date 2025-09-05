using System;
using System.Collections.Generic;

namespace nORM.Query
{
    /// <summary>
    /// Container for the various SQL clause builders used by <see cref="QueryTranslator"/>.
    /// </summary>
    internal sealed class SqlClauseBuilder : IDisposable
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

        public void Dispose()
        {
            var exceptions = new List<Exception>();

            try
            {
                Sql.Dispose();
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }

            try
            {
                Where.Dispose();
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }

            try
            {
                Having.Dispose();
            }
            catch (Exception ex)
            {
                exceptions.Add(ex);
            }

            if (exceptions.Count > 0)
            {
                throw new AggregateException("Disposal failures", exceptions);
            }
        }
    }
}
