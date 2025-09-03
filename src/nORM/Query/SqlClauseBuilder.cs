using System.Collections.Generic;
using System.Text;

namespace nORM.Query
{
    /// <summary>
    /// Simple container for the various SQL clause builders used by <see cref="QueryTranslator"/>.
    /// </summary>
    internal sealed class SqlClauseBuilder
    {
        public StringBuilder Sql { get; } = new();
        public StringBuilder Where { get; } = new();
        public StringBuilder Having { get; } = new();
        public List<(string col, bool asc)> OrderBy { get; } = new();
        public List<string> GroupBy { get; } = new();
        public int? Take { get; set; }
        public int? Skip { get; set; }
        public bool IsDistinct { get; set; }
    }
}
