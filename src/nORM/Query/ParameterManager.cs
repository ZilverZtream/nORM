using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;

namespace nORM.Query
{
    /// <summary>
    /// Manages SQL parameters, compiled parameter names and mapping between
    /// expression parameters and generated SQL parameter placeholders.
    /// Extracted from <see cref="QueryTranslator"/> to comply with the
    /// single responsibility principle.
    /// </summary>
    internal sealed class ParameterManager
    {
        public Dictionary<string, object> Parameters { get; set; } = new();
        public List<string> CompiledParameters { get; set; } = new();
        public Dictionary<ParameterExpression, string> ParameterMap { get; set; } = new();

        private int _index;

        public int Index
        {
            get => Volatile.Read(ref _index);
            set => Volatile.Write(ref _index, value);
        }

        public int GetNextIndex() => Interlocked.Increment(ref _index) - 1;

        public void Reset()
        {
            Parameters = new Dictionary<string, object>();
            CompiledParameters = new List<string>();
            ParameterMap = new Dictionary<ParameterExpression, string>();
            Volatile.Write(ref _index, 0);
        }
    }
}
