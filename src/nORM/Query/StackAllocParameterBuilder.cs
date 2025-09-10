using System;
using System.Data.Common;

namespace nORM.Query
{
    internal ref struct StackAllocParameterBuilder
    {
        private Span<DbParameter> _parameters;
        private Span<string> _names;
        private int _count;

        public StackAllocParameterBuilder(Span<DbParameter> parameters, Span<string> names)
        {
            _parameters = parameters;
            _names = names;
            _count = 0;
        }

        /// <summary>
        /// Adds a new parameter to the preallocated spans, avoiding additional allocations.
        /// </summary>
        /// <param name="name">Parameter name.</param>
        /// <param name="value">Parameter value.</param>
        public void Add(ReadOnlySpan<char> name, object value)
        {
            var param = _parameters[_count];
            param.ParameterName = name.ToString();
            ParameterAssign.AssignValue(param, value);
            _count++;
        }
    }
}
