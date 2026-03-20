using System;
using System.Data.Common;

namespace nORM.Query
{
    internal ref struct StackAllocParameterBuilder
    {
        private readonly Span<DbParameter> _parameters;
        private readonly Span<string> _names;
        private int _count;

        public StackAllocParameterBuilder(Span<DbParameter> parameters, Span<string> names)
        {
            _parameters = parameters;
            _names = names;
            _count = 0;
        }

        /// <summary>
        /// Gets the number of parameters added so far.
        /// </summary>
        public int Count => _count;

        /// <summary>
        /// Adds a new parameter to the preallocated spans, avoiding additional allocations.
        /// </summary>
        /// <param name="name">Parameter name.</param>
        /// <param name="value">Parameter value.</param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when attempting to add more parameters than the preallocated capacity.
        /// </exception>
        public void Add(ReadOnlySpan<char> name, object value)
        {
            if (_count >= _parameters.Length)
                throw new InvalidOperationException(
                    $"StackAllocParameterBuilder capacity exceeded: cannot add parameter at index {_count}, capacity is {_parameters.Length}.");
            var nameStr = name.ToString();
            var param = _parameters[_count];
            param.ParameterName = nameStr;
            _names[_count] = nameStr;
            ParameterAssign.AssignValue(param, value);
            _count++;
        }
    }
}
