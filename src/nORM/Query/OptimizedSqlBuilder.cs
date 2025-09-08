using System;
using System.Text;

namespace nORM.Query
{
    /// <summary>
    /// Simplified SQL builder that wraps a <see cref="StringBuilder"/> and
    /// exposes convenience methods used throughout the query translation
    /// pipeline.
    /// </summary>
    internal sealed class OptimizedSqlBuilder : IDisposable
    {
        private readonly StringBuilder _builder;

        public OptimizedSqlBuilder()
        {
            _builder = new StringBuilder();
        }

        public OptimizedSqlBuilder(int capacity)
        {
            _builder = new StringBuilder(capacity);
        }

        public OptimizedSqlBuilder(StringBuilder builder)
        {
            _builder = builder ?? new StringBuilder();
        }

        public StringBuilder InnerBuilder => _builder;

        public int Length => _builder.Length;

        public OptimizedSqlBuilder Append(string? value)
        {
            _builder.Append(value);
            return this;
        }

        public OptimizedSqlBuilder Append(ReadOnlySpan<char> value)
        {
            _builder.Append(value);
            return this;
        }

        public OptimizedSqlBuilder Append(char value)
        {
            _builder.Append(value);
            return this;
        }

        public OptimizedSqlBuilder AppendFragment(string value) => Append(value);

        public OptimizedSqlBuilder AppendSelect(ReadOnlySpan<char> columns)
        {
            _builder.Append("SELECT ");
            if (!columns.IsEmpty)
                _builder.Append(columns);
            return this;
        }

        public OptimizedSqlBuilder AppendAggregateFunction(string function, string column)
        {
            _builder.Append(function).Append('(').Append(column).Append(')');
            return this;
        }

        public OptimizedSqlBuilder AppendParameterizedValue(string paramName, object? value, System.Collections.Generic.IDictionary<string, object> parameters)
        {
            _builder.Append(paramName);
            parameters[paramName] = value!;
            return this;
        }

        public OptimizedSqlBuilder Remove(int startIndex, int length)
        {
            _builder.Remove(startIndex, length);
            return this;
        }

        public OptimizedSqlBuilder Insert(int index, string value)
        {
            _builder.Insert(index, value);
            return this;
        }

        public void Clear() => _builder.Clear();

        public string ToSqlString() => _builder.ToString();

        public string ToString(int startIndex, int length) => _builder.ToString(startIndex, length);

        public override string ToString() => _builder.ToString();

        public void Dispose()
        {
            // No resources to release; method included for usage compatibility
        }
    }
}

