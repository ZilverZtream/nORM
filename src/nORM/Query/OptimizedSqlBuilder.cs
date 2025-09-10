using System;
using System.Buffers;
using System.Collections.Generic;

namespace nORM.Query
{
    /// <summary>
    /// SQL builder that minimizes allocations by working directly with
    /// span-based operations over pooled character buffers.
    /// </summary>
    public sealed class OptimizedSqlBuilder : IDisposable
    {
        private char[] _buffer;
        private int _position;

        public OptimizedSqlBuilder()
            : this(256)
        {
        }

        public OptimizedSqlBuilder(int capacity)
        {
            _buffer = ArrayPool<char>.Shared.Rent(capacity);
            _position = 0;
        }

        public int Length => _position;

        public OptimizedSqlBuilder Append(string? value)
        {
            if (value != null)
                Append(value.AsSpan());
            return this;
        }

        public OptimizedSqlBuilder Append(ReadOnlySpan<char> value)
        {
            EnsureCapacity(value.Length);
            value.CopyTo(_buffer.AsSpan(_position));
            _position += value.Length;
            return this;
        }

        public OptimizedSqlBuilder Append(char value)
        {
            EnsureCapacity(1);
            _buffer[_position++] = value;
            return this;
        }

        public OptimizedSqlBuilder AppendFragment(string value) => Append(value);

        public OptimizedSqlBuilder AppendSelect(ReadOnlySpan<char> columns)
        {
            Append("SELECT ");
            if (!columns.IsEmpty)
                Append(columns);
            return this;
        }

        public OptimizedSqlBuilder AppendAggregateFunction(ReadOnlySpan<char> function, ReadOnlySpan<char> column)
        {
            Append(function);
            Append('(');
            Append(column);
            Append(')');
            return this;
        }

        public OptimizedSqlBuilder AppendParameterizedValue(string paramName, object? value, IDictionary<string, object> parameters)
        {
            Append(paramName.AsSpan());
            parameters[paramName] = value!;
            return this;
        }

        public OptimizedSqlBuilder AppendJoin(string separator, IEnumerable<string> values)
        {
            using var e = values.GetEnumerator();
            if (!e.MoveNext())
                return this;

            Append(e.Current);
            while (e.MoveNext())
            {
                Append(separator);
                Append(e.Current);
            }
            return this;
        }

        public OptimizedSqlBuilder Remove(int startIndex, int length)
        {
            _buffer.AsSpan(startIndex + length, _position - (startIndex + length))
                .CopyTo(_buffer.AsSpan(startIndex));
            _position -= length;
            return this;
        }

        public OptimizedSqlBuilder Insert(int index, string value)
        {
            var span = value.AsSpan();
            EnsureCapacity(span.Length);
            _buffer.AsSpan(index, _position - index).CopyTo(_buffer.AsSpan(index + span.Length));
            span.CopyTo(_buffer.AsSpan(index));
            _position += span.Length;
            return this;
        }

        public void Clear() => _position = 0;

        public string ToSqlString() => new string(_buffer, 0, _position);

        public string ToString(int startIndex, int length) => new string(_buffer, startIndex, length);

        public override string ToString() => ToSqlString();

        public void Dispose()
        {
            var buffer = _buffer;
            _buffer = Array.Empty<char>();
            ArrayPool<char>.Shared.Return(buffer);
        }

        private void EnsureCapacity(int additional)
        {
            var required = _position + additional;
            if (required <= _buffer.Length)
                return;

            var newSize = Math.Max(_buffer.Length * 2, required);
            var newBuffer = ArrayPool<char>.Shared.Rent(newSize);
            _buffer.AsSpan(0, _position).CopyTo(newBuffer);
            ArrayPool<char>.Shared.Return(_buffer);
            _buffer = newBuffer;
        }


        public OptimizedSqlBuilder Reserve(int additionalCapacity)
        {
            EnsureCapacity(additionalCapacity);
            return this;
        }
    }
}
