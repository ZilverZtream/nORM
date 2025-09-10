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

        /// <summary>
        /// Resets the builder, discarding all accumulated characters.
        /// </summary>
        public void Clear() => _position = 0;

        /// <summary>
        /// Creates a <see cref="string"/> from the current contents of the buffer.
        /// </summary>
        /// <returns>The SQL text represented by this builder.</returns>
        public string ToSqlString() => new string(_buffer, 0, _position);

        /// <summary>
        /// Converts a subset of the internal buffer to a <see cref="string"/>.
        /// </summary>
        /// <param name="startIndex">Index in the buffer at which to start.</param>
        /// <param name="length">Number of characters to include.</param>
        /// <returns>The specified substring.</returns>
        public string ToString(int startIndex, int length) => new string(_buffer, startIndex, length);

        /// <summary>
        /// Returns the complete SQL string represented by the builder.
        /// </summary>
        public override string ToString() => ToSqlString();

        /// <summary>
        /// Returns the rented buffer to the pool.
        /// </summary>
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
