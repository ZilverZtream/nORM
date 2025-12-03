using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

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

        /// <summary>
        /// Creates a new <see cref="OptimizedSqlBuilder"/> with a default initial buffer
        /// capacity of 256 characters.
        /// </summary>
        public OptimizedSqlBuilder()
            : this(256)
        {
        }

        /// <summary>
        /// Creates a new <see cref="OptimizedSqlBuilder"/> with the specified initial
        /// buffer size.
        /// </summary>
        /// <param name="capacity">Number of characters to initially allocate.</param>
        public OptimizedSqlBuilder(int capacity)
        {
            _buffer = ArrayPool<char>.Shared.Rent(capacity);
            _position = 0;
        }

        /// <summary>
        /// Gets the number of characters currently written to the underlying buffer.
        /// </summary>
        public int Length => _position;

        /// <summary>
        /// Appends a string to the builder if the value is not <c>null</c>.
        /// </summary>
        /// <param name="value">The string to append.</param>
        /// <returns>The current builder instance.</returns>
        // PERFORMANCE OPTIMIZATION 16: Aggressive inlining for SQL builder hot path
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public OptimizedSqlBuilder Append(string? value)
        {
            if (value != null)
                Append(value.AsSpan());
            return this;
        }

        /// <summary>
        /// Appends the specified span of characters to the builder.
        /// </summary>
        /// <param name="value">The characters to append.</param>
        /// <returns>The current builder instance.</returns>
        // PERFORMANCE OPTIMIZATION 17: Aggressive inlining + optimization for span append
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public OptimizedSqlBuilder Append(ReadOnlySpan<char> value)
        {
            int valueLength = value.Length;
            EnsureCapacity(valueLength);
            value.CopyTo(_buffer.AsSpan(_position));
            _position += valueLength;
            return this;
        }

        /// <summary>
        /// Appends a single character to the builder.
        /// </summary>
        /// <param name="value">The character to append.</param>
        /// <returns>The current builder instance.</returns>
        // PERFORMANCE OPTIMIZATION 18: Aggressive inlining for char append
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        public OptimizedSqlBuilder Append(char value)
        {
            EnsureCapacity(1);
            _buffer[_position++] = value;
            return this;
        }

        /// <summary>
        /// Appends a pre-constructed SQL fragment to the builder.
        /// </summary>
        /// <param name="value">The fragment to append.</param>
        /// <returns>The current builder instance.</returns>
        public OptimizedSqlBuilder AppendFragment(string value) => Append(value);

        /// <summary>
        /// Appends a <c>SELECT</c> keyword and the provided column list.
        /// </summary>
        /// <param name="columns">Columns to include in the <c>SELECT</c> clause.</param>
        /// <returns>The current builder instance.</returns>
        public OptimizedSqlBuilder AppendSelect(ReadOnlySpan<char> columns)
        {
            Append("SELECT ");
            if (!columns.IsEmpty)
                Append(columns);
            return this;
        }

        /// <summary>
        /// Appends a <c>GROUP BY</c> clause with the supplied columns.
        /// </summary>
        /// <param name="columns">The column list for the <c>GROUP BY</c> clause.</param>
        /// <returns>The current builder instance.</returns>
        public OptimizedSqlBuilder AppendGroupBy(ReadOnlySpan<char> columns)
        {
            if (!columns.IsEmpty)
            {
                Append(" GROUP BY ");
                Append(columns);
            }
            return this;
        }

        /// <summary>
        /// Appends a SQL aggregate function invocation such as <c>COUNT(column)</c>.
        /// </summary>
        /// <param name="function">Name of the aggregate function.</param>
        /// <param name="column">Column to aggregate.</param>
        /// <returns>The current builder instance.</returns>
        public OptimizedSqlBuilder AppendAggregateFunction(ReadOnlySpan<char> function, ReadOnlySpan<char> column)
        {
            Append(function);
            Append('(');
            Append(column);
            Append(')');
            return this;
        }

        /// <summary>
        /// Appends a parameter placeholder and stores its value in the supplied
        /// dictionary for later use when executing a command.
        /// </summary>
        /// <param name="paramName">The name of the parameter including prefix.</param>
        /// <param name="value">The value to associate with the parameter.</param>
        /// <param name="parameters">Dictionary that collects parameter values.</param>
        /// <returns>The current builder instance.</returns>
        public OptimizedSqlBuilder AppendParameterizedValue(string paramName, object? value, IDictionary<string, object> parameters)
        {
            Append(paramName.AsSpan());
            parameters[paramName] = value!;
            return this;
        }

        /// <summary>
        /// Appends a collection of values separated by the specified separator string.
        /// </summary>
        /// <param name="separator">Separator to insert between values.</param>
        /// <param name="values">Values to append.</param>
        /// <returns>The current builder instance.</returns>
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

        /// <summary>
        /// Removes a range of characters from the builder.
        /// </summary>
        /// <param name="startIndex">The starting index of the range to remove.</param>
        /// <param name="length">The number of characters to remove.</param>
        /// <returns>The current builder instance.</returns>
        public OptimizedSqlBuilder Remove(int startIndex, int length)
        {
            _buffer.AsSpan(startIndex + length, _position - (startIndex + length))
                .CopyTo(_buffer.AsSpan(startIndex));
            _position -= length;
            return this;
        }

        /// <summary>
        /// Inserts the specified string at the given index within the builder.
        /// </summary>
        /// <param name="index">Position at which to insert the string.</param>
        /// <param name="value">String to insert.</param>
        /// <returns>The current builder instance.</returns>
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

        /// <summary>
        /// Ensures that the internal buffer has enough space to accommodate the specified
        /// number of additional characters, resizing and copying the buffer when necessary.
        /// </summary>
        /// <param name="additional">The number of characters that need to be appended.</param>
        // PERFORMANCE OPTIMIZATION 19: Aggressive inlining + optimized capacity check
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        private void EnsureCapacity(int additional)
        {
            int required = _position + additional;
            int bufferLength = _buffer.Length;

            // PERFORMANCE OPTIMIZATION 20: Hoist buffer length check for better branch prediction
            if (required <= bufferLength)
                return;

            // Grow by 2x or required, whichever is larger
            int newSize = Math.Max(bufferLength * 2, required);
            var newBuffer = ArrayPool<char>.Shared.Rent(newSize);
            _buffer.AsSpan(0, _position).CopyTo(newBuffer);
            ArrayPool<char>.Shared.Return(_buffer);
            _buffer = newBuffer;
        }


        /// <summary>
        /// Reserves additional capacity in the underlying buffer to reduce future
        /// reallocations when appending large amounts of data.
        /// </summary>
        /// <param name="additionalCapacity">The number of extra characters to reserve space for.</param>
        /// <returns>The current builder instance.</returns>
        public OptimizedSqlBuilder Reserve(int additionalCapacity)
        {
            EnsureCapacity(additionalCapacity);
            return this;
        }
    }
}
