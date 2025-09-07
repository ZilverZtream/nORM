using System;

namespace nORM.Query
{
    /// <summary>
    /// Span-based SQL builder for zero-allocation query construction.
    /// </summary>
    internal ref struct SpanSqlBuilder
    {
        private Span<char> _buffer;
        private int _position;

        public SpanSqlBuilder(Span<char> buffer)
        {
            _buffer = buffer;
            _position = 0;
        }

        /// <summary>Append text to the builder.</summary>
        public void Append(ReadOnlySpan<char> text)
        {
            text.CopyTo(_buffer.Slice(_position));
            _position += text.Length;
        }

        /// <summary>Returns the built SQL as a string.</summary>
        public override string ToString()
            => new string(_buffer.Slice(0, _position));
    }
}
