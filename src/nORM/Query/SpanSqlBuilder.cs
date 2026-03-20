using System;

namespace nORM.Query
{
    internal ref struct SpanSqlBuilder
    {
        private Span<char> _buffer;
        private int _position;

        public SpanSqlBuilder(Span<char> buffer)
        {
            _buffer = buffer;
            _position = 0;
        }

        public int Position => _position;

        /// <summary>
        /// Appends a literal fragment directly to the underlying buffer.
        /// </summary>
        /// <param name="text">The text to append.</param>
        /// <exception cref="ArgumentException">
        /// Thrown when the text would overflow the remaining buffer capacity.
        /// </exception>
        public void AppendLiteral(ReadOnlySpan<char> text)
        {
            if (_position + text.Length > _buffer.Length)
                throw new ArgumentException(
                    $"Buffer overflow: cannot append {text.Length} chars at position {_position} into buffer of length {_buffer.Length}.");
            text.CopyTo(_buffer.Slice(_position));
            _position += text.Length;
        }

        /// <summary>
        /// Appends a parameter name to the buffer without any additional formatting.
        /// </summary>
        /// <param name="paramName">The parameter placeholder to append.</param>
        /// <exception cref="ArgumentException">
        /// Thrown when the parameter name would overflow the remaining buffer capacity.
        /// </exception>
        public void AppendParameter(ReadOnlySpan<char> paramName)
        {
            if (_position + paramName.Length > _buffer.Length)
                throw new ArgumentException(
                    $"Buffer overflow: cannot append {paramName.Length} chars at position {_position} into buffer of length {_buffer.Length}.");
            paramName.CopyTo(_buffer.Slice(_position));
            _position += paramName.Length;
        }
    }
}
