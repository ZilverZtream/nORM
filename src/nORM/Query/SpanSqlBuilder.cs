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
        public void AppendLiteral(ReadOnlySpan<char> text)
        {
            text.CopyTo(_buffer.Slice(_position));
            _position += text.Length;
        }

        /// <summary>
        /// Appends a parameter name to the buffer without any additional formatting.
        /// </summary>
        /// <param name="paramName">The parameter placeholder to append.</param>
        public void AppendParameter(ReadOnlySpan<char> paramName)
        {
            paramName.CopyTo(_buffer.Slice(_position));
            _position += paramName.Length;
        }
    }
}
