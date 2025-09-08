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

        public void AppendLiteral(ReadOnlySpan<char> text)
        {
            text.CopyTo(_buffer.Slice(_position));
            _position += text.Length;
        }

        public void AppendParameter(ReadOnlySpan<char> paramName)
        {
            paramName.CopyTo(_buffer.Slice(_position));
            _position += paramName.Length;
        }
    }
}
