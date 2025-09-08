using System;

namespace nORM.Providers
{
    public interface IFastProvider
    {
        void BuildSimpleSelect(Span<char> buffer, ReadOnlySpan<char> table,
            ReadOnlySpan<char> columns, out int length);
        char ParameterPrefixChar { get; }
    }
}
