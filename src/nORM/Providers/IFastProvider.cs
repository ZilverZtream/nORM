using System;

namespace nORM.Providers
{
    /// <summary>
    /// Interface for lightweight providers capable of constructing simple SQL
    /// statements without allocations.
    /// </summary>
    public interface IFastProvider
    {
        /// <summary>
        /// Builds a basic <c>SELECT</c> statement targeting the specified table and
        /// column list directly into the provided buffer.
        /// </summary>
        /// <param name="buffer">Character buffer to receive the SQL.</param>
        /// <param name="table">Table name segment.</param>
        /// <param name="columns">Comma separated column list.</param>
        /// <param name="length">Outputs the length of the written SQL.</param>
        void BuildSimpleSelect(Span<char> buffer, ReadOnlySpan<char> table,
            ReadOnlySpan<char> columns, out int length);

        /// <summary>
        /// Gets the parameter prefix character used by the provider.
        /// </summary>
        char ParameterPrefixChar { get; }
    }
}
