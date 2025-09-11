using System.Data.Common;

#nullable enable

namespace nORM.Providers
{
    /// <summary>
    /// Abstraction for creating provider-specific <see cref="DbParameter"/> instances.
    /// </summary>
    public interface IDbParameterFactory
    {
        /// <summary>
        /// Creates a database parameter with the provided name and value.
        /// </summary>
        /// <param name="name">Parameter name including provider prefix.</param>
        /// <param name="value">Parameter value or <c>null</c>.</param>
        /// <returns>A configured <see cref="DbParameter"/>.</returns>
        DbParameter CreateParameter(string name, object? value);
    }
}
