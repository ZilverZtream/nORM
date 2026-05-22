using System.Collections.Generic;
using System.Data;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Represents an output parameter for stored procedure execution.
    /// </summary>
    /// <param name="Name">Name of the parameter without provider-specific prefix.</param>
    /// <param name="DbType">Database type of the output parameter.</param>
    /// <param name="Size">Optional size for variable-length parameters.</param>
    public sealed record OutputParameter(string Name, DbType DbType, int? Size = null);

    /// <summary>
    /// Encapsulates the results of a stored procedure that returns both a result set
    /// and output parameters.
    /// </summary>
    /// <typeparam name="T">Type of entities in the result set.</typeparam>
    /// <param name="Results">List of materialized entities returned by the procedure.</param>
    /// <param name="OutputParameters">Dictionary of output parameter values keyed by name.</param>
    public sealed record StoredProcedureResult<T>(List<T> Results, IReadOnlyDictionary<string, object?> OutputParameters);
}
