using System.Collections.Generic;
using System.Data;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Represents an output, input/output, or return-value parameter for stored procedure execution.
    /// </summary>
    /// <param name="Name">Name of the parameter without provider-specific prefix.</param>
    /// <param name="DbType">Database type of the parameter.</param>
    /// <param name="Size">Optional size for variable-length parameters.</param>
    /// <param name="Precision">Optional decimal precision for providers that require explicit output precision.</param>
    /// <param name="Scale">Optional decimal scale for providers that require explicit output scale.</param>
    /// <param name="Direction">Parameter direction. Only Output, InputOutput, and ReturnValue are valid.</param>
    /// <param name="Value">Optional initial value for input/output parameters.</param>
    public sealed record OutputParameter(
        string Name,
        DbType DbType,
        int? Size,
        byte? Precision,
        byte? Scale,
        ParameterDirection Direction,
        object? Value)
    {
        /// <summary>
        /// Creates an output parameter with no explicit size.
        /// </summary>
        /// <param name="name">Name of the parameter without provider-specific prefix.</param>
        /// <param name="dbType">Database type of the parameter.</param>
        public OutputParameter(string name, DbType dbType)
            : this(name, dbType, null, null, null, ParameterDirection.Output, null)
        {
        }

        /// <summary>
        /// Creates an output parameter with the default <see cref="ParameterDirection.Output"/> direction.
        /// </summary>
        /// <param name="name">Name of the parameter without provider-specific prefix.</param>
        /// <param name="dbType">Database type of the parameter.</param>
        /// <param name="size">Optional size for variable-length parameters.</param>
        public OutputParameter(string name, DbType dbType, int? size = null)
            : this(name, dbType, size, null, null, ParameterDirection.Output, null)
        {
        }

        /// <summary>
        /// Creates a decimal output parameter with explicit precision and scale.
        /// </summary>
        /// <param name="name">Name of the parameter without provider-specific prefix.</param>
        /// <param name="dbType">Database type of the parameter.</param>
        /// <param name="precision">Decimal precision.</param>
        /// <param name="scale">Decimal scale.</param>
        public OutputParameter(string name, DbType dbType, byte precision, byte scale)
            : this(name, dbType, null, precision, scale, ParameterDirection.Output, null)
        {
        }

        /// <summary>
        /// Creates a decimal output, input/output, or return-value parameter definition.
        /// </summary>
        /// <param name="name">Name of the parameter without provider-specific prefix.</param>
        /// <param name="dbType">Database type of the parameter.</param>
        /// <param name="precision">Decimal precision.</param>
        /// <param name="scale">Decimal scale.</param>
        /// <param name="direction">Parameter direction.</param>
        public OutputParameter(string name, DbType dbType, byte precision, byte scale, ParameterDirection direction)
            : this(name, dbType, null, precision, scale, direction, null)
        {
        }

        /// <summary>
        /// Creates an output, input/output, or return-value parameter definition.
        /// </summary>
        /// <param name="name">Name of the parameter without provider-specific prefix.</param>
        /// <param name="dbType">Database type of the parameter.</param>
        /// <param name="size">Optional size for variable-length parameters.</param>
        /// <param name="direction">Parameter direction.</param>
        public OutputParameter(string name, DbType dbType, int? size, ParameterDirection direction)
            : this(name, dbType, size, null, null, direction, null)
        {
        }

        /// <summary>
        /// Creates an output, input/output, or return-value parameter definition.
        /// </summary>
        /// <param name="name">Name of the parameter without provider-specific prefix.</param>
        /// <param name="dbType">Database type of the parameter.</param>
        /// <param name="size">Optional size for variable-length parameters.</param>
        /// <param name="direction">Parameter direction.</param>
        /// <param name="value">Optional initial value for input/output parameters.</param>
        public OutputParameter(string name, DbType dbType, int? size, ParameterDirection direction, object? value)
            : this(name, dbType, size, null, null, direction, value)
        {
        }

        /// <summary>
        /// Deconstructs the v1 output-parameter fields.
        /// </summary>
        public void Deconstruct(out string name, out DbType dbType, out int? size)
        {
            name = Name;
            dbType = DbType;
            size = Size;
        }

        /// <summary>
        /// Deconstructs the v1 output-parameter fields.
        /// </summary>
        public void Deconstruct(
            out string name,
            out DbType dbType,
            out int? size,
            out ParameterDirection direction,
            out object? value)
        {
            name = Name;
            dbType = DbType;
            size = Size;
            direction = Direction;
            value = Value;
        }
    }

    /// <summary>
    /// Encapsulates the results of a stored procedure that returns both a result set
    /// and output parameters.
    /// </summary>
    /// <typeparam name="T">Type of entities in the result set.</typeparam>
    /// <param name="Results">List of materialized entities returned by the procedure.</param>
    /// <param name="OutputParameters">Dictionary of output parameter values keyed by name.</param>
    public sealed record StoredProcedureResult<T>(List<T> Results, IReadOnlyDictionary<string, object?> OutputParameters);

    /// <summary>
    /// Encapsulates the result of a stored procedure that does not return a
    /// result set but does report affected rows and output parameters.
    /// </summary>
    /// <param name="AffectedRows">Provider-reported affected row count.</param>
    /// <param name="OutputParameters">Dictionary of output parameter values keyed by name.</param>
    public sealed record StoredProcedureNonQueryResult(int AffectedRows, IReadOnlyDictionary<string, object?> OutputParameters);
}
