using System;
using System.Collections.Generic;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when a query fails to execute or produces an error result.
    /// </summary>
    public class NormQueryException : NormException
    {
        /// <summary>
        /// Initializes a new <see cref="NormQueryException"/>.
        /// </summary>
        /// <param name="message">Human-readable error message.</param>
        /// <param name="sql">Offending SQL statement, if available.</param>
        /// <param name="parameters">Parameters used in the statement.</param>
        /// <param name="inner">Underlying exception that triggered this error.</param>
        public NormQueryException(string message, string? sql = null,
            IReadOnlyDictionary<string, object>? parameters = null, Exception? inner = null)
            : base(message, sql, parameters, inner)
        {
        }
    }
}
