using System;
using System.Collections.Generic;
using System.Data.Common;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Base exception type for nORM that enriches <see cref="DbException"/> with SQL and parameter information.
    /// </summary>
    public class NormException : DbException
    {
        /// <summary>
        /// Gets the SQL statement that caused the exception, if available.
        /// </summary>
        public string? SqlStatement { get; }

        /// <summary>
        /// Gets the parameters used with the SQL statement, if any.
        /// </summary>
        public IReadOnlyDictionary<string, object>? Parameters { get; }

        /// <summary>
        /// Initializes a new instance of <see cref="NormException"/>.
        /// </summary>
        /// <param name="message">Human-readable error message.</param>
        /// <param name="sql">SQL statement associated with the error.</param>
        /// <param name="parameters">Parameters supplied with the SQL statement.</param>
        /// <param name="inner">The original exception that triggered this error.</param>
        public NormException(string message, string? sql, IReadOnlyDictionary<string, object>? parameters, Exception? inner = null)
            : base(message, inner)
        {
            SqlStatement = sql;
            Parameters = parameters;
        }
    }
}