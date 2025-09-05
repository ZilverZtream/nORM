using System;

namespace nORM.SourceGeneration
{
    /// <summary>
    /// Indicates that a method should be populated by the source generator with
    /// a pre-compiled query using the provided SQL statement.
    /// </summary>
    [AttributeUsage(AttributeTargets.Method, Inherited = false, AllowMultiple = false)]
    public sealed class CompileTimeQueryAttribute : Attribute
    {
        /// <summary>The SQL query to execute at runtime.</summary>
        public string Sql { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="CompileTimeQueryAttribute"/> class
        /// with the SQL query to execute.
        /// </summary>
        /// <param name="sql">The SQL command text.</param>
        public CompileTimeQueryAttribute(string sql) => Sql = sql;
    }
}
