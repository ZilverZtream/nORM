using System;

namespace nORM.Query
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public sealed class SqlFunctionAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SqlFunctionAttribute"/> class
        /// with the SQL format string used to translate the decorated method call.
        /// </summary>
        /// <param name="format">
        /// A composite format string representing the SQL fragment to emit. Parameters
        /// specified in the method invocation will be substituted into this format.
        /// </param>
        public SqlFunctionAttribute(string format)
        {
            Format = format;
        }

        /// <summary>
        /// Gets the SQL format string that describes how the attributed method should be
        /// represented in the generated SQL statement.
        /// </summary>
        public string Format { get; }
    }
}
