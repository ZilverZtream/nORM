using System;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Provides helper methods for working with JSON values in queries.
    /// These methods act as markers for the query translator and are not intended to be executed directly.
    /// </summary>
    public static class Json
    {
        /// <summary>
        /// Represents a value extracted from a JSON path. Throws if used outside of a nORM query.
        /// </summary>
        public static T Value<T>(string column, string jsonPath)
        {
            throw new InvalidOperationException("This method is for use in nORM LINQ queries only.");
        }
    }
}
