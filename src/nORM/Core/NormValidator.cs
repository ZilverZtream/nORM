using System;
using System.Collections.Generic;

#nullable enable

namespace nORM.Core
{
    /// <summary>
    /// Provides runtime validation helpers for entities, bulk operations and raw SQL statements
    /// to guard against misuse and excessively large payloads.
    /// </summary>
    public static partial class NormValidator
    {
        private const int MaxCollectionSize = 10000;
        private const int MaxBulkOperationSize = 50000;
        private const int MaxParameterCount = 2000;
        private const int MaxSqlLength = 100000;
        private const int MaxConnectionStringLength = 8192;
        private const int MaxConnectionTimeoutSeconds = 300;

        /// <summary>
        /// Validates that a bulk operation contains a reasonable number of entities and none are null.
        /// </summary>
        /// <typeparam name="T">Type of the entities involved in the operation.</typeparam>
        /// <param name="entities">Collection of entities to validate.</param>
        /// <param name="operation">Name of the bulk operation (for error messages).</param>
        public static void ValidateBulkOperation<T>(IEnumerable<T> entities, string operation) where T : class
        {
            if (entities == null)
                throw new ArgumentNullException(nameof(entities));

            var count = 0;
            foreach (var entity in entities)
            {
                if (entity == null)
                    throw new ArgumentException($"Null entity found in {operation} operation at index {count}");

                if (++count > MaxBulkOperationSize)
                    throw new ArgumentException($"Bulk {operation} operation exceeds maximum size of {MaxBulkOperationSize}");
            }

            if (count == 0)
                throw new ArgumentException($"Bulk {operation} operation cannot be empty");
        }
    }
}
