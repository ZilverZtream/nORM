using System;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when an optimistic concurrency conflict is detected.
    /// </summary>
    public class DbConcurrencyException : Exception
    {
        public DbConcurrencyException() { }
        public DbConcurrencyException(string? message) : base(message) { }
        public DbConcurrencyException(string? message, Exception? innerException) : base(message, innerException) { }
    }
}
