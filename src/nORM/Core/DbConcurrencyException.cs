using System;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when an optimistic concurrency conflict is detected.
    /// </summary>
    public class DbConcurrencyException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DbConcurrencyException"/> class.
        /// </summary>
        public DbConcurrencyException() { }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbConcurrencyException"/> class
        /// with a specified error <paramref name="message"/>.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public DbConcurrencyException(string? message) : base(message) { }

        /// <summary>
        /// Initializes a new instance of the <see cref="DbConcurrencyException"/> class
        /// with a specified error <paramref name="message"/> and a reference to the
        /// inner <paramref name="innerException"/> that is the cause of this exception.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="innerException">The exception that is the cause of the current exception.</param>
        public DbConcurrencyException(string? message, Exception? innerException) : base(message, innerException) { }
    }
}
