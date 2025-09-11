using System;

namespace nORM.Core
{
    /// <summary>
    /// Represents errors that occur when establishing or managing database connections.
    /// </summary>
    public class NormConnectionException : NormException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NormConnectionException"/>
        /// class with a specified error message and optional inner exception.
        /// </summary>
        /// <param name="message">Message describing the connection error.</param>
        /// <param name="innerException">The exception that caused this error, if any.</param>
        public NormConnectionException(string message, Exception? innerException = null)
            : base(message, null, null, innerException)
        {
        }
    }
}
