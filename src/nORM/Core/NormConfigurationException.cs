using System;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when nORM configuration is invalid or incomplete.
    /// </summary>
    public class NormConfigurationException : NormException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="NormConfigurationException"/>
        /// class with a specified error message and optional inner exception.
        /// </summary>
        /// <param name="message">The error message that explains the reason for the exception.</param>
        /// <param name="inner">The exception that is the cause of the current exception.</param>
        public NormConfigurationException(string message, Exception? inner = null)
            : base(message, null, null, inner)
        {
        }
    }
}
