using System;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when the nORM API is used incorrectly.
    /// </summary>
    public class NormUsageException : NormException
    {
        /// <summary>
        /// Initializes a new instance of <see cref="NormUsageException"/>.
        /// </summary>
        /// <param name="message">Description of the incorrect usage.</param>
        /// <param name="inner">Optional inner exception.</param>
        public NormUsageException(string message, Exception? inner = null)
            : base(message, null, null, inner)
        {
        }
    }
}
