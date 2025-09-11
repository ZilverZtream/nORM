using System;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when a requested feature is not supported by nORM.
    /// </summary>
    public class NormUnsupportedFeatureException : NormException
    {
        /// <summary>
        /// Initializes a new instance of <see cref="NormUnsupportedFeatureException"/>.
        /// </summary>
        /// <param name="message">Description of the unsupported feature.</param>
        /// <param name="inner">Optional inner exception.</param>
        public NormUnsupportedFeatureException(string message, Exception? inner = null)
            : base(message, null, null, inner)
        {
        }
    }
}
