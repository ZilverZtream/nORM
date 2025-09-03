using System;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when a requested feature is not supported by nORM.
    /// </summary>
    public class NormUnsupportedFeatureException : NormException
    {
        public NormUnsupportedFeatureException(string message, Exception? inner = null)
            : base(message, null, null, inner)
        {
        }
    }
}
