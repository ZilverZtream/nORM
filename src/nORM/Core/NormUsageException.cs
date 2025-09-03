using System;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when the nORM API is used incorrectly.
    /// </summary>
    public class NormUsageException : NormException
    {
        public NormUsageException(string message, Exception? inner = null)
            : base(message, null, null, inner)
        {
        }
    }
}
