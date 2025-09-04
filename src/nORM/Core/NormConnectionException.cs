using System;

namespace nORM.Core
{
    /// <summary>
    /// Represents errors that occur when establishing or managing database connections.
    /// </summary>
    public class NormConnectionException : NormException
    {
        public NormConnectionException(string message, Exception? innerException = null)
            : base(message, null, null, innerException)
        {
        }
    }
}
