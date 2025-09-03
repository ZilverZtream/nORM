using System;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when nORM configuration is invalid or incomplete.
    /// </summary>
    public class NormConfigurationException : NormException
    {
        public NormConfigurationException(string message, Exception? inner = null)
            : base(message, null, null, inner)
        {
        }
    }
}
