using System;

namespace nORM.Core
{
    /// <summary>
    /// Exception thrown when a LINQ query cannot be translated to SQL.
    /// </summary>
    public class NormQueryTranslationException : NormException
    {
        public NormQueryTranslationException(string message, Exception? inner = null)
            : base(message, null, null, inner)
        {
        }
    }
}
