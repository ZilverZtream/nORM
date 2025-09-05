using System;
using System.Collections.Generic;

namespace nORM.Core
{
    public class NormQueryException : NormException
    {
        public NormQueryException(string message, string? sql = null,
            IReadOnlyDictionary<string, object>? parameters = null, Exception? inner = null)
            : base(message, sql, parameters, inner)
        {
        }
    }
}
