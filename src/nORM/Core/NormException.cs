using System;
using System.Collections.Generic;
using System.Data.Common;

#nullable enable

namespace nORM.Core
{
    public class NormException : DbException
    {
        public string? SqlStatement { get; }
        public IReadOnlyDictionary<string, object>? Parameters { get; }
        
        public NormException(string message, string? sql, IReadOnlyDictionary<string, object>? parameters, Exception? inner = null)
            : base(message, inner)
        {
            SqlStatement = sql;
            Parameters = parameters;
        }
    }
}