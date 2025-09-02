using System;
using System.Collections.Generic;
using nORM.Core;

#nullable enable

namespace nORM.Enterprise
{
    public interface IDbContextLogger
    {
        void LogQuery(string sql, IReadOnlyDictionary<string, object> parameters, TimeSpan duration, int recordCount);
        void LogBulkOperation(string operation, string tableName, int recordCount, TimeSpan duration);
        void LogError(NormException exception, int retryAttempt);
    }
}