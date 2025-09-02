using System;
using System.Data.Common;
using Microsoft.Data.SqlClient;

#nullable enable

namespace nORM.Enterprise
{
    public class RetryPolicy
    {
        public int MaxRetries { get; set; } = 3;
        public TimeSpan BaseDelay { get; set; } = TimeSpan.FromSeconds(1);
        public Func<DbException, bool> ShouldRetry { get; set; } = ex => ex is SqlException sqlEx && sqlEx.Number is 4060 or 40197 or 40501 or 40613 or 49918 or 49919 or 49920 or 1205;
    }
}