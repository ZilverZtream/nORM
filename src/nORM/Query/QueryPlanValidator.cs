using System;
using nORM.Providers;

namespace nORM.Query
{
    internal static class QueryPlanValidator
    {
        public static void Validate(QueryPlan plan, DatabaseProvider provider)
        {
            if (plan.Sql.Length > provider.MaxSqlLength)
                throw new InvalidOperationException($"Generated SQL exceeds maximum length of {provider.MaxSqlLength}");

            if (plan.Parameters.Count > provider.MaxParameters)
                throw new InvalidOperationException($"Too many parameters in query. Maximum is {provider.MaxParameters}");
        }
    }
}
