using System;
using nORM.Providers;

namespace nORM.Query
{
    internal static class QueryPlanValidator
    {
        /// <summary>
        /// Ensures that a generated <see cref="QueryPlan"/> complies with provider-specific limits such as
        /// maximum SQL length and parameter count.
        /// </summary>
        /// <param name="plan">The query plan produced by translation.</param>
        /// <param name="provider">Database provider defining operational limits.</param>
        /// <exception cref="InvalidOperationException">
        /// Thrown when the plan exceeds the provider's supported SQL length or number of parameters.
        /// </exception>
        public static void Validate(QueryPlan plan, DatabaseProvider provider)
        {
            if (plan.Sql.Length > provider.MaxSqlLength)
                throw new InvalidOperationException($"Generated SQL exceeds maximum length of {provider.MaxSqlLength}");

            if (plan.Parameters.Count > provider.MaxParameters)
                throw new InvalidOperationException($"Too many parameters in query. Maximum is {provider.MaxParameters}");
        }
    }
}
