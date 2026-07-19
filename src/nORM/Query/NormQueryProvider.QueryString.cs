using System;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

#nullable enable

namespace nORM.Query
{
    internal sealed partial class NormQueryProvider
    {
        /// <summary>
        /// Renders the SQL a query would execute, without running it — the backend for the public
        /// <c>ToQueryString()</c> extension. The generated SQL is preceded by a comment line per parameter
        /// (name and value) so the statement is self-describing, mirroring EF Core's <c>ToQueryString</c>.
        /// </summary>
        internal string BuildQueryString(Expression expression)
        {
            var plan = GetPlan(expression, out _, out var parameterValues);
            var parameters = EnsureParameterDictionary(plan, parameterValues);
            if (parameters.Count == 0)
                return plan.Sql;

            var sb = new StringBuilder();
            foreach (var kvp in parameters.OrderBy(p => p.Key, StringComparer.Ordinal))
                sb.Append("-- ").Append(kvp.Key).Append(" = ").Append(FormatParameterValue(kvp.Value)).Append('\n');
            sb.Append('\n').Append(plan.Sql);
            return sb.ToString();
        }

        private static string FormatParameterValue(object? value) => value switch
        {
            null or DBNull => "NULL",
            string s => "'" + s.Replace("'", "''", StringComparison.Ordinal) + "'",
            bool b => b ? "1" : "0",
            byte[] bytes => "0x" + Convert.ToHexString(bytes),
            IFormattable f => f.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? "NULL"
        };
    }
}
