using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

#nullable enable

namespace nORM.Query
{
    /// <summary>
    /// REFACTOR (TASK 19): Shared utility for extracting constant values from expressions.
    /// Eliminates duplicate logic between ExpressionToSqlVisitor.TryGetConstantValue and
    /// FastPathQueryExecutor.TryGetSimpleValue.
    /// </summary>
    internal static class ExpressionValueExtractor
    {
        /// <summary>
        /// Safely extracts a constant value from an expression without executing arbitrary code.
        /// Supports ConstantExpression and simple MemberExpression (accessing a field/property).
        /// </summary>
        /// <param name="expr">The expression to extract a value from.</param>
        /// <param name="value">The extracted value, or null if extraction failed.</param>
        /// <param name="visited">Optional set to track visited expressions and prevent infinite recursion.</param>
        /// <returns>True if the value was successfully extracted; false otherwise.</returns>
        /// <remarks>
        /// SECURITY: This method does not compile or execute arbitrary expressions.
        /// Only safe operations like reading constants and accessing members are performed.
        /// Method calls are explicitly rejected to prevent RCE vulnerabilities.
        /// </remarks>
        public static bool TryGetConstantValue(Expression expr, out object? value, HashSet<Expression>? visited = null)
        {
            visited ??= new HashSet<Expression>(ReferenceEqualityComparer.Instance);

            // Prevent infinite recursion from circular references
            if (!visited.Add(expr))
            {
                value = null;
                return false;
            }

            switch (expr)
            {
                case ConstantExpression ce:
                    value = ce.Value;
                    return true;

                case MemberExpression me:
                    // Recursively evaluate the parent expression
                    if (me.Expression != null && TryGetConstantValue(me.Expression, out var obj, visited))
                    {
                        value = GetMemberValue(me.Member, obj);
                        return true;
                    }
                    break;

                // SECURITY FIX: Method calls are explicitly NOT supported.
                // Previous implementations allowed MethodCallExpression which could execute
                // arbitrary user code via Compile().DynamicInvoke() - a critical RCE vulnerability.
                // Method calls should be translated to SQL (e.g., string.Contains -> LIKE) or rejected.
            }

            value = null;
            return false;
        }

        /// <summary>
        /// Gets the value of a field or property member from an object instance.
        /// </summary>
        /// <param name="member">The member to read (FieldInfo or PropertyInfo).</param>
        /// <param name="obj">The object instance containing the member value.</param>
        /// <returns>The value of the member.</returns>
        /// <exception cref="InvalidOperationException">Thrown when the member is neither a field nor property.</exception>
        internal static object? GetMemberValue(MemberInfo member, object? obj)
        {
            return member switch
            {
                FieldInfo fi => fi.GetValue(obj),
                PropertyInfo pi => pi.GetValue(obj),
                _ => throw new InvalidOperationException($"Member type {member.MemberType} is not supported.")
            };
        }
    }
}
