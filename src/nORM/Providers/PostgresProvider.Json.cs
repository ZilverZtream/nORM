using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.ObjectPool;
using nORM.Core;
using nORM.Internal;
using nORM.Mapping;
using nORM.Query;

#nullable enable

namespace nORM.Providers
{
    public sealed partial class PostgresProvider
    {
        /// <summary>
        /// Produces a SQL fragment that accesses a JSON value using PostgreSQL's <c>jsonb_extract_path_text</c>.
        /// </summary>
        /// <param name="columnName">The JSON column being accessed.</param>
        /// <param name="jsonPath">The JSON path expression (dot-delimited with optional array indices).</param>
        /// <returns>SQL fragment that retrieves the JSON value as text.</returns>
        /// <remarks>
        /// Supports both dot-notation and array accessors.
        /// Supported patterns:
        /// - Simple dot-notation: "user.address.city"
        /// - Array accessors: "items[0]", "items[0].name", "data[1].users[2].id"
        /// - Mixed: "order.items[0].product.name"
        /// NOT supported: JSONPath filter expressions like "items[*]", "items[?(@.active)]"
        ///
        /// Uses a pooled StringBuilder to reduce allocations; sb.ToString() still allocates for the returned string.
        /// </remarks>
        public override string TranslateJsonPathAccess(string columnName, string jsonPath)
        {
            ArgumentNullException.ThrowIfNull(columnName);
            ArgumentNullException.ThrowIfNull(jsonPath);
            ValidateJsonPath(jsonPath);

            var sb = _stringBuilderPool.Get();
            try
            {
                sb.Append("jsonb_extract_path_text(");
                sb.Append(columnName);
                sb.Append(", ");

                // Skip first segment (root '$') if present
                int startIndex = jsonPath.StartsWith("$.") ? 2 : (jsonPath.StartsWith("$") ? 1 : 0);
                if (startIndex > 0 && startIndex < jsonPath.Length && jsonPath[startIndex] == '.')
                    startIndex++;

                // Root-only path "$" has no path segments after stripping the $.
                // PostgreSQL's jsonb_extract_path_text requires at least one path argument.
                // For root access, cast the column to text directly instead of using the function.
                if (startIndex >= jsonPath.Length)
                {
                    sb.Clear();
                    sb.Append(columnName);
                    sb.Append(" #>> '{}'");
                    return sb.ToString();
                }

                bool isFirst = true;

                while (startIndex < jsonPath.Length)
                {
                    if (!isFirst)
                    {
                        sb.Append(", ");
                    }
                    isFirst = false;

                    // Find next delimiter: either '.' or '['
                    int dotIndex = jsonPath.IndexOf('.', startIndex);
                    int bracketIndex = jsonPath.IndexOf('[', startIndex);

                    int nextDelimiter;
                    if (dotIndex == -1 && bracketIndex == -1)
                    {
                        // No more delimiters, consume rest of string
                        nextDelimiter = jsonPath.Length;
                    }
                    else if (dotIndex == -1)
                    {
                        nextDelimiter = bracketIndex;
                    }
                    else if (bracketIndex == -1)
                    {
                        nextDelimiter = dotIndex;
                    }
                    else
                    {
                        nextDelimiter = Math.Min(dotIndex, bracketIndex);
                    }

                    // Extract property name before delimiter
                    if (nextDelimiter > startIndex)
                    {
                        sb.Append('\'');
                        sb.Append(jsonPath, startIndex, nextDelimiter - startIndex);
                        sb.Append('\'');

                        startIndex = nextDelimiter;
                    }

                    // Handle array index if present
                    if (startIndex < jsonPath.Length && jsonPath[startIndex] == '[')
                    {
                        int closeBracketIndex = jsonPath.IndexOf(']', startIndex);
                        if (closeBracketIndex == -1)
                        {
                            throw new ArgumentException($"Invalid JSON path: unclosed bracket at position {startIndex}", nameof(jsonPath));
                        }

                        // Extract array index (between '[' and ']')
                        int indexStart = startIndex + 1;
                        int indexLength = closeBracketIndex - indexStart;

                        if (indexLength > 0)
                        {
                            sb.Append(", '");
                            sb.Append(jsonPath, indexStart, indexLength);
                            sb.Append('\'');
                        }

                        startIndex = closeBracketIndex + 1;
                    }

                    // Skip dot delimiter
                    if (startIndex < jsonPath.Length && jsonPath[startIndex] == '.')
                    {
                        startIndex++;
                    }
                }

                sb.Append(')');
                return sb.ToString();
            }
            finally
            {
                sb.Clear();
                _stringBuilderPool.Return(sb);
            }
        }

        /// <summary>
        /// Builds an optimized <c>ANY</c> expression for arrays to implement a <c>Contains</c> filter.
        /// </summary>
        /// <param name="cmd">Command to which parameters are added.</param>
        /// <param name="columnName">Name of the column being filtered.</param>
        /// <param name="values">Values to check for containment.</param>
        /// <returns>SQL fragment implementing the containment check.</returns>
        public override string BuildContainsClause(DbCommand cmd, string columnName, IReadOnlyList<object?> values)
        {
            ArgumentNullException.ThrowIfNull(cmd);
            ArgumentNullException.ThrowIfNull(columnName);
            ArgumentNullException.ThrowIfNull(values);

            if (values.Count == 0)
                return "(1=0)";

            var hasNulls = false;
            List<object?>? nonNullValues = null;
            for (var i = 0; i < values.Count; i++)
            {
                if (values[i] == null)
                {
                    hasNulls = true;
                    nonNullValues ??= values.Take(i).ToList();
                }
                else
                {
                    nonNullValues?.Add(values[i]);
                }
            }

            if (hasNulls && (nonNullValues == null || nonNullValues.Count == 0))
                return $"{columnName} IS NULL";

            var arrayValues = nonNullValues ?? values;
            var pName = ParamPrefix + "p0";
            var p = cmd.CreateParameter();
            p.ParameterName = pName;
            // Create a typed array so Npgsql can infer NpgsqlDbType correctly.
            // An untyped object[] causes binding failures for Guid, int, enum, nullable types on live PostgreSQL.
            p.Value = CreateTypedArray(arrayValues);
            cmd.Parameters.Add(p);
            return hasNulls
                ? $"({columnName} = ANY({pName}) OR {columnName} IS NULL)"
                : $"{columnName} = ANY({pName})";
        }

        /// <summary>
        /// Builds the strongest common-type array from the supplied values for Npgsql type inference.
        /// </summary>
        /// <param name="values">Values to convert into a typed array.</param>
        /// <returns>A strongly-typed array when all non-null values share the same type; otherwise an <c>object[]</c>.</returns>
        [System.Diagnostics.CodeAnalysis.UnconditionalSuppressMessage("AOT", "IL3050",
            Justification = "Element types are runtime types of live values or entries from the static nullable table; when the runtime cannot provide the array type the catch below falls back to object[].")]
        private static Array CreateTypedArray(IReadOnlyList<object?> values)
        {
            Type? commonType = null;
            bool hasNull = false;
            foreach (var v in values)
            {
                if (v == null) { hasNull = true; continue; }
                var t = v.GetType();
                if (commonType == null) { commonType = t; }
                else if (commonType != t) { commonType = null; break; }
            }
            if (commonType == null)
                return values.ToArray(); // mixed or all-null -- object[] fallback

            // When the collection contains nulls AND the common type is a non-nullable
            // value type (e.g., int), use Nullable<T> as the array element type. Otherwise
            // Array.SetValue(null, i) throws InvalidCastException on value-type arrays.
            // The nullable type comes from a static table of statically-referenced
            // instantiations (runtime Nullable<> construction is not NativeAOT-compatible);
            // exotic value types keep the documented object[] fallback.
            var elementType = commonType;
            if (hasNull && commonType.IsValueType && Nullable.GetUnderlyingType(commonType) == null)
            {
                elementType = GetKnownNullableType(commonType);
                if (elementType == null)
                    return values.ToArray();
            }

            Array arr;
            try
            {
                arr = Array.CreateInstance(elementType, values.Count);
            }
            catch (NotSupportedException)
            {
                // NativeAOT cannot create arrays of types it never saw as array elements;
                // the object[] fallback keeps parameter binding functional.
                return values.ToArray();
            }
            for (int i = 0; i < values.Count; i++)
                arr.SetValue(values[i], i);
            return arr;
        }

        private static Type? GetKnownNullableType(Type valueType)
        {
            if (valueType == typeof(bool)) return typeof(bool?);
            if (valueType == typeof(byte)) return typeof(byte?);
            if (valueType == typeof(sbyte)) return typeof(sbyte?);
            if (valueType == typeof(short)) return typeof(short?);
            if (valueType == typeof(ushort)) return typeof(ushort?);
            if (valueType == typeof(int)) return typeof(int?);
            if (valueType == typeof(uint)) return typeof(uint?);
            if (valueType == typeof(long)) return typeof(long?);
            if (valueType == typeof(ulong)) return typeof(ulong?);
            if (valueType == typeof(float)) return typeof(float?);
            if (valueType == typeof(double)) return typeof(double?);
            if (valueType == typeof(decimal)) return typeof(decimal?);
            if (valueType == typeof(char)) return typeof(char?);
            if (valueType == typeof(Guid)) return typeof(Guid?);
            if (valueType == typeof(DateTime)) return typeof(DateTime?);
            if (valueType == typeof(DateTimeOffset)) return typeof(DateTimeOffset?);
            if (valueType == typeof(TimeSpan)) return typeof(TimeSpan?);
            if (valueType == typeof(DateOnly)) return typeof(DateOnly?);
            if (valueType == typeof(TimeOnly)) return typeof(TimeOnly?);
            return null;
        }
    }
}
