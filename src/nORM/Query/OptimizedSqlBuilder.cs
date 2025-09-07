using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.Extensions.ObjectPool;
using nORM.Mapping;

namespace nORM.Query
{
    /// <summary>
    /// A pooled SQL builder that reduces allocations during query generation.
    /// Wraps a <see cref="StringBuilder"/> obtained from an object pool and caches
    /// common SQL fragments and parameter values.
    /// </summary>
    internal sealed class OptimizedSqlBuilder : IDisposable
    {
        private static readonly ObjectPool<StringBuilder> _pool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        private static readonly ThreadLocal<StringBuilder> _threadLocalBuilder =
            new(() => new StringBuilder(512));

        private static readonly ObjectPool<Dictionary<(Type? type, int hash), string>> _parameterCachePool =
            new DefaultObjectPool<Dictionary<(Type? type, int hash), string>>(new DictionaryPooledObjectPolicy());

        private static readonly ConcurrentDictionary<string, string> _fragmentCache = new();

        // Common fragments that are frequently used in SQL generation.
        private static readonly string[] CommonFragments =
        {
            " WHERE ", " ORDER BY ", " GROUP BY ", " HAVING ", " JOIN ", " LEFT JOIN ",
            " INNER JOIN ", " ON ", " AND ", " OR ", " IN ", " LIKE ", " IS NULL ",
            " IS NOT NULL ", " COUNT(*)", " SUM(", " AVG(", " MIN(", " MAX("
        };

        // Pre-allocated templates for aggregate functions and SELECT clauses
        private static readonly string[] SqlFunctions = { "COUNT", "SUM", "AVG", "MIN", "MAX" };
        private static readonly ConcurrentDictionary<string, string> SqlTemplates = new();

        static OptimizedSqlBuilder()
        {
            foreach (var frag in CommonFragments)
                _fragmentCache.TryAdd(frag, frag);

            foreach (var fn in SqlFunctions)
                SqlTemplates.TryAdd(fn, $"{fn}({{0}})");
        }

        private readonly StringBuilder _buffer;
        private readonly Dictionary<(Type? type, int hash), string> _parameterCache;
        private readonly bool _returnToPool;
        private bool _hasWhere;

        public OptimizedSqlBuilder(int estimatedLength = 512)
        {
            _buffer = _pool.Get();
            _buffer.Clear();
            if (_buffer.Capacity < estimatedLength)
                _buffer.Capacity = estimatedLength;
            _parameterCache = _parameterCachePool.Get();
            _returnToPool = true;
        }

        public OptimizedSqlBuilder(StringBuilder builder, int estimatedLength = 512)
        {
            _buffer = builder;
            _buffer.Clear();
            if (_buffer.Capacity < estimatedLength)
                _buffer.Capacity = estimatedLength;
            _parameterCache = _parameterCachePool.Get();
            _returnToPool = false;
        }

        /// <summary>Length of the underlying buffer.</summary>
        public int Length => _buffer.Length;

        /// <summary>Exposes the underlying <see cref="StringBuilder"/> for interop.</summary>
        public StringBuilder InnerBuilder => _buffer;

        public OptimizedSqlBuilder Append(string? value)
        {
            if (!string.IsNullOrEmpty(value))
            {
                var estimated = EstimateAdditionalCapacity(value);
                _buffer.EnsureCapacity(_buffer.Length + estimated);
                _buffer.Append(value);
            }
            return this;
        }

        public OptimizedSqlBuilder Append(char value)
        {
            _buffer.Append(value);
            return this;
        }

        public OptimizedSqlBuilder Append(ReadOnlySpan<char> value)
        {
            _buffer.Append(value);
            return this;
        }

        public OptimizedSqlBuilder AppendFragment(string fragment)
        {
            _buffer.Append(_fragmentCache.GetOrAdd(fragment, static f => f));
            return this;
        }

        /// <summary>
        /// Append an aggregate function without incurring string interpolation costs.
        /// </summary>
        public OptimizedSqlBuilder AppendAggregateFunction(string function, string column)
        {
            var template = SqlTemplates.GetOrAdd(function, f => $"{f}({{0}})");
            _buffer.EnsureCapacity(_buffer.Length + function.Length + column.Length + 2);
            _buffer.AppendFormat(template, column);
            return this;
        }

        /// <summary>
        /// Append a SELECT clause with pre-sized capacity.
        /// </summary>
        public OptimizedSqlBuilder AppendSelect(ReadOnlySpan<char> columns)
        {
            _buffer.EnsureCapacity(_buffer.Length + 7 + columns.Length);
            _buffer.Append("SELECT ");
            _buffer.Append(columns);
            return this;
        }

        public OptimizedSqlBuilder Insert(int index, string value)
        {
            _buffer.Insert(index, value);
            return this;
        }

        public OptimizedSqlBuilder Remove(int start, int length)
        {
            _buffer.Remove(start, length);
            return this;
        }

        private static int EstimateAdditionalCapacity(string value)
        {
            var length = value.Length;
            if (value.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase))
                return length + 128;
            if (value.StartsWith("WHERE", StringComparison.OrdinalIgnoreCase) ||
                value.StartsWith("ORDER BY", StringComparison.OrdinalIgnoreCase) ||
                value.StartsWith("GROUP BY", StringComparison.OrdinalIgnoreCase) ||
                value.StartsWith("HAVING", StringComparison.OrdinalIgnoreCase))
                return length + 64;
            if (value.Contains("JOIN", StringComparison.OrdinalIgnoreCase))
                return length + 64;
            return length + 32;
        }

        public string ToString(int start, int length) => _buffer.ToString(start, length);

        public void Clear()
        {
            _buffer.Clear();
            _parameterCache.Clear();
            _hasWhere = false;
        }

        public void AppendSelect(string columns, string table, string? alias = null)
        {
            _buffer.Append("SELECT ").Append(columns).Append(" FROM ").Append(table);
            if (alias != null)
                _buffer.Append(' ').Append(alias);
        }

        public void AppendWhere(ReadOnlySpan<char> condition)
        {
            if (condition.IsEmpty) return;
            if (!_hasWhere)
            {
                _buffer.Append(" WHERE ");
                _hasWhere = true;
            }
            else
            {
                _buffer.Append(" AND ");
            }
            _buffer.Append(condition);
        }

        public OptimizedSqlBuilder AppendParameterizedValue(string parameterName, object? value, Dictionary<string, object> parameters)
        {
            var type = value?.GetType();
            var valueHash = value?.GetHashCode() ?? 0;
            var cacheKey = (type, valueHash);
            if (_parameterCache.TryGetValue(cacheKey, out var existing))
            {
                _buffer.Append(existing);
                return this;
            }

            parameters[parameterName] = value ?? DBNull.Value;
            _parameterCache[cacheKey] = parameterName;
            _buffer.Append(parameterName);
            return this;
        }

        public void AppendStandardSelect(TableMapping mapping, bool distinct = false)
        {
            var fragmentKey = $"SELECT_{mapping.Type.Name}_{distinct}";
            if (_fragmentCache.TryGetValue(fragmentKey, out var cached))
            {
                _buffer.Append(cached);
                return;
            }

            var sb = _threadLocalBuilder.Value!;
            sb.Clear();
            sb.Append(distinct ? "SELECT DISTINCT " : "SELECT ");
            var first = true;
            foreach (var col in mapping.Columns)
            {
                if (!first) sb.Append(", ");
                sb.Append(col.EscCol);
                first = false;
            }
            sb.Append(" FROM ").Append(mapping.EscTable);
            var result = sb.ToString();
            _fragmentCache.TryAdd(fragmentKey, result);
            _buffer.Append(result);
        }

        public string ToSqlString() => _buffer.ToString();

        public override string ToString() => _buffer.ToString();

        public void Dispose()
        {
            Clear();
            _parameterCachePool.Return(_parameterCache);
            if (_returnToPool)
                _pool.Return(_buffer);
        }

        private sealed class DictionaryPooledObjectPolicy : PooledObjectPolicy<Dictionary<(Type? type, int hash), string>>
        {
            public override Dictionary<(Type? type, int hash), string> Create() => new();

            public override bool Return(Dictionary<(Type? type, int hash), string> obj)
            {
                obj.Clear();
                return true;
            }
        }
    }
}

