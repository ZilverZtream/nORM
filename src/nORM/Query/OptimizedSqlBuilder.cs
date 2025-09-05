using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
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
        private static readonly ObjectPool<StringBuilder> _stringBuilderPool =
            new DefaultObjectPool<StringBuilder>(new StringBuilderPooledObjectPolicy());

        private static readonly ConcurrentDictionary<string, string> _fragmentCache = new();

        // Common fragments that are frequently used in SQL generation.
        private static readonly string[] CommonFragments =
        {
            " WHERE ", " ORDER BY ", " GROUP BY ", " HAVING ", " JOIN ", " LEFT JOIN ",
            " INNER JOIN ", " ON ", " AND ", " OR ", " IN ", " LIKE ", " IS NULL ",
            " IS NOT NULL ", " COUNT(*)", " SUM(", " AVG(", " MIN(", " MAX("
        };

        static OptimizedSqlBuilder()
        {
            foreach (var frag in CommonFragments)
                _fragmentCache.TryAdd(frag, frag);
        }

        private readonly StringBuilder _buffer;
        private readonly Dictionary<string, string> _parameterCache = new();
        private readonly bool _returnToPool;
        private bool _hasWhere;

        public OptimizedSqlBuilder(int estimatedLength = 512)
        {
            _buffer = _stringBuilderPool.Get();
            _buffer.Clear();
            if (_buffer.Capacity < estimatedLength)
                _buffer.Capacity = estimatedLength;
            _returnToPool = true;
        }

        public OptimizedSqlBuilder(StringBuilder builder, int estimatedLength = 512)
        {
            _buffer = builder;
            _buffer.Clear();
            if (_buffer.Capacity < estimatedLength)
                _buffer.Capacity = estimatedLength;
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
            var valueHash = value?.GetHashCode() ?? 0;
            var cacheKey = $"{value?.GetType().Name}_{valueHash}";
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

            var sb = new StringBuilder(256);
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
            try
            {
                if (_returnToPool)
                    _stringBuilderPool.Return(_buffer);
            }
            catch
            {
                // Swallow to avoid leaking if the pool rejects the builder
            }
        }
    }
}

