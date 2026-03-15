using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Threading;

namespace nORM.Query
{
    /// <summary>
    /// Manages SQL parameters, compiled parameter names and mapping between
    /// expression parameters and generated SQL parameter placeholders.
    /// Uses a small preallocated array to avoid dictionary allocations for
    /// common query scenarios with few parameters.
    /// </summary>
    internal sealed class ParameterManager
    {
        public IDictionary<string, object> Parameters { get; set; } = new Dictionary<string, object>();
        public List<string> CompiledParameters { get; set; } = new();
        public Dictionary<ParameterExpression, string> ParameterMap { get; set; } = new();

        private int _index;

        public int Index
        {
            get => Volatile.Read(ref _index);
            set => Volatile.Write(ref _index, value);
        }

        public int GetNextIndex() => Interlocked.Increment(ref _index) - 1;

        public void Reset()
        {
            Parameters.Clear();
            CompiledParameters.Clear();
            ParameterMap.Clear();
            Volatile.Write(ref _index, 0);
        }
    }
}

namespace nORM.Query
{
    internal static class ParameterAssign
    {
        // PERFORMANCE OPTIMIZATION 1: AggressiveInlining + AggressiveOptimization for hot path
        [MethodImpl(MethodImplOptions.AggressiveInlining | MethodImplOptions.AggressiveOptimization)]
        internal static void AssignValue(DbParameter p, object? v)
        {
            // PERFORMANCE OPTIMIZATION 2: Fast null check first (most common nullable scenario)
            // P1 fix: reset all provider-visible metadata (DbType, Size, Precision, Scale) so
            // stale values from a previous non-null assignment do not carry over on reuse.
            if (v is null)
            {
                p.Value = DBNull.Value;
                p.DbType = System.Data.DbType.Object;
                p.Size = 0;
                p.Precision = 0;
                p.Scale = 0;
                return;
            }

            // PERFORMANCE OPTIMIZATION 3: Reordered cases by frequency (int/string/long most common)
            // Pattern matching with is + cast is faster than switch expression for primitives
            var type = v.GetType();

            // Most common types first (int, string, long, bool)
            if (type == typeof(int))
            {
                p.DbType = System.Data.DbType.Int32;
                p.Value = v;
                return;
            }

            if (type == typeof(string))
            {
                p.DbType = System.Data.DbType.String;
                p.Value = v;
                // PERFORMANCE OPTIMIZATION 4: Set size hint for strings to avoid provider guessing
                if (v is string str && str.Length <= 4000)
                    p.Size = str.Length;
                return;
            }

            if (type == typeof(long))
            {
                p.DbType = System.Data.DbType.Int64;
                p.Value = v;
                return;
            }

            if (type == typeof(bool))
            {
                p.DbType = System.Data.DbType.Boolean;
                p.Value = v;
                return;
            }

            // Other numeric types
            if (type == typeof(decimal))
            {
                p.DbType = System.Data.DbType.Decimal;
                // P1 fix: reset precision/scale to 0 (driver-default) so stale values from a
                // previous high-precision decimal assignment do not coerce the new value.
                p.Precision = 0;
                p.Scale = 0;
                p.Value = v;
                return;
            }

            if (type == typeof(double))
            {
                p.DbType = System.Data.DbType.Double;
                p.Value = v;
                return;
            }

            if (type == typeof(short))
            {
                p.DbType = System.Data.DbType.Int16;
                p.Value = v;
                return;
            }

            if (type == typeof(byte))
            {
                p.DbType = System.Data.DbType.Byte;
                p.Value = v;
                return;
            }

            if (type == typeof(float))
            {
                p.DbType = System.Data.DbType.Double;
                p.Value = (double)(float)v;
                return;
            }

            // DateTime and related types
            if (type == typeof(DateTime))
            {
                // Provider will set correct DbType (DateTime2 for SQL Server, Timestamp for Postgres, etc.)
                p.Value = v;
                return;
            }

            if (type == typeof(Guid))
            {
                p.DbType = System.Data.DbType.Guid;
                p.Value = v;
                return;
            }

            if (type == typeof(byte[]))
            {
                p.DbType = System.Data.DbType.Binary;
                p.Value = v;
                return;
            }

            if (type == typeof(DateOnly))
            {
                p.DbType = System.Data.DbType.Date;
                p.Value = v;
                return;
            }

            if (type == typeof(TimeOnly))
            {
                p.DbType = System.Data.DbType.Time;
                p.Value = ((TimeOnly)v).ToTimeSpan();
                return;
            }

            if (type == typeof(DateTimeOffset))
            {
                p.DbType = System.Data.DbType.DateTimeOffset;
                p.Value = v;
                return;
            }

            if (type == typeof(TimeSpan))
            {
                p.DbType = System.Data.DbType.Time;
                p.Value = v;
                return;
            }

            if (type == typeof(char))
            {
                p.DbType = System.Data.DbType.StringFixedLength;
                p.Value = v.ToString();
                return;
            }

            if (type == typeof(uint))
            {
                p.DbType = System.Data.DbType.UInt32;
                p.Value = v;
                return;
            }

            if (type == typeof(ulong))
            {
                p.DbType = System.Data.DbType.UInt64;
                p.Value = v;
                return;
            }

            if (type == typeof(sbyte))
            {
                p.DbType = System.Data.DbType.SByte;
                p.Value = v;
                return;
            }

            if (type == typeof(ushort))
            {
                p.DbType = System.Data.DbType.UInt16;
                p.Value = v;
                return;
            }

            // Enum: convert to underlying integral type for cross-provider consistency.
            // Without this, raw enum values hit the driver with no DbType hint which causes
            // provider-dependent coercion failures (P2).
            if (type.IsEnum)
            {
                var underlyingType = Enum.GetUnderlyingType(type);
                var converted = Convert.ChangeType(v, underlyingType);
                if (underlyingType == typeof(int))        p.DbType = System.Data.DbType.Int32;
                else if (underlyingType == typeof(long))  p.DbType = System.Data.DbType.Int64;
                else if (underlyingType == typeof(short)) p.DbType = System.Data.DbType.Int16;
                else if (underlyingType == typeof(byte))  p.DbType = System.Data.DbType.Byte;
                else if (underlyingType == typeof(uint))  p.DbType = System.Data.DbType.UInt32;
                else if (underlyingType == typeof(ulong)) p.DbType = System.Data.DbType.UInt64;
                else if (underlyingType == typeof(sbyte)) p.DbType = System.Data.DbType.SByte;
                else if (underlyingType == typeof(ushort)) p.DbType = System.Data.DbType.UInt16;
                p.Value = converted;
                return;
            }

            // Default fallback
            p.Value = v;
        }
    }
}
