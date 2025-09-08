using System.Data.Common;
using System.Runtime.CompilerServices;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Runtime.CompilerServices;
using System.Linq.Expressions;
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
            Parameters = new Dictionary<string, object>();
            CompiledParameters = new List<string>();
            ParameterMap = new Dictionary<ParameterExpression, string>();
            Volatile.Write(ref _index, 0);
        }
    }
}

namespace nORM.Query
{
    internal static class ParameterAssign
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static void AssignValue(DbParameter p, object? v)
        {
            if (v is null) { p.Value = DBNull.Value; return; }

            switch (v)
            {
                case int i: p.DbType = System.Data.DbType.Int32; p.Value = i; return;
                case long l: p.DbType = System.Data.DbType.Int64; p.Value = l; return;
                case short s: p.DbType = System.Data.DbType.Int16; p.Value = s; return;
                case byte b: p.DbType = System.Data.DbType.Byte; p.Value = b; return;
                case bool bo: p.DbType = System.Data.DbType.Int32; p.Value = bo ? 1 : 0; return; // SQLite
                case double d: p.DbType = System.Data.DbType.Double; p.Value = d; return;
                case float f: p.DbType = System.Data.DbType.Double; p.Value = (double)f; return;
                case decimal m: p.DbType = System.Data.DbType.Decimal; p.Value = m; return;
                case string s2: p.DbType = System.Data.DbType.String; p.Value = s2; return;
                case DateTime dt: p.DbType = System.Data.DbType.String; p.Value = dt.ToString("O"); return; // ISO for SQLite
                default:
                    p.Value = v; return;
            }
        }
    }
}
